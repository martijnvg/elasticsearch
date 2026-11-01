/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.fielddata.plain;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.SortField;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.columnar.string.DictionaryStringColumnReader;
import org.elasticsearch.columnar.string.StringColumnReader;
import org.elasticsearch.columnar.string.StringColumnSource;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.fielddata.FieldData;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldData.XFieldComparatorSource.Nested;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.fielddata.LeafFieldData;
import org.elasticsearch.index.fielddata.LeafOrdinalsFieldData;
import org.elasticsearch.index.fielddata.SortableBinaryDocValues;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.script.field.DocValuesScriptFieldFactory;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.search.sort.BucketedSort;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.ExecutionException;

/**
 * A columnar keyword field read through ordinals, for a reader {@link ColumnarKeywordIndexFieldData#forReader} found
 * every segment of to qualify. Global ordinals are built from it as they are for any other field with ordinals.
 *
 * <p>How a segment gets its ordinals depends on how its column was written, which {@link #classify} decides from what
 * the column records, without reading a value. A dictionary that names every value is read as it is. A dictionary that
 * some values escaped, or a column stored plainly, has those values read and ordered the first time the segment is
 * loaded, and the result is held in the field data cache for as long as the segment lives, so a refresh does not
 * redo it. That is bounded by {@code maxMaterializedValues} so it stays cheap.
 *
 * <p>Values are still read as values wherever they are asked for as such, through the same doc values a field read
 * without ordinals uses, and sorting is left to that field data too.
 */
final class ColumnarOrdinalsIndexFieldData extends AbstractIndexOrdinalsFieldData {

    /** How a segment's column offers ordinals. */
    enum SegmentKind {
        /** The segment holds no value for the field. */
        ABSENT,
        /** A dictionary naming every value: its dense ordinals are the segment's ordinals, at no cost. */
        DICTIONARY,
        /** A dictionary that some values escaped, which are read and ordered among its terms. */
        ESCAPED_DICTIONARY,
        /** A column stored plainly, whose values are all read and ordered. */
        PLAIN,
        /** A segment that cannot offer ordinals for the field, or only at a cost past the bound. */
        UNSUPPORTED
    }

    private final BytesBinaryIndexFieldData values;
    private final IndexFieldDataCache cache;
    private final long maxMaterializedValues;
    private final Materializer materializer = new Materializer();

    ColumnarOrdinalsIndexFieldData(
        BytesBinaryIndexFieldData values,
        IndexFieldDataCache cache,
        CircuitBreakerService breakerService,
        long maxMaterializedValues
    ) {
        super(
            values.getFieldName(),
            values.getValuesSourceType(),
            cache,
            breakerService,
            (dv, name) -> values.toScriptFieldFactory.getScriptFieldFactory(FieldData.toString(dv), name)
        );
        this.values = values;
        this.cache = cache;
        this.maxMaterializedValues = maxMaterializedValues;
    }

    /**
     * How {@code reader}'s segment offers ordinals for {@code field}. Decided from what the column records, so no value
     * is read: whether it has a dictionary, how many values escaped it, and how many values it holds.
     */
    static SegmentKind classify(LeafReader reader, String field, long maxMaterializedValues) throws IOException {
        final FieldInfo fieldInfo = reader.getFieldInfos().fieldInfo(field);
        if (fieldInfo == null) {
            return SegmentKind.ABSENT;
        }
        if (fieldInfo.getDocValuesType() != DocValuesType.BINARY) {
            return SegmentKind.UNSUPPORTED;
        }
        final BinaryDocValues values = reader.getBinaryDocValues(field);
        if (values == null) {
            return SegmentKind.ABSENT;
        }
        if (values instanceof StringColumnSource == false) {
            // A segment written by another format, or doc values wrapped by something that does not offer the column.
            return SegmentKind.UNSUPPORTED;
        }
        final StringColumnReader column = ((StringColumnSource) values).reader();
        if (column.hasDictionary()) {
            if (column.escapeCount() == 0) {
                return SegmentKind.DICTIONARY;
            }
            return column.escapeCount() <= maxMaterializedValues ? SegmentKind.ESCAPED_DICTIONARY : SegmentKind.UNSUPPORTED;
        }
        return column.numValues() <= maxMaterializedValues ? SegmentKind.PLAIN : SegmentKind.UNSUPPORTED;
    }

    @Override
    public LeafOrdinalsFieldData load(LeafReaderContext context) {
        final SegmentKind kind;
        try {
            kind = classify(context.reader(), getFieldName(), maxMaterializedValues);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return switch (kind) {
            case ABSENT -> AbstractLeafOrdinalsFieldData.empty(toScriptFieldFactory);
            case DICTIONARY -> new Leaf(context, ColumnarSegmentOrdinals.DICTIONARY);
            case ESCAPED_DICTIONARY, PLAIN -> new Leaf(context, materialized(context));
            case UNSUPPORTED -> throw new IllegalStateException(
                "field [" + getFieldName() + "] cannot be read through ordinals in segment [" + context.reader() + "]"
            );
        };
    }

    @Override
    public LeafOrdinalsFieldData loadDirect(LeafReaderContext context) {
        return load(context);
    }

    /** The ordinals worked out for a segment, from the cache, which keeps them for as long as the segment lives. */
    private ColumnarSegmentOrdinals materialized(LeafReaderContext context) {
        if (context.reader().getCoreCacheHelper() == null) {
            // A reader that cannot be cached on. Working the ordinals out each time is correct, only slower.
            return materializer.build(context.reader());
        }
        try {
            return cache.load(context, materializer).ordinals;
        } catch (Exception e) {
            if (e instanceof ElasticsearchException ese) {
                throw ese;
            }
            if (e instanceof ExecutionException && e.getCause() instanceof ElasticsearchException ese) {
                throw ese;
            }
            throw new ElasticsearchException(e);
        }
    }

    @Override
    public SortField sortField(@Nullable Object missingValue, MultiValueMode sortMode, Nested nested, boolean reverse) {
        return values.sortField(missingValue, sortMode, nested, reverse);
    }

    @Override
    public SortField indexSort(IndexVersion indexCreatedVersion, @Nullable Object missingValue, MultiValueMode sortMode, boolean reverse) {
        return values.indexSort(indexCreatedVersion, missingValue, sortMode, reverse);
    }

    @Override
    public BucketedSort newBucketedSort(
        BigArrays bigArrays,
        Object missingValue,
        MultiValueMode sortMode,
        Nested nested,
        SortOrder sortOrder,
        DocValueFormat format,
        int bucketSize,
        BucketedSort.ExtraData extra
    ) {
        return values.newBucketedSort(bigArrays, missingValue, sortMode, nested, sortOrder, format, bucketSize, extra);
    }

    @Override
    public boolean supportsGlobalOrdinalsMapping() {
        return true;
    }

    /**
     * One segment's field data. Ordinals come from the segment's {@link ColumnarSegmentOrdinals}, and values from the
     * doc values a field read without ordinals uses, so the paths that read values work as they do without ordinals.
     */
    private final class Leaf implements LeafOrdinalsFieldData {
        private final LeafReader reader;
        private final ColumnarSegmentOrdinals ordinals;
        private final MultiValuedBinaryDVLeafFieldData bytes;

        Leaf(LeafReaderContext context, ColumnarSegmentOrdinals ordinals) {
            this.reader = context.reader();
            this.ordinals = ordinals;
            this.bytes = values.load(context);
        }

        @Override
        public SortedSetDocValues getOrdinalsValues() {
            try {
                final BinaryDocValues binary = reader.getBinaryDocValues(getFieldName());
                if (binary == null) {
                    return DocValues.emptySortedSet();
                }
                return ColumnarOrdinalsDocValues.open(binary, ordinals);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        @Override
        public SortableBinaryDocValues getBytesValues() {
            return bytes.getBytesValues();
        }

        @Override
        public DocValuesScriptFieldFactory getScriptFieldFactory(String name) {
            return bytes.getScriptFieldFactory(name);
        }

        /** Nothing of its own: the ordinals a segment needed working out are accounted for by the cache that holds them. */
        @Override
        public long ramBytesUsed() {
            return 0;
        }
    }

    /**
     * Works out the ordinals of a segment whose column does not already have them. Field data in its own right only so
     * the field data cache can hold what it builds, which is what ties the ordinals to the segment's lifetime, charges
     * them to the field data circuit breaker, and lets clearing the cache drop them.
     */
    private final class Materializer implements IndexFieldData<MaterializedOrdinals> {

        ColumnarSegmentOrdinals build(LeafReader reader) {
            try {
                final BinaryDocValues binary = reader.getBinaryDocValues(getFieldName());
                final StringColumnReader column = ((StringColumnSource) binary).reader();
                return column instanceof DictionaryStringColumnReader dictionary
                    ? ColumnarSegmentOrdinals.escapedDictionary(dictionary)
                    : ColumnarSegmentOrdinals.plain(binary);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        @Override
        public MaterializedOrdinals loadDirect(LeafReaderContext context) {
            final ColumnarSegmentOrdinals ordinals = build(context.reader());
            breakerService.getBreaker(CircuitBreaker.FIELDDATA)
                .addEstimateBytesAndMaybeBreak(ordinals.ramBytesUsed(), "columnar ordinals [" + getFieldName() + "]");
            return new MaterializedOrdinals(ordinals);
        }

        @Override
        public MaterializedOrdinals load(LeafReaderContext context) {
            return loadDirect(context);
        }

        @Override
        public String getFieldName() {
            return ColumnarOrdinalsIndexFieldData.this.getFieldName();
        }

        @Override
        public ValuesSourceType getValuesSourceType() {
            return ColumnarOrdinalsIndexFieldData.this.getValuesSourceType();
        }

        @Override
        public SortField sortField(@Nullable Object missingValue, MultiValueMode sortMode, Nested nested, boolean reverse) {
            throw new UnsupportedOperationException("only builds ordinals");
        }

        @Override
        public BucketedSort newBucketedSort(
            BigArrays bigArrays,
            Object missingValue,
            MultiValueMode sortMode,
            Nested nested,
            SortOrder sortOrder,
            DocValueFormat format,
            int bucketSize,
            BucketedSort.ExtraData extra
        ) {
            throw new UnsupportedOperationException("only builds ordinals");
        }
    }

    /**
     * A segment's worked out ordinals as the field data cache holds them. Not field data anyone reads: it only carries
     * the ordinals and their size.
     */
    private static final class MaterializedOrdinals implements LeafFieldData {
        private final ColumnarSegmentOrdinals ordinals;

        MaterializedOrdinals(ColumnarSegmentOrdinals ordinals) {
            this.ordinals = ordinals;
        }

        @Override
        public long ramBytesUsed() {
            return ordinals.ramBytesUsed();
        }

        @Override
        public DocValuesScriptFieldFactory getScriptFieldFactory(String name) {
            throw new UnsupportedOperationException("only carries ordinals");
        }

        @Override
        public SortableBinaryDocValues getBytesValues() {
            throw new UnsupportedOperationException("only carries ordinals");
        }
    }
}
