/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.fielddata.plain;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.fielddata.ReaderDependentOrdinalsFieldData;
import org.elasticsearch.index.fielddata.SortableBinaryDocValues;
import org.elasticsearch.index.fielddata.plain.ColumnarOrdinalsIndexFieldData.SegmentKind;
import org.elasticsearch.index.mapper.BinaryDocValuesFormat;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.script.field.ToScriptFieldFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.EnumMap;
import java.util.Map;

/**
 * A keyword field written by the columnar codec, read by value as {@link BytesBinaryIndexFieldData} reads it, which can
 * also offer ordinals for a reader whose segments all have them to offer.
 *
 * <p>The columnar codec decides for each segment whether a column gets a dictionary, and a dictionary need not name
 * every value, so whether a field has ordinals is a question about a reader rather than about the mapping. A merged
 * segment of a low cardinality field usually has a dictionary that names every value, while a freshly flushed one may
 * be stored plainly or have values escape its dictionary. Those are small, so their ordinals are worked out when they
 * are first read, up to {@link IndexSettings#COLUMNAR_GLOBAL_ORDINALS_MAX_MATERIALIZED_VALUES_SETTING} values a segment.
 * A reader with any segment past that, or one not written by the codec, is read by value, as it is when
 * {@link IndexSettings#COLUMNAR_GLOBAL_ORDINALS_ENABLED_SETTING} is off.
 */
public final class ColumnarKeywordIndexFieldData extends BytesBinaryIndexFieldData implements ReaderDependentOrdinalsFieldData {

    private static final Logger logger = LogManager.getLogger(ColumnarKeywordIndexFieldData.class);

    private final IndexSettings indexSettings;
    private final IndexFieldDataCache cache;
    private final CircuitBreakerService breakerService;

    private ColumnarKeywordIndexFieldData(
        String fieldName,
        ValuesSourceType valuesSourceType,
        ToScriptFieldFactory<SortableBinaryDocValues> toScriptFieldFactory,
        IndexVersion indexVersion,
        IndexSettings indexSettings,
        BinaryDocValuesFormat binaryFormat,
        IndexFieldDataCache cache,
        CircuitBreakerService breakerService
    ) {
        super(fieldName, valuesSourceType, toScriptFieldFactory, indexVersion, binaryFormat);
        this.indexSettings = indexSettings;
        this.cache = cache;
        this.breakerService = breakerService;
    }

    @Override
    public IndexFieldData<?> forReader(IndexReader reader) {
        if (indexSettings.isColumnarGlobalOrdinalsEnabled() == false || reader instanceof DirectoryReader == false) {
            return this;
        }
        final long maxMaterializedValues = indexSettings.getColumnarGlobalOrdinalsMaxMaterializedValues();
        final Map<SegmentKind, Integer> kinds = new EnumMap<>(SegmentKind.class);
        try {
            for (LeafReaderContext leaf : reader.leaves()) {
                final SegmentKind kind = ColumnarOrdinalsIndexFieldData.classify(leaf.reader(), fieldName, maxMaterializedValues);
                kinds.merge(kind, 1, Integer::sum);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        if (kinds.containsKey(SegmentKind.UNSUPPORTED)) {
            logger.debug("field [{}] is read by value, as not every segment can offer ordinals: {}", fieldName, kinds);
            return this;
        }
        logger.trace("field [{}] is read through ordinals: {}", fieldName, kinds);
        return new ColumnarOrdinalsIndexFieldData(this, cache, breakerService, maxMaterializedValues);
    }

    /** Builds {@link ColumnarKeywordIndexFieldData} for a keyword field the columnar codec writes. */
    public static class Builder implements IndexFieldData.Builder {
        private final String name;
        private final ValuesSourceType valuesSourceType;
        private final ToScriptFieldFactory<SortableBinaryDocValues> toScriptFieldFactory;
        private final IndexVersion indexVersion;
        private final IndexSettings indexSettings;
        private final BinaryDocValuesFormat binaryFormat;

        public Builder(
            String name,
            ValuesSourceType valuesSourceType,
            ToScriptFieldFactory<SortableBinaryDocValues> toScriptFieldFactory,
            IndexVersion indexVersion,
            IndexSettings indexSettings,
            BinaryDocValuesFormat binaryFormat
        ) {
            this.name = name;
            this.valuesSourceType = valuesSourceType;
            this.toScriptFieldFactory = toScriptFieldFactory;
            this.indexVersion = indexVersion;
            this.indexSettings = indexSettings;
            this.binaryFormat = binaryFormat;
        }

        @Override
        public ColumnarKeywordIndexFieldData build(IndexFieldDataCache cache, CircuitBreakerService breakerService) {
            return new ColumnarKeywordIndexFieldData(
                name,
                valuesSourceType,
                toScriptFieldFactory,
                indexVersion,
                indexSettings,
                binaryFormat,
                cache,
                breakerService
            );
        }
    }
}
