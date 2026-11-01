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
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.columnar.string.StringColumnReader;
import org.elasticsearch.columnar.string.StringColumnSource;

import java.io.IOException;
import java.util.Arrays;

/**
 * A columnar keyword column read as {@link SortedSetDocValues}, through the {@link ColumnarSegmentOrdinals} of its
 * segment.
 *
 * <p>The column keeps a document's values in the order they arrived, duplicates and nulls included, while sorted set
 * doc values promise each document's distinct values in ordinal order. So a document's ordinals are gathered from its
 * slots, nulls dropped, and sorted and deduplicated, which only a document holding several values pays for. A column of
 * one slot a document is read as {@link SortedDocValues}, so that callers can unwrap it and take their single valued
 * paths. A document whose slots are all null has no value, as it has none for every other read of the field.
 */
final class ColumnarOrdinalsDocValues {

    private ColumnarOrdinalsDocValues() {}

    /** Ordinals over {@code values}, which must be a {@link StringColumnSource} that {@code ordinals} was made for. */
    static SortedSetDocValues open(BinaryDocValues values, ColumnarSegmentOrdinals ordinals) {
        final StringColumnSource source = (StringColumnSource) values;
        if (source.reader().hasValueAddresses() == false) {
            return DocValues.singleton(new Single(values, source, ordinals));
        }
        return new Multi(values, source, ordinals);
    }

    /** A column of one slot a document. */
    private static final class Single extends SortedDocValues {
        private final BinaryDocValues values;
        private final StringColumnSource source;
        private final StringColumnReader reader;
        private final ColumnarSegmentOrdinals ordinals;
        private final int valueCount;
        private final BytesRef scratch = new BytesRef();
        private int ordinal = -1;

        Single(BinaryDocValues values, StringColumnSource source, ColumnarSegmentOrdinals ordinals) {
            this.values = values;
            this.source = source;
            this.reader = source.reader();
            this.ordinals = ordinals;
            this.valueCount = Math.toIntExact(ordinals.valueCount(reader));
        }

        /** Reads the ordinal of the document the column is on, and answers whether it has a value. */
        private boolean load() throws IOException {
            ordinal = (int) ordinals.singleOrdinal(source, reader);
            return ordinal >= 0;
        }

        @Override
        public boolean advanceExact(int target) throws IOException {
            return values.advanceExact(target) && load();
        }

        @Override
        public int nextDoc() throws IOException {
            return skipToValue(values.nextDoc());
        }

        @Override
        public int advance(int target) throws IOException {
            return skipToValue(values.advance(target));
        }

        private int skipToValue(int doc) throws IOException {
            while (doc != DocIdSetIterator.NO_MORE_DOCS && load() == false) {
                doc = values.nextDoc();
            }
            return doc;
        }

        @Override
        public int docID() {
            return values.docID();
        }

        @Override
        public long cost() {
            return values.cost();
        }

        @Override
        public int ordValue() {
            return ordinal;
        }

        @Override
        public BytesRef lookupOrd(int ord) throws IOException {
            return ordinals.lookupOrd(reader, ord, scratch);
        }

        @Override
        public int getValueCount() {
            return valueCount;
        }
    }

    /** A column whose documents may hold several slots. */
    private static final class Multi extends SortedSetDocValues {
        private final BinaryDocValues values;
        private final StringColumnSource source;
        private final StringColumnReader reader;
        private final ColumnarSegmentOrdinals ordinals;
        private final long valueCount;
        private final BytesRef scratch = new BytesRef();
        private long[] documentOrdinals = new long[1];
        private int count;
        private int upto;

        Multi(BinaryDocValues values, StringColumnSource source, ColumnarSegmentOrdinals ordinals) {
            this.values = values;
            this.source = source;
            this.reader = source.reader();
            this.ordinals = ordinals;
            this.valueCount = ordinals.valueCount(reader);
        }

        /** Gathers the distinct ordinals of the document the column is on, and answers whether it has any. */
        private boolean load() throws IOException {
            final int slots = source.slotCount();
            documentOrdinals = ArrayUtil.grow(documentOrdinals, slots);
            count = 0;
            for (int slot = 0; slot < slots; slot++) {
                final long ordinal = ordinals.ordinalAt(reader, source.slotAddress(slot));
                if (ordinal >= 0) {
                    documentOrdinals[count++] = ordinal;
                }
            }
            if (count > 1) {
                Arrays.sort(documentOrdinals, 0, count);
                int distinct = 1;
                for (int i = 1; i < count; i++) {
                    if (documentOrdinals[i] != documentOrdinals[distinct - 1]) {
                        documentOrdinals[distinct++] = documentOrdinals[i];
                    }
                }
                count = distinct;
            }
            upto = 0;
            return count > 0;
        }

        @Override
        public boolean advanceExact(int target) throws IOException {
            return values.advanceExact(target) && load();
        }

        @Override
        public int nextDoc() throws IOException {
            return skipToValue(values.nextDoc());
        }

        @Override
        public int advance(int target) throws IOException {
            return skipToValue(values.advance(target));
        }

        private int skipToValue(int doc) throws IOException {
            while (doc != DocIdSetIterator.NO_MORE_DOCS && load() == false) {
                doc = values.nextDoc();
            }
            return doc;
        }

        @Override
        public int docID() {
            return values.docID();
        }

        @Override
        public long cost() {
            return values.cost();
        }

        @Override
        public long nextOrd() {
            assert upto < count : "read past the document's [" + count + "] ordinals";
            return documentOrdinals[upto++];
        }

        @Override
        public int docValueCount() {
            return count;
        }

        @Override
        public BytesRef lookupOrd(long ord) throws IOException {
            return ordinals.lookupOrd(reader, ord, scratch);
        }

        @Override
        public long getValueCount() {
            return valueCount;
        }
    }
}
