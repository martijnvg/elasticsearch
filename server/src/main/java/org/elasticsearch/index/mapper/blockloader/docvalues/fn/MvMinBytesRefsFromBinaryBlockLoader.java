/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.blockloader.docvalues.fn;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.mapper.blockloader.ConstantNull;
import org.elasticsearch.index.mapper.blockloader.docvalues.BlockDocValuesReader;
import org.elasticsearch.index.mapper.blockloader.docvalues.MultiValueSeparateCountBinaryDocValuesReader;

import java.io.IOException;

import static org.elasticsearch.index.mapper.MultiValuedBinaryDocValuesField.SeparateCount.COUNT_FIELD_SUFFIX;

/**
 * Loads the MIN {@code keyword} in each doc from binary doc values (high cardinality fields).
 */
public class MvMinBytesRefsFromBinaryBlockLoader extends BlockDocValuesReader.DocValuesBlockLoader {
    private final String fieldName;

    public MvMinBytesRefsFromBinaryBlockLoader(String fieldName) {
        this.fieldName = fieldName;
    }

    @Override
    public Builder builder(BlockFactory factory, int expectedCount) {
        return factory.bytesRefs(expectedCount);
    }

    @Override
    public AllReader reader(LeafReaderContext context) throws IOException {
        BinaryDocValues values = context.reader().getBinaryDocValues(fieldName);
        if (values == null) {
            return ConstantNull.READER;
        }

        String countsFieldName = fieldName + COUNT_FIELD_SUFFIX;
        DocValuesSkipper countsSkipper = context.reader().getDocValuesSkipper(countsFieldName);
        assert countsSkipper != null : "no skipper for counts field [" + countsFieldName + "]";
        if (countsSkipper.minValue() == 1 && countsSkipper.maxValue() == 1) {
            return new SingleValued(values);
        }

        NumericDocValues counts = context.reader().getNumericDocValues(countsFieldName);
        return new MvMinBytesRefsFromBinary(values, counts);
    }

    @Override
    public String toString() {
        return "MvMinBytesRefsFromBinary[" + fieldName + "]";
    }

    private static class SingleValued extends BlockDocValuesReader {
        private final BinaryDocValues docValues;

        SingleValued(BinaryDocValues docValues) {
            this.docValues = docValues;
        }

        @Override
        public Block read(BlockFactory factory, Docs docs, int offset, boolean nullsFiltered) throws IOException {
            if (docs.count() - offset == 1) {
                return readSingleDoc(factory, docs.get(offset));
            }

            try (var builder = factory.bytesRefs(docs.count() - offset)) {
                for (int i = offset; i < docs.count(); i++) {
                    int doc = docs.get(i);
                    if (false == docValues.advanceExact(doc)) {
                        builder.appendNull();
                    } else {
                        builder.appendBytesRef(docValues.binaryValue());
                    }
                }
                return builder.build();
            }
        }

        @Override
        public void read(int docId, StoredFields storedFields, Builder builder) throws IOException {
            if (false == docValues.advanceExact(docId)) {
                builder.appendNull();
            } else {
                ((BytesRefBuilder) builder).appendBytesRef(docValues.binaryValue());
            }
        }

        private Block readSingleDoc(BlockFactory factory, int docId) throws IOException {
            if (docValues.advanceExact(docId) == false) {
                return factory.constantNulls(1);
            }
            BytesRef v = docValues.binaryValue();
            return factory.constantBytes(BytesRef.deepCopyOf(v), 1);
        }

        @Override
        public int docId() {
            return docValues.docID();
        }

        @Override
        public String toString() {
            return "MvMinBytesRefsFromBinary.SingleValued";
        }
    }

    private static class MvMinBytesRefsFromBinary extends BlockDocValuesReader {
        private final BinaryDocValues docValues;
        private final NumericDocValues counts;
        private final MultiValueSeparateCountBinaryDocValuesReader reader = new MultiValueSeparateCountBinaryDocValuesReader();

        MvMinBytesRefsFromBinary(BinaryDocValues docValues, NumericDocValues counts) {
            this.docValues = docValues;
            this.counts = counts;
        }

        @Override
        public Block read(BlockFactory factory, Docs docs, int offset, boolean nullsFiltered) throws IOException {
            if (docs.count() - offset == 1) {
                return readSingleDoc(factory, docs.get(offset));
            }

            try (BytesRefBuilder builder = factory.bytesRefs(docs.count() - offset)) {
                for (int i = offset; i < docs.count(); i++) {
                    int doc = docs.get(i);
                    read(doc, builder);
                }
                return builder.build();
            }
        }

        @Override
        public void read(int docId, StoredFields storedFields, Builder builder) throws IOException {
            read(docId, (BytesRefBuilder) builder);
        }

        private Block readSingleDoc(BlockFactory factory, int docId) throws IOException {
            if (docValues.advanceExact(docId) == false) {
                return factory.constantNulls(1);
            }

            boolean advancedCounts = counts.advanceExact(docId);
            assert advancedCounts;
            long count = counts.longValue();

            BytesRef min = readMin(docValues.binaryValue(), count);
            return factory.constantBytes(BytesRef.deepCopyOf(min), 1);
        }

        private void read(int docId, BytesRefBuilder builder) throws IOException {
            if (false == docValues.advanceExact(docId)) {
                builder.appendNull();
                return;
            }

            boolean advancedCounts = counts.advanceExact(docId);
            assert advancedCounts;
            long count = counts.longValue();

            BytesRef min = readMin(docValues.binaryValue(), count);
            builder.appendBytesRef(min);
        }

        private BytesRef readMin(BytesRef bytes, long count) throws IOException {
            return reader.readMin(bytes, count);
        }

        @Override
        public int docId() {
            return docValues.docID();
        }

        @Override
        public String toString() {
            return "MvMinBytesRefsFromBinary";
        }
    }
}
