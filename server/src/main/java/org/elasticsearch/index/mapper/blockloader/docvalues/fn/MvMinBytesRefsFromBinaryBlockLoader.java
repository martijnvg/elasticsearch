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
import org.elasticsearch.index.mapper.blockloader.docvalues.BytesRefsFromBinaryBlockLoader;
import org.elasticsearch.index.mapper.blockloader.docvalues.BytesRefsFromCustomBinaryBlockLoader;
import org.elasticsearch.index.mapper.blockloader.docvalues.MultiValueSeparateCountBinaryDocValuesReader;

import java.io.IOException;

import static org.elasticsearch.index.mapper.MultiValuedBinaryDocValuesField.SeparateCount.COUNT_FIELD_SUFFIX;

/**
 * Loads the MIN {@code keyword} in each doc using separate-count binary doc values.
 */
public class MvMinBytesRefsFromBinaryBlockLoader extends BlockDocValuesReader.DocValuesBlockLoader {
    private final String fieldName;

    public MvMinBytesRefsFromBinaryBlockLoader(String fieldName) {
        this.fieldName = fieldName;
    }

    @Override
    public BytesRefBuilder builder(BlockFactory factory, int expectedCount) {
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
            return BytesRefsFromBinaryBlockLoader.createReader(values);
        }

        NumericDocValues counts = context.reader().getNumericDocValues(countsFieldName);
        if (counts == null) {
            return ConstantNull.READER;
        }
        return new MinBinaryWithSeparateCounts(values, counts);
    }

    @Override
    public String toString() {
        return "MvMinBytesRefsFromBinary[" + fieldName + "]";
    }

    private static class MinBinaryWithSeparateCounts extends BytesRefsFromCustomBinaryBlockLoader.AbstractBytesRefsFromBinary {
        private final NumericDocValues counts;
        private final MultiValueSeparateCountBinaryDocValuesReader reader = new MultiValueSeparateCountBinaryDocValuesReader();

        MinBinaryWithSeparateCounts(BinaryDocValues values, NumericDocValues counts) {
            super(values);
            this.counts = counts;
        }

        @Override
        public void read(int docId, BytesRefBuilder builder) throws IOException {
            if (false == docValues.advanceExact(docId)) {
                builder.appendNull();
                return;
            }

            boolean advanced = counts.advanceExact(docId);
            assert advanced;

            long valueCount = counts.longValue();
            if (valueCount <= 0) {
                builder.appendNull();
                return;
            }

            BytesRef bytes = docValues.binaryValue();
            if (valueCount == 1) {
                builder.appendBytesRef(bytes);
                return;
            }
            BytesRef min = reader.min(bytes, valueCount);
            if (min == null) {
                builder.appendNull();
            } else {
                builder.appendBytesRef(min);
            }
        }

        @Override
        public String toString() {
            return "MvMinBytesRefsFromBinary.SeparateCounts";
        }
    }

}
