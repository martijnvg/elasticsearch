/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.blockloader.docvalues;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.ByteArrayStreamInput;
import org.elasticsearch.index.mapper.BlockLoader;

import java.io.IOException;
import java.util.function.Predicate;

/**
 * Helper class to read custom binary doc values.
 */
public final class MultiValueSeparateCountBinaryDocValuesReader {

    private final BytesRef scratch = new BytesRef();
    private final ByteArrayStreamInput in = new ByteArrayStreamInput();

    public MultiValueSeparateCountBinaryDocValuesReader() {}

    public void read(BytesRef bytes, long count, BlockLoader.BytesRefBuilder builder) throws IOException {
        if (count == 1) {
            builder.appendBytesRef(bytes);
            return;
        }

        scratch.bytes = bytes.bytes;
        in.reset(bytes.bytes, bytes.offset, bytes.length);
        builder.beginPositionEntry();
        for (int v = 0; v < count; v++) {
            initializeScratch();
            builder.appendBytesRef(scratch);
        }
        builder.endPositionEntry();
    }

    public boolean match(BytesRef bytes, long count, Predicate<BytesRef> predicate) throws IOException {
        if (count == 1) {
            return predicate.test(bytes);
        }

        scratch.bytes = bytes.bytes;
        in.reset(bytes.bytes, bytes.offset, bytes.length);
        for (int v = 0; v < count; v++) {
            initializeScratch();
            if (predicate.test(scratch)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Reads all values from the binary doc values and returns the minimum value.
     *
     * @param bytes the binary doc values bytes
     * @param count the number of values encoded in the bytes
     * @return the minimum BytesRef value, or null if count is 0
     * @throws IOException if reading fails
     */
    public BytesRef readMin(BytesRef bytes, long count) throws IOException {
        if (count == 1) {
            return bytes;
        }
        if (count == 0) {
            return null;
        }

        scratch.bytes = bytes.bytes;
        in.reset(bytes.bytes, bytes.offset, bytes.length);

        // Read the first value as the initial minimum
        initializeScratch();
        BytesRef min = BytesRef.deepCopyOf(scratch);

        // Compare with remaining values
        for (int v = 1; v < count; v++) {
            initializeScratch();
            if (scratch.compareTo(min) < 0) {
                min = BytesRef.deepCopyOf(scratch);
            }
        }

        return min;
    }

    private void initializeScratch() throws IOException {
        scratch.length = in.readVInt();
        scratch.offset = in.getPosition();
        in.setPosition(scratch.offset + scratch.length);
    }
}
