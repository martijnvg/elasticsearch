/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ingest.lookup;

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.fst.ByteSequenceOutputs;
import org.apache.lucene.util.fst.FST;

import java.io.IOException;

class LookupLoader implements Accountable {

    private final IndexInput fstIn;
    private final long filePointer;

    private FST<BytesRef> fst;

    LookupLoader(IndexInput fstIn, long filePointer) {
        this.fstIn = fstIn;
        this.filePointer = filePointer;
    }

    synchronized FST<BytesRef> getLookup() throws IOException {
        if (fst == null) {
            try (IndexInput clone = fstIn.clone()) {
                clone.seek(filePointer);
                fst = new FST<>(clone, ByteSequenceOutputs.getSingleton());
            }
        }
        return fst;
    }

    @Override
    public long ramBytesUsed() {
        return fst != null ? fst.ramBytesUsed() : 0L;
    }
}
