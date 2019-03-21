/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ingest.lookup;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.Accountables;
import org.apache.lucene.util.IOUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.ingest.lookup.LookupPostingsFormat.CODEC_NAME;
import static org.elasticsearch.xpack.ingest.lookup.LookupPostingsFormat.FST_EXTENSION;
import static org.elasticsearch.xpack.ingest.lookup.LookupPostingsFormat.INDEX_EXTENSION;
import static org.elasticsearch.xpack.ingest.lookup.LookupPostingsFormat.LOOKUP_CODEC_VERSION;
import static org.elasticsearch.xpack.ingest.lookup.LookupPostingsFormat.LOOKUP_VERSION_CURRENT;

public class LookupFieldsProducer extends FieldsProducer {

    private final FieldsProducer delegateFieldsProducer;
    private final Map<String, LookupLoader> readers;
    private IndexInput fstIn;

    // copy ctr for merge instance
    private LookupFieldsProducer(FieldsProducer delegateFieldsProducer, Map<String, LookupLoader> readers) {
        this.delegateFieldsProducer = delegateFieldsProducer;
        this.readers = readers;
    }

    LookupFieldsProducer(SegmentReadState state) throws IOException {
        boolean success = false;
        FieldsProducer delegateFieldsProducer = null;
        String indexFile = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, INDEX_EXTENSION);
        try (ChecksumIndexInput index = state.directory.openChecksumInput(indexFile, state.context)) {
            // open up dict file containing all fsts
            String dictFile = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, FST_EXTENSION);
            fstIn = state.directory.openInput(dictFile, state.context);
            CodecUtil.checkIndexHeader(fstIn, CODEC_NAME, LOOKUP_CODEC_VERSION, LOOKUP_VERSION_CURRENT, state.segmentInfo.getId(), state.segmentSuffix);
            // just validate the footer for the dictIn
            CodecUtil.retrieveChecksum(fstIn);

            // open up index file (fieldNumber, offset)
            CodecUtil.checkIndexHeader(index, CODEC_NAME, LOOKUP_CODEC_VERSION, LOOKUP_VERSION_CURRENT, state.segmentInfo.getId(), state.segmentSuffix);
            // load delegate PF
            PostingsFormat delegatePostingsFormat = PostingsFormat.forName(index.readString());
            delegateFieldsProducer = delegatePostingsFormat.fieldsProducer(state);

            // read suggest field numbers and their offsets in the terms file from index
            int numFields = index.readVInt();
            readers = new HashMap<>(numFields);
            for (int i = 0; i < numFields; i++) {
                int fieldNumber = index.readVInt();
                long filePointer = index.readVLong();
                FieldInfo fieldInfo = state.fieldInfos.fieldInfo(fieldNumber);
                // we don't load the FST yet
                readers.put(fieldInfo.name, new LookupLoader(fstIn, filePointer));
            }
            CodecUtil.checkFooter(index);
            success = true;
            this.delegateFieldsProducer = delegateFieldsProducer;
        } finally {
            if (success == false) {
                IOUtils.closeWhileHandlingException(delegateFieldsProducer, fstIn);
            }
        }
    }

    @Override
    public void close() throws IOException {
        boolean success = false;
        try {
            delegateFieldsProducer.close();
            IOUtils.close(fstIn);
            success = true;
        } finally {
            if (success == false) {
                IOUtils.closeWhileHandlingException(delegateFieldsProducer, fstIn);
            }
        }
    }

    @Override
    public void checkIntegrity() throws IOException {
        delegateFieldsProducer.checkIntegrity();
    }

    @Override
    public Iterator<String> iterator() {
        return readers.keySet().iterator();
    }

    @Override
    public Terms terms(String field) throws IOException {
        Terms in = delegateFieldsProducer.terms(field);
        return new LookupTerms(in, readers.get(field));
    }

    @Override
    public int size() {
        return readers.size();
    }

    @Override
    public FieldsProducer getMergeInstance() throws IOException {
        return new LookupFieldsProducer(delegateFieldsProducer, readers);
    }

    @Override
    public long ramBytesUsed() {
        long ramBytesUsed = delegateFieldsProducer.ramBytesUsed();
        for (LookupLoader reader : readers.values()) {
            ramBytesUsed += reader.ramBytesUsed();
        }
        return ramBytesUsed;
    }

    @Override
    public Collection<Accountable> getChildResources() {
        List<Accountable> accountableList = new ArrayList<>();
        for (Map.Entry<String, LookupLoader> readerEntry : readers.entrySet()) {
            accountableList.add(Accountables.namedAccountable(readerEntry.getKey(), readerEntry.getValue()));
        }
        return Collections.unmodifiableCollection(accountableList);
    }
}
