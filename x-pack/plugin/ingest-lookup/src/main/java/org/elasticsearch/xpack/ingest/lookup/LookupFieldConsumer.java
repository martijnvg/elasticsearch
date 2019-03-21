/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ingest.lookup;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.NormsProducer;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.IntsRefBuilder;
import org.apache.lucene.util.fst.ByteSequenceOutputs;
import org.apache.lucene.util.fst.FST;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

class LookupFieldConsumer extends FieldsConsumer {

    private final IndexOutput fstOut;
    private final SegmentWriteState state;
    private final String delegatePostingsFormatName;
    private final FieldsConsumer delegateFieldsConsumer;
    private final Map<String, LookupMetaData> seenFields = new HashMap<>();

    LookupFieldConsumer(PostingsFormat delegatePostingsFormat, SegmentWriteState state) throws IOException {
        this.state = state;
        this.delegatePostingsFormatName = delegatePostingsFormat.getName();

        boolean success = false;
        IndexOutput fstOut = null;
        FieldsConsumer delegateFieldsConsumer = null;
        try {
            delegateFieldsConsumer = delegatePostingsFormat.fieldsConsumer(state);
            String dictFile = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, LookupPostingsFormat.FST_EXTENSION);
            fstOut = state.directory.createOutput(dictFile, state.context);
            CodecUtil.writeIndexHeader(fstOut, LookupPostingsFormat.CODEC_NAME, LookupPostingsFormat.LOOKUP_VERSION_CURRENT,
                state.segmentInfo.getId(), state.segmentSuffix);
            success = true;
        } finally {
            this.fstOut = fstOut;
            this.delegateFieldsConsumer = delegateFieldsConsumer;
            if (success == false) {
                IOUtils.closeWhileHandlingException(fstOut, delegateFieldsConsumer);
            }
        }
    }

    @Override
    public void write(Fields fields, NormsProducer norms) throws IOException {
        delegateFieldsConsumer.write(fields, norms);

        for (String field : fields) {
            Terms terms = fields.terms(field);
            if (terms == null) {
                continue;
            }

            PostingsEnum postingsEnum = null;
            final IntsRefBuilder intsSpare = new IntsRefBuilder();
            final org.apache.lucene.util.fst.Builder<BytesRef> builder =
                new org.apache.lucene.util.fst.Builder<>(FST.INPUT_TYPE.BYTE4, ByteSequenceOutputs.getSingleton());
            TermsEnum termsEnum = terms.iterator();

            // write terms
            for (BytesRef term = termsEnum.next(); term != null; term = termsEnum.next()) {
                intsSpare.copyUTF8Bytes(term);

                // Currently assume that each term has a single payload
                postingsEnum = termsEnum.postings(postingsEnum, PostingsEnum.PAYLOADS);
                postingsEnum.nextDoc();
                assert postingsEnum.docID() != DocIdSetIterator.NO_MORE_DOCS;
                assert postingsEnum.freq() == 1;
                postingsEnum.nextPosition();
                assert postingsEnum.getPayload() != null;
                BytesRef payload = BytesRef.deepCopyOf(postingsEnum.getPayload());
                builder.add(intsSpare.get(), payload);
            }

            final FST<BytesRef> fst = builder.finish();
            if (fst != null) {
                final long filePointer = fstOut.getFilePointer();
                fst.save(fstOut);
                seenFields.put(field, new LookupMetaData(filePointer));
            }
        }
    }

    private boolean closed = false;

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        String indexFile = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, LookupPostingsFormat.INDEX_EXTENSION);
        boolean success = false;
        try (IndexOutput indexOut = state.directory.createOutput(indexFile, state.context)) {
            delegateFieldsConsumer.close();
            CodecUtil.writeIndexHeader(indexOut, LookupPostingsFormat.CODEC_NAME, LookupPostingsFormat.LOOKUP_VERSION_CURRENT,
                state.segmentInfo.getId(), state.segmentSuffix);

            /*
             * we write the delegate postings format name so we can load it
             * without getting an instance in the ctor
             */
            indexOut.writeString(delegatePostingsFormatName);
            // write # of seen fields
            indexOut.writeVInt(seenFields.size());
            // write field numbers and dictOut offsets
            for (Map.Entry<String, LookupMetaData> seenField : seenFields.entrySet()) {
                FieldInfo fieldInfo = state.fieldInfos.fieldInfo(seenField.getKey());
                indexOut.writeVInt(fieldInfo.number);
                LookupMetaData metaData = seenField.getValue();
                indexOut.writeVLong(metaData.filePointer);
            }
            CodecUtil.writeFooter(indexOut);
            CodecUtil.writeFooter(fstOut);
            IOUtils.close(fstOut);
            success = true;
        } finally {
            if (success == false) {
                IOUtils.closeWhileHandlingException(fstOut, delegateFieldsConsumer);
            }
        }
    }

    private static class LookupMetaData {
        private final long filePointer;

        private LookupMetaData(long filePointer) {
            this.filePointer = filePointer;
        }
    }
}
