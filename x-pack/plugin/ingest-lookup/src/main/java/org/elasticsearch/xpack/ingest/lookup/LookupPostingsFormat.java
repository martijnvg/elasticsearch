/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ingest.lookup;

import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;

import java.io.IOException;
import java.util.Objects;

public class LookupPostingsFormat extends PostingsFormat {

    static final String CODEC_NAME = "lookup";
    static final int LOOKUP_CODEC_VERSION = 1;
    static final int LOOKUP_VERSION_CURRENT = LOOKUP_CODEC_VERSION;
    static final String FST_EXTENSION = "lookup.fst";
    static final String INDEX_EXTENSION = "lookup.idx";

    public LookupPostingsFormat() {
        super(CODEC_NAME);
    }

    @Override
    public FieldsConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
        PostingsFormat delegatePostingsFormat = Objects.requireNonNull(PostingsFormat.forName("Lucene50"));
        return new LookupFieldConsumer(delegatePostingsFormat, state);
    }

    @Override
    public FieldsProducer fieldsProducer(SegmentReadState state) throws IOException {
        return new LookupFieldsProducer(state);
    }
}
