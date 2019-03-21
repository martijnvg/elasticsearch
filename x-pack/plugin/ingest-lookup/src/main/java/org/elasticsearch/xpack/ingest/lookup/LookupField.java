/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ingest.lookup;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;

public class LookupField extends Field {

    private static final FieldType FIELD_TYPE = new FieldType();
    static {
        FIELD_TYPE.setTokenized(false);
        FIELD_TYPE.setStored(false);
        FIELD_TYPE.setStoreTermVectors(false);
        FIELD_TYPE.setOmitNorms(true);
        FIELD_TYPE.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
        FIELD_TYPE.freeze();
    }

    private final BytesRef out;

    LookupField(String name, BytesRef in, BytesRef out) {
        super(name, in, FIELD_TYPE);
        this.out = out;
    }

    @Override
    public TokenStream tokenStream(Analyzer analyzer, TokenStream reuse) {
        TokenStream in = super.tokenStream(analyzer, reuse);
        PayloadTokenStream payloadTokenStream;
        if (in instanceof PayloadTokenStream) {
            payloadTokenStream = (PayloadTokenStream) in;
        } else {
            payloadTokenStream = new PayloadTokenStream(in);
        }
        payloadTokenStream.payload = BytesRef.deepCopyOf(out);
        return payloadTokenStream;
    }

    public final static class PayloadTokenStream extends TokenFilter {

        private final PayloadAttribute payloadAttr = addAttribute(PayloadAttribute.class);

        private BytesRef payload;

        PayloadTokenStream(TokenStream input) {
            super(input);
        }

        @Override
        public boolean incrementToken() throws IOException {
            if (input.incrementToken()) {
                payloadAttr.setPayload(payload);
                return true;
            } else {
                return false;
            }
        }
    }
}
