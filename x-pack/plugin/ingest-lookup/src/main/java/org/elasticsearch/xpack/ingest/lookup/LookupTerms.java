/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ingest.lookup;

import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.Terms;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.fst.FST;

import java.io.IOException;

public class LookupTerms extends FilterLeafReader.FilterTerms {

    private final LookupLoader lookupLoader;

    LookupTerms(Terms in, LookupLoader lookupLoader) {
        super(in);
        this.lookupLoader = lookupLoader;
    }

    public FST<BytesRef> getLookup() throws IOException {
        return lookupLoader.getLookup();
    }

}
