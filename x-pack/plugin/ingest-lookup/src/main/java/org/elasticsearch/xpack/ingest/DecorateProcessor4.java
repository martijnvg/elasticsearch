/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ingest;

import org.apache.lucene.index.Terms;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IntsRefBuilder;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.Util;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.ingest.AbstractProcessor;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.xpack.ingest.lookup.LookupTerms;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

public final class DecorateProcessor4 extends AbstractProcessor {

    public static final String TYPE = "decorate4";

    private final Function<String, Engine.Searcher> searcherProvider;
    private final BiFunction<String, String, IndexFieldData<?>> fieldDataProvider;

    DecorateProcessor4(String tag,
                       Function<String, Engine.Searcher> searcherProvider,
                       BiFunction<String, String, IndexFieldData<?>> fieldDataProvider) {
        super(tag);
        this.searcherProvider = searcherProvider;
        this.fieldDataProvider = fieldDataProvider;
    }

    @Override
    public IngestDocument execute(IngestDocument ingestDocument) throws Exception {
        final String domainField = "Domain";
        final String targetField = "lookup";
        final String indexName = "lookup2";
        final String fieldName = "lookup";
        final String[] extractFields = new String[] {"GlobalRank"};

        final String value = ingestDocument.getFieldValue(domainField, String.class);
        Engine.Searcher searcher = ingestDocument.searcherMap.computeIfAbsent(indexName, searcherProvider);
        if (searcher.getDirectoryReader().leaves().size() > 1) {
            throw new RuntimeException("lookup shard should only have 1 segment");
        }

        Terms terms = searcher.getDirectoryReader().leaves().get(0).reader().terms(fieldName);
        if (terms != null) {
            LookupTerms lookupTerms = (LookupTerms) terms;
            FST<BytesRef> lookup = lookupTerms.getLookup();

            Map<String, Object> additionalData = new HashMap<>();

            IntsRefBuilder intsSpare = new IntsRefBuilder();
            intsSpare.copyUTF8Bytes(new BytesRef(value));
            BytesRef result = Util.get(lookup, intsSpare.get());
            if (result != null) {
                additionalData.put(extractFields[0], result.utf8ToString());
            }

            ingestDocument.setFieldValue(targetField, additionalData);
        }

        return ingestDocument;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    public static final class Factory implements Processor.Factory {

        private final Function<String, Engine.Searcher> searcherProvider;
        private final BiFunction<String, String, IndexFieldData<?>> fieldDateProvider;

        Factory(Function<String, Engine.Searcher> searcherProvider,
                       BiFunction<String, String, IndexFieldData<?>> fieldDateProvider) {
            this.searcherProvider = searcherProvider;
            this.fieldDateProvider = fieldDateProvider;
        }

        @Override
        public Processor create(Map<String, Processor.Factory> processorFactories, String tag, Map<String, Object> config) throws Exception {
            return new DecorateProcessor4(tag, searcherProvider, fieldDateProvider);
        }
    }
}
