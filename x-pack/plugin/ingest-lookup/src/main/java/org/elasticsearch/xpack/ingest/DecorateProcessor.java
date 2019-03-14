/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ingest;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.PrefixQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.ingest.AbstractProcessor;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

public final class DecorateProcessor extends AbstractProcessor {

    public static final String TYPE = "decorate";

    private final Function<SearchRequest, SearchResponse> localSearchClient;

    DecorateProcessor(String tag, Function<SearchRequest, SearchResponse> localSearchClient) {
        super(tag);
        this.localSearchClient = localSearchClient;
    }

    @Override
    public IngestDocument execute(IngestDocument ingestDocument) throws Exception {
        final String sourceField = "Domain";
        final String targetField = "GlobalRank";
        final String indexName = "lookup";
        final String domainField = "Domain";
        final String[] extractFields = new String[] {"GlobalRank"};

        String value = ingestDocument.getFieldValue(sourceField, String.class);
        TermQueryBuilder termQuery = new TermQueryBuilder(domainField, value);
        SearchRequest searchRequest = new SearchRequest(indexName);
        searchRequest.allowPartialSearchResults(true);
        searchRequest.source(new SearchSourceBuilder()
            .query(termQuery)
            .fetchSource(false)
        );
        for (String extractField : extractFields) {
            searchRequest.source().docValueField(extractField);
        }

        SearchResponse searchResponse = localSearchClient.apply(searchRequest);
        if (searchResponse.getHits().getTotalHits().value == 1) {
            Map<String, Object> additionalData = new HashMap<>();
            for (String extractField : extractFields) {
                additionalData.put(extractField, searchResponse.getHits().getAt(0).field(extractField).getValue());
            }
            ingestDocument.setFieldValue(targetField, additionalData);
        } else if (searchResponse.getHits().getTotalHits().value > 1) {
            throw new RuntimeException();
        }
        return ingestDocument;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    public static final class Factory implements Processor.Factory {

        private final Function<SearchRequest, SearchResponse> localSearchClient;

        public Factory(Function<SearchRequest, SearchResponse> localSearchClient) {
            this.localSearchClient = localSearchClient;
        }

        @Override
        public Processor create(Map<String, Processor.Factory> processorFactories, String tag, Map<String, Object> config) throws Exception {
            return new DecorateProcessor(tag, localSearchClient);
        }
    }
}
