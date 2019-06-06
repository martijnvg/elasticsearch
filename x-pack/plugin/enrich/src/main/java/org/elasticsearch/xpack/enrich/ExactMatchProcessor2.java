/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.enrich;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.ConstantScoreQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.ingest.AbstractProcessor;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xpack.core.enrich.EnrichPolicy;
import org.elasticsearch.xpack.enrich.EnrichProcessorFactory.EnrichSpecification;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

final class ExactMatchProcessor2 extends AbstractProcessor {

    private final Function<SearchRequest, SearchResponse> searchClient;

    private final String policyName;
    private final EnrichPolicy enrichPolicy;
    private final String enrichKey;
    private final boolean ignoreMissing;
    private final List<EnrichSpecification> specifications;

    private final Cache<String, Map<String, Object>> cache;

    ExactMatchProcessor2(String tag,
                         Function<SearchRequest, SearchResponse> searchClient,
                         String policyName,
                         EnrichPolicy enrichPolicy,
                         String enrichKey,
                         boolean ignoreMissing,
                         List<EnrichSpecification> specifications) {
        super(tag);
        this.searchClient = searchClient;
        this.policyName = policyName;
        this.enrichPolicy = enrichPolicy;
        this.enrichKey = enrichKey;
        this.ignoreMissing = ignoreMissing;
        this.specifications = specifications;
        this.cache = CacheBuilder.<String, Map<String, Object>>builder().setMaximumWeight(5000).build();
    }

    @Override
    public IngestDocument execute(IngestDocument ingestDocument) throws Exception {
        String value = ingestDocument.getFieldValue(enrichKey, String.class);

        Map<String, Object> result = cache.computeIfAbsent(value, key -> {
            TermQueryBuilder termQuery = new TermQueryBuilder(enrichPolicy.getEnrichKey(), value);
            SearchRequest searchRequest = new SearchRequest(EnrichPolicy.getBaseName(policyName));
            searchRequest.allowPartialSearchResults(true);
            searchRequest.source(new SearchSourceBuilder()
                .query(new ConstantScoreQueryBuilder(termQuery))
                .fetchSource(false)
                .trackScores(false)
            );
            searchRequest.source().docValueField("_enrich_source");

            SearchResponse searchResponse = searchClient.apply(searchRequest);
            if (searchResponse.getHits().getTotalHits().value == 1) {
                Map<String, Object> map = new HashMap<>();
                String enrichSource = searchResponse.getHits().getAt(0).getFields().get("_enrich_source").getValue();
                try (InputStream in = new ByteArrayInputStream(Base64.getDecoder().decode(enrichSource))) {
                    Map<String, Object> decoded = XContentHelper.convertToMap(XContentType.SMILE.xContent(), in, false);
                    for (EnrichSpecification extractField : specifications) {
                        Object enrichValue = decoded.get(extractField.sourceField);
                        map.put(extractField.targetField, enrichValue);
                    }
                }
                return map;
            } else {
                return Map.of();
            }
        });
        for (Map.Entry<String, Object> entry : result.entrySet()) {
            ingestDocument.setFieldValue(entry.getKey(), entry.getValue());
        }
        return ingestDocument;
    }

    @Override
    public String getType() {
        return EnrichProcessorFactory.TYPE;
    }

    String getPolicyName() {
        return policyName;
    }

    String getEnrichKey() {
        return enrichKey;
    }

    boolean isIgnoreMissing() {
        return ignoreMissing;
    }

    List<EnrichSpecification> getSpecifications() {
        return specifications;
    }
}
