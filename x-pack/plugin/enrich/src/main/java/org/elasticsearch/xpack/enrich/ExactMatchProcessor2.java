/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.enrich;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.client.Client;
import org.elasticsearch.ingest.AbstractProcessor;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.xpack.core.enrich.EnrichPolicy;
import org.elasticsearch.xpack.enrich.EnrichProcessorFactory.EnrichSpecification;
import org.elasticsearch.xpack.enrich.action.DummyAction;

import java.util.List;

final class ExactMatchProcessor2 extends AbstractProcessor {

    private final Client client;

    private final String policyName;
    private final EnrichPolicy enrichPolicy;
    private final String enrichKey;
    private final boolean ignoreMissing;
    private final List<EnrichSpecification> specifications;

    ExactMatchProcessor2(String tag,
                         Client client,
                         String policyName,
                         EnrichPolicy enrichPolicy,
                         String enrichKey,
                         boolean ignoreMissing,
                         List<EnrichSpecification> specifications) {
        super(tag);
        this.client = client;
        this.policyName = policyName;
        this.enrichPolicy = enrichPolicy;
        this.enrichKey = enrichKey;
        this.ignoreMissing = ignoreMissing;
        this.specifications = specifications;
    }

    @Override
    public IngestDocument execute(IngestDocument ingestDocument) throws Exception {
        String value = ingestDocument.getFieldValue(enrichKey, String.class);

        DummyAction.Request request =
            new DummyAction.Request(EnrichPolicy.getBaseName(policyName), enrichPolicy.getEnrichKey(), new BytesRef(value));
        DummyAction.Response response = client.execute(DummyAction.INSTANCE, request).actionGet();
        if (response.getResponse().isEmpty() == false) {
            for (EnrichSpecification extractField : specifications) {
                Object val = response.getResponse().get(extractField.sourceField);
                if (val != null) {
                    ingestDocument.setFieldValue(extractField.targetField, val);
                }
            }
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
