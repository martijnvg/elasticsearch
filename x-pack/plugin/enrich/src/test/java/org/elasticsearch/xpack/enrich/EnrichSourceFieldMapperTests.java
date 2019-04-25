/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.enrich;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.LeafReader;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class EnrichSourceFieldMapperTests extends ESSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(EnrichPlugin.class);
    }

    public void testEnrichFieldMapper() throws Exception {
        String mapping = "{\"_source\": {\"enabled\": false},\"_enrich_source\": {\"enabled\": true}, \"dynamic\": false}";
        CreateIndexRequest createIndexRequest = new CreateIndexRequest("enrich");
        createIndexRequest.mapping("_doc", mapping, XContentType.JSON);
        client().admin().indices().create(createIndexRequest).actionGet();

        IndexRequest indexRequest = new IndexRequest("enrich");
        indexRequest.source("{\"globalRank\": 25, \"tldRank\": 7, \"tld\": \"co\"}", XContentType.JSON);
        indexRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        client().index(indexRequest).actionGet();

        Index index = getInstanceFromNode(ClusterService.class).state().metaData().index("enrich").getIndex();
        IndicesService indicesServices = getInstanceFromNode(IndicesService.class);
        IndexService indexService = indicesServices.indexService(index);
        IndexShard indexShard = indexService.getShard(0);
        try (Engine.Searcher searcher = indexShard.acquireSearcher(getClass().getSimpleName())) {
            LeafReader leafReader = searcher.getDirectoryReader().leaves().get(0).reader();
            BinaryDocValues binaryDocValues = leafReader.getBinaryDocValues(EnrichSourceFieldMapper.NAME);
            assertThat(binaryDocValues, notNullValue());
            assertThat(binaryDocValues.advanceExact(0), is(true));
            Map<String, Object> result = XContentHelper.convertToMap(
                new BytesArray(binaryDocValues.binaryValue()), false, XContentType.SMILE).v2();
            assertThat(result.size(), equalTo(3));
            assertThat(result.get("globalRank"), equalTo(25));
            assertThat(result.get("tldRank"), equalTo(7));
            assertThat(result.get("tld"), equalTo("co"));
        }
    }

    public void testDisabled() throws Exception {
        CreateIndexRequest createIndexRequest = new CreateIndexRequest("enrich");
        if (randomBoolean()) {
            String mapping = "{\"_source\": {\"enabled\": false},\"_enrich_source\": {\"enabled\": false}, \"dynamic\": false}";
            createIndexRequest.mapping("_doc", mapping, XContentType.JSON);
        }
        client().admin().indices().create(createIndexRequest).actionGet();

        IndexRequest indexRequest = new IndexRequest("enrich");
        indexRequest.source("{\"globalRank\": 25, \"tldRank\": 7, \"tld\": \"co\"}", XContentType.JSON);
        indexRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        client().index(indexRequest).actionGet();

        Index index = getInstanceFromNode(ClusterService.class).state().metaData().index("enrich").getIndex();
        IndicesService indicesServices = getInstanceFromNode(IndicesService.class);
        IndexService indexService = indicesServices.indexService(index);
        IndexShard indexShard = indexService.getShard(0);
        try (Engine.Searcher searcher = indexShard.acquireSearcher(getClass().getSimpleName())) {
            LeafReader leafReader = searcher.getDirectoryReader().leaves().get(0).reader();
            BinaryDocValues binaryDocValues = leafReader.getBinaryDocValues(EnrichSourceFieldMapper.NAME);
            assertThat(binaryDocValues, nullValue());
        }
    }

}
