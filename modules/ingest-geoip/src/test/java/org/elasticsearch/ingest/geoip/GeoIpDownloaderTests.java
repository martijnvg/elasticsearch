/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.ingest.geoip;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkAction;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchResponseSections;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.ThreadPool;
import org.mockito.Mockito;

import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.ingest.geoip.GeoIpDownloader.DATABASES_INDEX;

public class GeoIpDownloaderTests extends ESTestCase {

    public void testDownload() throws Exception {
        HttpClient httpClient = Mockito.mock(HttpClient.class);
        ClusterService clusterService = Mockito.mock(ClusterService.class);

        String uuid = "11111111-1111-1111-1111-111111111111";
        String endpoint = "endpoint.test";

        ThreadPool threadPool = new ThreadPool(Settings.builder().put(Node.NODE_NAME_SETTING.getKey(), "test").build());

        Mockito.when(clusterService.getClusterSettings()).thenReturn(new ClusterSettings(Settings.EMPTY,
            Set.of(GeoIpDownloader.ENDPOINT_SETTING, GeoIpDownloader.POLL_INTERVAL_SETTING)));
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).build();
        Mockito.when(clusterService.state()).thenReturn(state);
        Mockito.when(httpClient.getString(endpoint + "?key=" + uuid)).thenReturn("[{\n" +
            "        \"md5_hash\": \"ed1025ec2735230653db05864cfdfd21\",\n" +
            "        \"name\": \"GeoLite2-City.mmdb.gz\",\n" +
            "        \"provider\": \"maxmind\",\n" +
            "        \"updated\": 1609286450,\n" +
            "        \"url\": \"" + endpoint + "/data\"}]");
        Mockito.when(httpClient.getBytes(endpoint + "/data")).thenReturn(new byte[]{1, 2, 3});

        try (MockClient client = new MockClient("testDownload")) {
            GeoIpDownloader geoIpDownloader = new GeoIpDownloader(client, httpClient, clusterService, threadPool, Settings.EMPTY);
            geoIpDownloader.setEndpoint(endpoint);
            AtomicBoolean success = new AtomicBoolean();
            geoIpDownloader.updateDatabases(() -> success.set(true));
            assertTrue(success.get());
            Mockito.verify(httpClient).getBytes(endpoint + "/data");
        }

        threadPool.shutdownNow();
    }

    private static class MockClient extends NoOpClient {

        private MockClient(String testName) {
            super(testName);
        }

        @SuppressWarnings("unchecked")
        @Override
        protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(ActionType<Response> action,
                                                                                                  Request request,
                                                                                                  ActionListener<Response> listener) {
            if (action == SearchAction.INSTANCE) {
                SearchRequest searchRequest = (SearchRequest) request;
                assertArrayEquals(new String[]{DATABASES_INDEX}, searchRequest.indices());
                SearchHits hits = new SearchHits(new SearchHit[0], new TotalHits(0, TotalHits.Relation.EQUAL_TO), 0);
                SearchResponseSections internal = new SearchResponseSections(hits, null, null, false, false, null, 0);
                SearchResponse s = new SearchResponse(internal, null, 0, 1, 0, 0, null, SearchResponse.Clusters.EMPTY);
                listener.onResponse((Response) s);
            } else if (action == BulkAction.INSTANCE) {
                BulkRequest bulkRequest = (BulkRequest) request;
                List<DocWriteRequest<?>> requests = bulkRequest.requests();
                assertEquals(1, requests.size());
                IndexRequest writeRequest = (IndexRequest) requests.get(0);
                assertEquals(DATABASES_INDEX, writeRequest.index());
                assertEquals("GeoLite2-City.mmdb", writeRequest.id());
                Map<String, Object> source = writeRequest.sourceAsMap();
                Object data = source.get("data");
                if (data instanceof String) {
                    data = Base64.getDecoder().decode((String) data);
                }
                assertArrayEquals(new byte[]{1, 2, 3}, (byte[]) data);
                listener.onResponse((Response) new BulkResponse(new BulkItemResponse[]{}, 100));
            } else {
                throw new IllegalStateException("unexpected action called [" + action.name() + "]");
            }
        }
    }
}
