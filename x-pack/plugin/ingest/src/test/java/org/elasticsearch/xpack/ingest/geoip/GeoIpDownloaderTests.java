/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ingest.geoip;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkAction;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.Set;

public class GeoIpDownloaderTests extends ESTestCase {

    public void testDownload() throws Exception {
        HttpClient httpClient = Mockito.mock(HttpClient.class);
        ClusterService clusterService = Mockito.mock(ClusterService.class);
        Clock clock = Clock.fixed(Instant.now(), ZoneId.systemDefault());

        String uuid = "11111111-1111-1111-1111-111111111111";
        String endpoint = "endpoint.test";

        Mockito.when(clusterService.getClusterSettings()).thenReturn(new ClusterSettings(Settings.EMPTY,
            Set.of(GeoIpDownloader.ENDPOINT_SETTING, GeoIpDownloader.POLL_INTERVAL_SETTING)));
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).build();
        Mockito.when(clusterService.state()).thenReturn(state);
        ArgumentCaptor<ClusterStateUpdateTask> valueCapture = ArgumentCaptor.forClass(ClusterStateUpdateTask.class);
        Mockito.doNothing().when(clusterService).submitStateUpdateTask(Mockito.eq(GeoIpDownloader.JOB_NAME), valueCapture.capture());
        Mockito.when(httpClient.getString(endpoint + "?key=" + uuid)).thenReturn("[{\n" +
            "        \"md5_hash\": \"ed1025ec2735230653db05864cfdfd21\",\n" +
            "        \"name\": \"GeoLite2-City.mmdb.gz\",\n" +
            "        \"provider\": \"maxmind\",\n" +
            "        \"updated\": 1609286450,\n" +
            "        \"url\": \"" + endpoint + "/data\"}]");
        Mockito.when(httpClient.getBytes(endpoint + "/data?key=" + uuid)).thenReturn(new byte[]{1, 2, 3});

        try (MockClient client = new MockClient("testDownload")) {
            GeoIpDownloader geoIpDownloader = new GeoIpDownloader(Settings.EMPTY, client, httpClient, clusterService, clock) {
                @Override
                protected String getLicenseUid() {
                    return uuid;
                }
            };
            geoIpDownloader.setEndpoint(endpoint);
            geoIpDownloader.updateDatabases();
        }
        GeoIpMetadata geoIpMetadata = valueCapture.getValue().execute(state).metadata().custom(GeoIpMetadata.TYPE);
        assertEquals(clock.millis(), geoIpMetadata.getTimestamp());
    }

    private static class MockClient extends NoOpClient {

        public MockClient(String testName) {
            super(testName);
        }

        @SuppressWarnings("unchecked")
        @Override
        protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(ActionType<Response> action,
                                                                                                  Request request,
                                                                                                  ActionListener<Response> listener) {
            if (action == BulkAction.INSTANCE) {
                BulkRequest bulkRequest = (BulkRequest) request;
                List<DocWriteRequest<?>> requests = bulkRequest.requests();
                assertEquals(1, requests.size());
                listener.onResponse((Response) new BulkResponse(new BulkItemResponse[]{}, 100));
            } else {
                throw new IllegalStateException("unexpected action called [" + action.name() + "]");
            }
        }
    }
}
