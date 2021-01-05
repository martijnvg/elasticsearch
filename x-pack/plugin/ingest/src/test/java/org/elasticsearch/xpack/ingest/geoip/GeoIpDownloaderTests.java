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
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsAction;
import org.elasticsearch.action.bulk.BulkAction;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.license.License;
import org.elasticsearch.license.LicenseService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.mockito.Mockito;

import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;

public class GeoIpDownloaderTests extends ESTestCase {

    public void testDownload() throws IOException {
        LicenseService licenseService = Mockito.mock(LicenseService.class);
        License license = Mockito.mock(License.class);
        HttpClient httpClient = Mockito.mock(HttpClient.class);
        ClusterService clusterService = Mockito.mock(ClusterService.class);
        Clock clock = Clock.fixed(Instant.now(), ZoneId.of("UTC"));

        String uuid = "11111111-1111-1111-1111-111111111111";
        String endpoint = "endpoint.test";

        Mockito.when(clusterService.state()).thenReturn(ClusterState.builder(ClusterName.DEFAULT).build());
        Mockito.when(licenseService.getLicense()).thenReturn(license);
        Mockito.when(license.uid()).thenReturn(uuid);
        Mockito.when(httpClient.getString(endpoint + "?key=" + uuid)).thenReturn("[{\n" +
            "        \"md5_hash\": \"ed1025ec2735230653db05864cfdfd21\",\n" +
            "        \"name\": \"GeoLite2-City.mmdb.gz\",\n" +
            "        \"provider\": \"maxmind\",\n" +
            "        \"updated\": 1609286450,\n" +
            "        \"url\": \"" + endpoint + "/data\"}]");
        Mockito.when(httpClient.getBytes(endpoint + "/data?key=" + uuid)).thenReturn(new byte[]{1, 2, 3});

        try (MockClient client = new MockClient("testDownload")) {
            GeoIpDownloader geoIpDownloader = new GeoIpDownloader(licenseService, client, httpClient, clusterService, clock);
            geoIpDownloader.setEndpoint(endpoint);
            geoIpDownloader.updateDatabases();
        }
    }

    private static class MockClient extends NoOpClient {

        public MockClient(String testName) {
            super(testName);
        }

        @Override
        protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(ActionType<Response> action,
                                                                                                  Request request,
                                                                                                  ActionListener<Response> listener) {
            if (action == ClusterUpdateSettingsAction.INSTANCE) {
                listener.onResponse((Response) AcknowledgedResponse.TRUE);
            } else if (action == BulkAction.INSTANCE) {
                BulkRequest bulkRequest = (BulkRequest) request;
                List<DocWriteRequest<?>> requests = bulkRequest.requests();
                assertEquals(1, requests.size());
                assertEquals(requests.get(0), );
                listener.onResponse((Response) new BulkResponse(new BulkItemResponse[]{
                }, 100));
            } else {
                throw new IllegalStateException("unexpected action called [" + action.name() + "]");
            }
        }
    }
}
