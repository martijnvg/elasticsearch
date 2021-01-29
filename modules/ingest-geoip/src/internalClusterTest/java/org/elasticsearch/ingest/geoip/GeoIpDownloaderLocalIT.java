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

package org.elasticsearch.ingest.geoip;

import com.sun.net.httpserver.HttpServer;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.mocksocket.MockHttpServer;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@ClusterScope(scope = Scope.TEST)
@SuppressForbidden(reason = "fix test to not use com.sun.net.httpserver.HttpServer, which isn't portable on all JVMs")
public class GeoIpDownloaderLocalIT extends AbstractGeoIpIT {

    private static HttpServer mockServer;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(IngestGeoIpPlugin.class, IngestGeoIpSettingsPlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(GeoIpDownloader.ENDPOINT_SETTING.getKey(), getServerAddress() + "/databases")
            .build();
    }

    private static String getServerAddress() {
        InetSocketAddress address = mockServer.getAddress();
        return String.format(Locale.ROOT, "http://%s:%d", address.getHostName(), address.getPort());
    }

    @BeforeClass
    public static void setupMockHttpServer() throws IOException {
        mockServer = MockHttpServer.createHttp(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        mockServer.createContext("/databases", exchange -> {
            String data = String.format("[\n" +
                "  {\n" +
                "    \"md5_hash\": \"61a09e631020091f049b528a17c8db03\",\n" +
                "    \"name\": \"db.mmdb.gz\",\n" +
                "    \"provider\": \"maxmind\",\n" +
                "    \"updated\": 1611619252,\n" +
                "    \"url\": \"%s\"\n" +
                "  }\n" +
                "]", getServerAddress() + "/db.mmdb.gz");
            exchange.sendResponseHeaders(200, data.length());
            exchange.getResponseBody().write(data.getBytes(StandardCharsets.UTF_8));
            exchange.getResponseBody().close();
            exchange.close();
        });
        mockServer.createContext("/db.mmdb.gz", exchange -> {
            exchange.sendResponseHeaders(200, 3);
            exchange.getResponseBody().write(new byte[]{1, 2, 3});
            exchange.getResponseBody().close();
            exchange.close();
        });
        mockServer.createContext("/", exchange -> {
            exchange.sendResponseHeaders(500, 0);
            exchange.close();
        });
        mockServer.start();
    }

    @AfterClass
    public static void cleanupMockHttpServer() {
        mockServer.stop(1);
    }

    public void testGeoIpDatabasesDownload() throws Exception {
        assertBusy(() -> {
            try {
                SearchResponse searchResponse = client()
                    .prepareSearch(GeoIpDownloader.DATABASES_INDEX)
                    .setIndicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN_HIDDEN)
                    .setSize(0)
                    .get();
                assertEquals(1, searchResponse.getHits().getTotalHits().value);
            } catch (Exception e) {
                fail(e.getMessage());
            }
        }, 30, TimeUnit.SECONDS);

        String id = "db.mmdb";
        GetResponse res = client().prepareGet(GeoIpDownloader.DATABASES_INDEX, id).get();
        Map<String, Object> source = res.getSourceAsMap();
        assertEquals(Set.of("md5_hash", "updated", "data", "provider", "name"), source.keySet());
        String name = (String) source.get("name");
        assertEquals(id, name);
        Object data = source.get("data");
        if (data instanceof String) {
            data = Base64.getDecoder().decode((String) data);
        }
        assertArrayEquals(new byte[]{1, 2, 3}, (byte[]) data);

    }

    @Override
    protected int maximumNumberOfReplicas() {
        return 0;
    }

    @Override
    protected int maximumNumberOfShards() {
        return 1;
    }
}
