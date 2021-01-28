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

import com.maxmind.geoip2.DatabaseReader;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;

import java.io.ByteArrayInputStream;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;

@ClusterScope(scope = Scope.TEST)
public class GeoIpDownloaderIT extends AbstractGeoIpIT {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(IngestGeoIpPlugin.class, GeoIpProcessorNonIngestNodeIT.IngestGeoIpSettingsPlugin.class);
    }

    public void testGeoIpDatabasesDownload() throws Exception {
        assertBusy(() -> {
            SearchResponse searchResponse = client()
                .prepareSearch(GeoIpDownloader.DATABASES_INDEX)
                .setIndicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN_HIDDEN)
                .setSize(0)
                .get();
            assertEquals(3, searchResponse.getHits().getTotalHits().value);
        }, 30, TimeUnit.SECONDS);

        for (String id : List.of("GeoLite2-ASN.mmdb", "GeoLite2-City.mmdb", "GeoLite2-Country.mmdb")) {
            GetResponse res = client().prepareGet(GeoIpDownloader.DATABASES_INDEX, id).get();
            Map<String, Object> source = res.getSourceAsMap();
            assertEquals(Set.of("md5_hash", "updated", "data", "provider", "name"), source.keySet());
            String name = (String) source.get("name");
            assertEquals(id, name);
            Object data = source.get("data");
            if (data instanceof String) {
                data = Base64.getDecoder().decode((String) data);
            }
            byte[] input = (byte[]) data;
            DatabaseReader build = new DatabaseReader.Builder(new GZIPInputStream(new ByteArrayInputStream(input))).build();
            assertEquals(id.replace(".mmdb", ""), build.getMetadata().getDatabaseType());
        }
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
