/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.ingest.geoip;

import com.maxmind.db.NodeCache;
import com.maxmind.geoip2.model.AbstractResponse;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.Version;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.persistent.PersistentTaskParams;
import org.elasticsearch.persistent.PersistentTaskState;
import org.elasticsearch.persistent.PersistentTasksExecutor;
import org.elasticsearch.plugins.IngestPlugin;
import org.elasticsearch.plugins.PersistentTaskPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SystemIndexPlugin;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.mapper.MapperService.SINGLE_MAPPING_NAME;
import static org.elasticsearch.ingest.geoip.GeoIpDownloader.DATABASES_INDEX;
import static org.elasticsearch.ingest.geoip.GeoIpDownloader.GEOIP_DOWNLOADER;

public class IngestGeoIpPlugin extends Plugin implements IngestPlugin, SystemIndexPlugin, Closeable, PersistentTaskPlugin {
    public static final Setting<Long> CACHE_SIZE =
        Setting.longSetting("ingest.geoip.cache_size", 1000, 0, Setting.Property.NodeScope);

    static String[] DEFAULT_DATABASE_FILENAMES = new String[]{"GeoLite2-ASN.mmdb", "GeoLite2-City.mmdb", "GeoLite2-Country.mmdb"};

    private final SetOnce<DatabaseRegistry> databaseRegistry = new SetOnce<>();

    @Override
    public List<Setting<?>> getSettings() {
        return Arrays.asList(CACHE_SIZE, GeoIpDownloader.ENDPOINT_SETTING, GeoIpDownloader.POLL_INTERVAL_SETTING);
    }

    @Override
    public Map<String, Processor.Factory> getProcessors(Processor.Parameters parameters) {
        long cacheSize = CACHE_SIZE.get(parameters.env.settings());
        try {
            DatabaseRegistry registry = new DatabaseRegistry(parameters.env, parameters.client, parameters.ingestService);
            databaseRegistry.set(registry);
            registry.initialize();
            return Map.of(GeoIpProcessor.TYPE, new GeoIpProcessor.Factory(registry, new GeoIpCache(cacheSize)));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws IOException {
        databaseRegistry.get().close();
    }

    /**
     * The in-memory cache for the geoip data. There should only be 1 instance of this class..
     * This cache differs from the maxmind's {@link NodeCache} such that this cache stores the deserialized Json objects to avoid the
     * cost of deserialization for each lookup (cached or not). This comes at slight expense of higher memory usage, but significant
     * reduction of CPU usage.
     */
    // TODO: cache should be purged when database files are updated.
    // (The geoip cache should be managed from the database registry)
    static class GeoIpCache {
        private final Cache<CacheKey<?>, AbstractResponse> cache;

        //package private for testing
        GeoIpCache(long maxSize) {
            if (maxSize < 0) {
                throw new IllegalArgumentException("geoip max cache size must be 0 or greater");
            }
            this.cache = CacheBuilder.<CacheKey<?>, AbstractResponse>builder().setMaximumWeight(maxSize).build();
        }

        <T extends AbstractResponse> T putIfAbsent(InetAddress ip, Class<T> responseType,
                                                   Function<InetAddress, AbstractResponse> retrieveFunction) {

            //can't use cache.computeIfAbsent due to the elevated permissions for the jackson (run via the cache loader)
            CacheKey<T> cacheKey = new CacheKey<>(ip, responseType);
            //intentionally non-locking for simplicity...it's OK if we re-put the same key/value in the cache during a race condition.
            AbstractResponse response = cache.get(cacheKey);
            if (response == null) {
                response = retrieveFunction.apply(ip);
                cache.put(cacheKey, response);
            }
            return responseType.cast(response);
        }

        //only useful for testing
        <T extends AbstractResponse> T get(InetAddress ip, Class<T> responseType) {
            CacheKey<T> cacheKey = new CacheKey<>(ip, responseType);
            return responseType.cast(cache.get(cacheKey));
        }

        /**
         * The key to use for the cache. Since this cache can span multiple geoip processors that all use different databases, the response
         * type is needed to be included in the cache key. For example, if we only used the IP address as the key the City and ASN the same
         * IP may be in both with different values and we need to cache both. The response type scopes the IP to the correct database
         * provides a means to safely cast the return objects.
         *
         * @param <T> The AbstractResponse type used to scope the key and cast the result.
         */
        private static class CacheKey<T extends AbstractResponse> {

            private final InetAddress ip;
            private final Class<T> responseType;

            private CacheKey(InetAddress ip, Class<T> responseType) {
                this.ip = ip;
                this.responseType = responseType;
            }

            //generated
            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;
                CacheKey<?> cacheKey = (CacheKey<?>) o;
                return Objects.equals(ip, cacheKey.ip) &&
                    Objects.equals(responseType, cacheKey.responseType);
            }

            //generated
            @Override
            public int hashCode() {
                return Objects.hash(ip, responseType);
            }
        }
    }

    @Override
    public List<PersistentTasksExecutor<?>> getPersistentTasksExecutor(ClusterService clusterService, ThreadPool threadPool,
                                                                       Client client, SettingsModule settingsModule,
                                                                       IndexNameExpressionResolver expressionResolver) {
        HttpClient httpClient = new HttpClient();
        return List.of(new GeoIpDownloader(client, httpClient, clusterService, threadPool, settingsModule.getSettings()));
    }

    @Override
    public List<NamedXContentRegistry.Entry> getNamedXContent() {
        return List.of(new NamedXContentRegistry.Entry(PersistentTaskParams.class, new ParseField(GEOIP_DOWNLOADER),
            GeoIpTaskParams::fromXContent));
    }

    @Override
    public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        return List.of(new NamedWriteableRegistry.Entry(PersistentTaskState.class, GEOIP_DOWNLOADER, GeoIpDownloaderTaskState::new),
            new NamedWriteableRegistry.Entry(PersistentTaskParams.class, GEOIP_DOWNLOADER, GeoIpTaskParams::new));
    }

    @Override
    public Collection<SystemIndexDescriptor> getSystemIndexDescriptors(Settings settings) {
        SystemIndexDescriptor geoipDatabasesIndex = SystemIndexDescriptor.builder()
            .setIndexPattern(DATABASES_INDEX)
            .setDescription("GeoIP databases")
            .setMappings(mappings())
            .setOrigin("geoip")
            .setVersionMetaKey("version")
            .setPrimaryIndex(DATABASES_INDEX)
            .build();
        return Collections.singleton(geoipDatabasesIndex);
    }

    private static XContentBuilder mappings() {
        try {
            return jsonBuilder()
                .startObject()
                    .startObject(SINGLE_MAPPING_NAME)
                        .startObject("_meta")
                            .field("version", Version.CURRENT)
                        .endObject()
                        .field("dynamic", "strict")
                        .startObject("properties")
                            .startObject("name")
                                .field("type", "keyword")
                            .endObject()
                            .startObject("md5_hash")
                                .field("type", "keyword")
                            .endObject()
                            .startObject("updated")
                                .field("type", "date")
                            .endObject()
                            .startObject("data")
                                .field("type", "binary")
                            .endObject()
                            .startObject("provider")
                                .field("type", "keyword")
                            .endObject()
                        .endObject()
                    .endObject()
                .endObject();
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to build mappings for " + DATABASES_INDEX, e);
        }
    }
}
