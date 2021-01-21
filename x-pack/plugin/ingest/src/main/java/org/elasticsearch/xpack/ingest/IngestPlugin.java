/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ingest;

import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;


import java.time.Clock;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

public class IngestPlugin extends Plugin implements org.elasticsearch.plugins.IngestPlugin {

    private final Settings settings;

    public IngestPlugin(Settings settings) {
        this.settings = settings;
    }

    @Override
    public Map<String, Processor.Factory> getProcessors(Processor.Parameters parameters) {
        return Map.of(
            UriPartsProcessor.TYPE,
            new UriPartsProcessor.Factory(),
            CommunityIdProcessor.TYPE,
            new CommunityIdProcessor.Factory()
        );
    }

//    @Override
//    public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
//        return List.of(
//            new NamedWriteableRegistry.Entry(Metadata.Custom.class, GeoIpMetadata.TYPE, GeoIpMetadata::new),
//            new NamedWriteableRegistry.Entry(NamedDiff.class, GeoIpMetadata.TYPE, GeoIpMetadata.GeoIpMetadataMetadataDiff::new));
//    }
//
//    @Override
//    public List<Setting<?>> getSettings() {
//        return List.of(GeoIpDownloader.ENDPOINT_SETTING, GeoIpDownloader.POLL_INTERVAL_SETTING);
//    }
//
//    @Override
//    public Collection<Object> createComponents(Client client, ClusterService clusterService, ThreadPool threadPool,
//                                               ResourceWatcherService resourceWatcherService, ScriptService scriptService,
//                                               NamedXContentRegistry xContentRegistry, Environment environment,
//                                               NodeEnvironment nodeEnvironment, NamedWriteableRegistry namedWriteableRegistry,
//                                               IndexNameExpressionResolver indexNameExpressionResolver,
//                                               Supplier<RepositoriesService> repositoriesServiceSupplier) {
//        new GeoIpDownloader(settings, client, new HttpClient(), clusterService, Clock.systemDefaultZone());
//        return Collections.emptyList();
//    }
}
