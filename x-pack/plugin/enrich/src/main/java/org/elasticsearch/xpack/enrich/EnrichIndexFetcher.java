/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.enrich;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateApplier;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.env.ShardLock;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreFileMetaData;
import org.elasticsearch.indices.recovery.MultiFileWriter;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.xpack.core.enrich.EnrichPolicy;
import org.elasticsearch.xpack.enrich.EnrichIndexProvider.LocalEnrichIndex;
import org.elasticsearch.xpack.enrich.action.ChunkEnrichIndexFileAction;
import org.elasticsearch.xpack.enrich.action.ListEnrichIndexFilesAction;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

public final class EnrichIndexFetcher implements ClusterStateApplier {

    private static final Logger logger = LogManager.getLogger(EnrichIndexFetcher.class);
    private static final ByteSizeValue MAX_TRANSFER_SIZE = new ByteSizeValue(10, ByteSizeUnit.MB);

    private final Client client;
    private final Settings settings;
    private final ExecutorService genericTP;
    private final EnrichIndexProvider enrichIndexProvider;

    public EnrichIndexFetcher(Settings settings, Client client, ExecutorService genericTP, EnrichIndexProvider enrichIndexProvider) {
        this.settings = Objects.requireNonNull(settings);
        this.client = Objects.requireNonNull(client);
        this.genericTP = Objects.requireNonNull(genericTP);
        this.enrichIndexProvider = Objects.requireNonNull(enrichIndexProvider);
    }

    public void fetch(String policy, IndexMetaData enrichIndex, Consumer<Exception> handler) throws IOException {
        String indexName = enrichIndex.getIndex().getName();
        LocalEnrichIndex localEnrichIndex = enrichIndexProvider.createEnrichIndexPath(policy);
        Path path = localEnrichIndex.prepareNewIndex(indexName);
        if (localEnrichIndex.newIndex()) {
            handler.accept(null);
        }

        genericTP.execute(new AbstractRunnable() {
            @Override
            public void onFailure(Exception e) {
                try {
                    localEnrichIndex.clearNewIndex();
                } catch (IOException ex) {
                    e.addSuppressed(ex);
                }
                handler.accept(e);
            }

            @Override
            protected void doRun() throws Exception {
                ListEnrichIndexFilesAction.Request request = new ListEnrichIndexFilesAction.Request(indexName);
                ListEnrichIndexFilesAction.Response response =
                    client.execute(ListEnrichIndexFilesAction.INSTANCE, request).actionGet();

                try (Directory directory = FSDirectory.open(path)) {
                    ShardLock shardLock = new ShardLock(new ShardId(enrichIndex.getIndex(), 0)) {
                        @Override
                        protected void closeInternal() {
                            // do nothing
                        }
                    };

                    IndexSettings indexSettings = new IndexSettings(enrichIndex, settings);
                    Store store = new Store(new ShardId(enrichIndex.getIndex(), 0), indexSettings, directory, shardLock);

                    RecoveryState.Index indexRecoveryState = new RecoveryState.Index();
                    try (MultiFileWriter writer = new MultiFileWriter(store, indexRecoveryState, "", logger, () -> {})) {
                        for (StoreFileMetaData file : response.getSnapshot()) {
                            indexRecoveryState.addFileDetail(file.name(), file.length(), false);
                            for (long offset = 0; offset < file.length(); ) {
                                int size = Math.toIntExact(Math.min(MAX_TRANSFER_SIZE.getBytes(), file.length() - offset));
                                ChunkEnrichIndexFileAction.Request chunkRequest =
                                    new ChunkEnrichIndexFileAction.Request(indexName, file.name(), offset, size);
                                ChunkEnrichIndexFileAction.Response chunkResponse =
                                    client.execute(ChunkEnrichIndexFileAction.INSTANCE, chunkRequest).actionGet();

                                int actualChunkSize = chunkResponse.getChunk().length();
                                boolean lastChunk = chunkResponse.getOffset() + actualChunkSize >= file.length();
                                writer.writeFileChunk(file, offset, chunkResponse.getChunk(), lastChunk);
                                offset = chunkResponse.getOffset();
                            }
                        }
                    }

                    localEnrichIndex.swap();
                }

            }
        });
    }

    @Override
    public void applyClusterState(ClusterChangedEvent event) {
        MetaData metaData = event.state().metaData();
        EnrichMetadata enrichMetadata = metaData.custom(EnrichMetadata.TYPE);
        for (EnrichPolicy policy : enrichMetadata.getPolicies().values()) {

        }
    }
}
