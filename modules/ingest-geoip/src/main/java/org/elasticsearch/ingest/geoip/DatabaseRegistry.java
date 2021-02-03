/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.ingest.geoip;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.OriginSettingClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.common.CheckedRunnable;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.ingest.IngestService;
import org.elasticsearch.ingest.PipelineConfiguration;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.watcher.ResourceWatcherService;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.zip.GZIPInputStream;

final class DatabaseRegistry implements Closeable {

    private static final Logger LOGGER = LogManager.getLogger(DatabaseRegistry.class);

    private final Client client;
    private final GeoIpCache cache;
    private final Path geoipTmpDirectory;
    private final IngestService ingestService;
    private final LocalDatabases localDatabases;
    private final Consumer<Runnable> genericExecutor;

    private volatile Map<String, DatabaseReference> databases = Map.of();

    DatabaseRegistry(Environment environment,
                     Client client,
                     GeoIpCache cache,
                     IngestService ingestService,
                     Consumer<Runnable> genericExecutor) {
        this(
            environment.tmpFile(),
            client,
            cache,
            ingestService,
            new LocalDatabases(environment, cache, ingestService),
            genericExecutor
        );
    }

    DatabaseRegistry(Path tmpDir,
                     Client client,
                     GeoIpCache cache,
                     IngestService ingestService,
                     LocalDatabases localDatabases,
                     Consumer<Runnable> genericExecutor) {
        this.client = new OriginSettingClient(client, "geoip");
        this.cache = cache;
        this.geoipTmpDirectory = tmpDir.resolve("geoip-databases");
        this.ingestService = ingestService;
        this.localDatabases = localDatabases;
        this.genericExecutor = genericExecutor;
    }

    public void initialize(ResourceWatcherService resourceWatcher, Supplier<ClusterState> clusterStateSupplier) throws IOException {
        localDatabases.initialize(resourceWatcher, clusterStateSupplier);
        if (Files.exists(geoipTmpDirectory)) {
            Files.walk(geoipTmpDirectory)
                .filter(Files::isRegularFile)
                .peek(path -> LOGGER.info("deleting stale file [{}]", path))
                .forEach(path -> {
                    try {
                        Files.delete(path);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                });
        } else {
            Files.createDirectory(geoipTmpDirectory);
        }
        LOGGER.info("initialized database registry, using geoip-databases directory [{}]", geoipTmpDirectory);
        ingestService.addIngestClusterStateListener(this::checkDatabases);
    }

    public DatabaseReaderLazyLoader getDatabase(String name, boolean fallbackUsingBuiltinDatabases) {
        DatabaseReference reference = databases.get(name);
        DatabaseReaderLazyLoader database = reference != null ? reference.reader: null;
        if (database != null) {
            return database;
        } else if (fallbackUsingBuiltinDatabases) {
            return localDatabases.getDatabase(name);
        } else {
            return null;
        }
    }

    Map<String, DatabaseReaderLazyLoader> getAllDatabases() {
        Map<String, DatabaseReaderLazyLoader> databases = new HashMap<>(localDatabases.getAllDatabases());
        this.databases.forEach((key, value) -> databases.put(key, value.reader));
        return databases;
    }

    @Override
    public void close() throws IOException {
        if (databases != null) {
            IOUtils.close(databases.values());
        }
    }

    void checkDatabases(ClusterState state) {
        DiscoveryNode localNode = state.nodes().getLocalNode();
        if (localNode.isIngestNode() == false) {
            return;
        }

        PersistentTasksCustomMetadata persistentTasks = state.metadata().custom(PersistentTasksCustomMetadata.TYPE);
        if (persistentTasks == null) {
            return;
        }

        IndexRoutingTable databasesIndexRT = state.getRoutingTable().index(GeoIpDownloader.DATABASES_INDEX);
        if (databasesIndexRT == null || databasesIndexRT.allPrimaryShardsActive() == false) {
            return;
        }

        GeoIpTaskState taskState = persistentTasks.findTasks(GeoIpDownloader.GEOIP_DOWNLOADER, task -> true).stream()
            .filter(persistentTask -> persistentTask.getState() != null)
            .map(persistentTask -> (GeoIpTaskState) persistentTask.getState())
            .findAny()
            .orElse(new GeoIpTaskState()); // Empty state will purge stale entries in databases map.

        for (var entry : taskState.getDatabases().entrySet()) {
            String name = entry.getKey();
            GeoIpTaskState.Metadata metadata = entry.getValue();
            DatabaseReference reference = databases.get(entry.getKey());
            Date lastDownloaded = reference != null ? reference.lastDownloaded : null;
            if (lastDownloaded != null && lastDownloaded.after(new Date(metadata.getLastUpdate()))) {
                LOGGER.debug("Current reference of [{}] is up to date [{}] with was recorded in CS [{}]",
                    name, lastDownloaded, new Date(metadata.getLastUpdate()));
                continue;
            }

            try {
                retrieveAndUpdateDatabase(name, metadata, state);
            } catch (Exception e) {
                LOGGER.error((Supplier<?>) () -> new ParameterizedMessage("attempt to download database [{}]", name), e);
            }
        }

        List<String> staleEntries = new ArrayList<>();
        for (var entry : databases.entrySet()) {
            if (taskState.getDatabases().get(entry.getKey()) == null) {
                staleEntries.add(entry.getKey());
            }
        }
        removeStaleEntries(staleEntries, state);
    }

    void retrieveAndUpdateDatabase(String databaseName, GeoIpTaskState.Metadata metadata, ClusterState state) throws IOException {
        final Path databaseTmpGzFile;
        try {
            databaseTmpGzFile = Files.createFile(geoipTmpDirectory.resolve(databaseName + ".tmp.gz"));
        } catch (FileAlreadyExistsException e) {
            LOGGER.debug("database update [{}] already in progress, skipping...", databaseName);
            return;
        }

        final Path databaseTmpFile = Files.createFile(geoipTmpDirectory.resolve(databaseName + ".tmp"));
        LOGGER.info("downloading geoip database [{}] to [{}]", databaseName, databaseTmpGzFile);
        retrieveDatabase(
            databaseName,
            metadata,
            bytes -> Files.write(databaseTmpGzFile, bytes, StandardOpenOption.APPEND),
            () -> {
                LOGGER.debug("decompressing [{}]", databaseTmpGzFile.getFileName());
                decompress(databaseTmpGzFile, databaseTmpFile);

                Path databaseFile = geoipTmpDirectory.resolve(databaseName);
                LOGGER.debug("moving database from [{}] to [{}]", databaseTmpFile, databaseFile);
                Files.move(databaseTmpFile, databaseFile, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
                updateDatabase(databaseName, databaseFile, state);
                Files.delete(databaseTmpGzFile);
            },
            failure -> {
                LOGGER.error((Supplier<?>) () -> new ParameterizedMessage("failed to download database [{}]", databaseName), failure);
                try {
                    Files.delete(databaseTmpGzFile);
                    Files.delete(databaseTmpFile);
                } catch (IOException ioe) {
                    ioe.addSuppressed(failure);
                    LOGGER.error("Unable to delete tmp database file after failure", ioe);
                }
            },
            null);
    }

    void updateDatabase(String databaseFileName, Path file, ClusterState state) {
        try {
            Map<String, DatabaseReference> databases = new HashMap<>(this.databases);
            LOGGER.info("database file changed [{}], reload database...", file);
            DatabaseReaderLazyLoader loader = new DatabaseReaderLazyLoader(cache, file);
            DatabaseReference existing = databases.put(databaseFileName, new DatabaseReference(new Date(), file, loader));
            this.databases = Map.copyOf(databases);
            reloadIngestPipelines(databaseFileName, state);
            if (existing != null) {
                existing.close();
                int numEntriesEvicted = cache.purgeCacheEntriesForDatabase(existing.databaseFile);
                LOGGER.debug("evicted [{}] entries from cache after reloading database [{}]", numEntriesEvicted, file);
            }
        } catch (Exception e) {
            LOGGER.error((Supplier<?>) () -> new ParameterizedMessage("failed to update database [{}]", databaseFileName), e);
        }
    }

    void removeStaleEntries(Collection<String> staleEntries, ClusterState state) {
        try {
            Map<String, DatabaseReference> databases = new HashMap<>(this.databases);
            List<DatabaseReference> references = new ArrayList<>();
            for (String staleEntry : staleEntries) {
                LOGGER.info("database file removed [{}], close database...", staleEntries);
                DatabaseReference existing = databases.remove(staleEntry);
                assert existing != null;
                references.add(existing);
            }
            this.databases = Map.copyOf(databases);

            for (DatabaseReference reference : references) {
                reloadIngestPipelines(reference.databaseFile.getFileName().toString(), state);
                reference.close();
                Files.delete(reference.databaseFile);
                cache.purgeCacheEntriesForDatabase(reference.databaseFile);
            }
        } catch (Exception e) {
            LOGGER.error((Supplier<?>) () -> new ParameterizedMessage("failed to clean database [{}]", staleEntries), e);
        }
    }

    void retrieveDatabase(String databaseName,
                          GeoIpTaskState.Metadata metadata,
                          CheckedConsumer<byte[], IOException> chunkConsumer,
                          CheckedRunnable<Exception> completedHandler,
                          Consumer<Exception> failureHandler,
                          Long chunk) {
        SearchRequest searchRequest = new SearchRequest(GeoIpDownloader.DATABASES_INDEX);
        BoolQueryBuilder boolQuery = new BoolQueryBuilder();
        boolQuery.filter(new TermQueryBuilder("name", databaseName));
        boolQuery.filter(new RangeQueryBuilder("chunk").from(metadata.getFirstChunk()).to(metadata.getLastChunk()));
        searchRequest.source().query(boolQuery);
        searchRequest.source().sort("chunk");
        searchRequest.source().size(10);
        if (chunk != null) {
            searchRequest.source().searchAfter(new Object[]{chunk});
        }
        // Need to run the search from a different thread, since this can be executed from cluster state applier thread:
        // (forking has minimal overhead, because we immediately execute a search)
        genericExecutor.accept(() -> {
            client.search(searchRequest, ActionListener.wrap(
                searchResponse -> {
                    if (searchResponse.getHits().getHits().length == 0) {
                        completedHandler.run();
                    } else {
                        Long lastSortValue = null;
                        for (SearchHit searchHit : searchResponse.getHits()) {
                            byte[] data = (byte[]) searchHit.getSourceAsMap().get("data");
                            chunkConsumer.accept(data);
                            lastSortValue = (Long) searchHit.getSortValues()[0];
                        }
                        retrieveDatabase(databaseName, metadata, chunkConsumer, completedHandler, failureHandler, lastSortValue);
                    }
                },
                failureHandler)
            );
        });
    }

    static void decompress(Path source, Path target) throws IOException {
        try (GZIPInputStream in = new GZIPInputStream(Files.newInputStream(source), 8192)) {
            Files.copy(in, target, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    void reloadIngestPipelines(String databaseName, ClusterState state) throws Exception {
        List<String> pipelinesToReload = findPipelines(databaseName, state, ingestService);
        LOGGER.info("reloading pipelines [{}], because database [{}] changed", pipelinesToReload, databaseName);
        ingestService.reloadPipelines(pipelinesToReload);
    }

    static List<String> findPipelines(String databaseName, ClusterState state, IngestService ingestService) {
        List<PipelineConfiguration> pipelines = IngestService.getPipelines(state);
        List<String> pipelinesToReload = new ArrayList<>();
        for (var pipeline : pipelines) {
            List<GeoIpProcessor> processors = ingestService.getProcessorsInPipeline(pipeline.getId(), GeoIpProcessor.class);
            if (processors.isEmpty() == false) {
                // TODO: also check for used database and only reload pipelines with updated database
                pipelinesToReload.add(pipeline.getId());
            } else {
                // Also check for pipelines with geoip processors that failed to load the database before:
                List<IngestService.FailureProcessor> failureProcessors =
                    ingestService.getProcessorsInPipeline(pipeline.getId(), IngestService.FailureProcessor.class);
                if (failureProcessors.isEmpty() == false) {
                    assert failureProcessors.size() == 1;
                    if (failureProcessors.get(0).getType().equals(GeoIpProcessor.TYPE) &&
                        failureProcessors.get(0).getProperty().equals("database_file")) {
                        pipelinesToReload.add(pipeline.getId());
                    }
                }
            }
        }
        return pipelinesToReload;
    }

    static final class DatabaseReference implements Closeable {

        private final Date lastDownloaded;
        private final Path databaseFile;
        private final DatabaseReaderLazyLoader reader;

        DatabaseReference(Date lastDownloaded, Path databaseFile, DatabaseReaderLazyLoader reader) {
            this.lastDownloaded = lastDownloaded;
            this.databaseFile = databaseFile;
            this.reader = reader;
        }

        @Override
        public void close() throws IOException {
            reader.close();
        }
    }

}
