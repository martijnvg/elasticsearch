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
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.OriginSettingClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.common.CheckedRunnable;
import org.elasticsearch.common.hash.MessageDigests;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.ingest.IngestService;
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
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;
import java.util.zip.GZIPInputStream;

/**
 * A component that is responsible for making the databases maintained by {@link GeoIpDownloader}
 * available for ingest processors.
 *
 * Also provided a lookup mechanism for geoip processors with fallback to {@link LocalDatabases}.
 * All databases are downloaded into a geoip tmp directory, which is created at node startup.
 *
 * The following high level steps are executed after each cluster state update:
 * 1) Check which databases are available in {@link GeoIpTaskState},
 *    which is part of the geoip downloader persistent task.
 * 2) For each database check whether the databases have changed
 *    by comparing the local and remote md5 hash or are locally missing.
 * 3) For each database identified in step 2 start downloading the database
 *    chunks. Each chunks is appended to a tmp file (inside geoip tmp dir) and
 *    after all chunks have been downloaded, the database is uncompressed and
 *    renamed to the final filename.After this the database is loaded and
 *    if there is an old instance of this database then that is closed.
 * 4) Cleanup locally loaded databases that are no longer mentioned in {@link GeoIpTaskState}.
 */
final class DatabaseRegistry implements Closeable {

    private static final Logger LOGGER = LogManager.getLogger(DatabaseRegistry.class);

    private final Client client;
    private final GeoIpCache cache;
    private final Path geoipTmpDirectory;
    private final LocalDatabases localDatabases;
    private final Consumer<Runnable> genericExecutor;

    private final ConcurrentMap<String, DatabaseReference> databases = new ConcurrentHashMap<>();

    DatabaseRegistry(Environment environment, Client client, GeoIpCache cache, Consumer<Runnable> genericExecutor) {
        this(
            environment.tmpFile(),
            new OriginSettingClient(client, "geoip"),
            cache,
            new LocalDatabases(environment, cache),
            genericExecutor
        );
    }

    DatabaseRegistry(Path tmpDir,
                     Client client,
                     GeoIpCache cache,
                     LocalDatabases localDatabases,
                     Consumer<Runnable> genericExecutor) {
        this.client = client;
        this.cache = cache;
        this.geoipTmpDirectory = tmpDir.resolve("geoip-databases");
        this.localDatabases = localDatabases;
        this.genericExecutor = genericExecutor;
    }

    public void initialize(ResourceWatcherService resourceWatcher, IngestService ingestService) throws IOException {
        localDatabases.initialize(resourceWatcher);
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

    public DatabaseReaderLazyLoader getDatabase(String name, boolean fallbackUsingDefaultDatabases) {
        // There is a need for reference counting in order to avoid using an instance
        // that gets closed while using it. (this can happen during a database update)
        while (true) {
            DatabaseReference reference = databases.get(name);
            DatabaseReaderLazyLoader instance = reference != null ? reference.reader: null;
            if (instance == null) {
                instance = localDatabases.getDatabase(name, fallbackUsingDefaultDatabases);
            }
            if (instance == null || instance.preLookup()) {
                return instance;
            }
            // instance is closed after incrementing its usage,
            // drop this instance and fetch another one.
        }
    }

    List<DatabaseReaderLazyLoader> getAllDatabases() {
        List<DatabaseReaderLazyLoader> all = new ArrayList<>(localDatabases.getAllDatabases());
        this.databases.forEach((key, value) -> all.add(value.reader));
        return all;
    }

    @Override
    public void close() throws IOException {
        IOUtils.close(databases.values());
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

        PersistentTasksCustomMetadata.PersistentTask<?> task =
            PersistentTasksCustomMetadata.getTaskWithId(state, GeoIpDownloader.GEOIP_DOWNLOADER);
        // Empty state will purge stale entries in databases map.
        GeoIpTaskState taskState = task == null || task.getState() == null ? GeoIpTaskState.EMPTY : (GeoIpTaskState) task.getState();

        taskState.getDatabases().forEach((name, metadata) -> {
            DatabaseReference reference = databases.get(name);
            String remoteMd5 = metadata.getMd5();
            String localMd5 = reference != null ? reference.md5 : null;
            if (Objects.equals(localMd5, remoteMd5)) {
                LOGGER.debug("Current reference of [{}] is up to date [{}] with was recorded in CS [{}]", name, localMd5, remoteMd5);
                return;
            }

            try {
                retrieveAndUpdateDatabase(name, metadata);
            } catch (Exception e) {
                LOGGER.error((Supplier<?>) () -> new ParameterizedMessage("attempt to download database [{}]", name), e);
            }
        });

        List<String> staleEntries = new ArrayList<>(databases.keySet());
        staleEntries.removeAll(taskState.getDatabases().keySet());
        removeStaleEntries(staleEntries);
    }

    void retrieveAndUpdateDatabase(String databaseName, GeoIpTaskState.Metadata metadata) throws IOException {
        final String recordedMd5 = metadata.getMd5();

        // This acts as a lock, if this method for a specific db is executed later and downloaded for this db is still ongoing then
        // FileAlreadyExistsException is thrown and this method silently returns.
        // (this method is never invoked concurrently and is invoked by a cluster state applier thread)
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
            recordedMd5,
            metadata,
            bytes -> Files.write(databaseTmpGzFile, bytes, StandardOpenOption.APPEND),
            () -> {
                LOGGER.debug("decompressing [{}]", databaseTmpGzFile.getFileName());
                decompress(databaseTmpGzFile, databaseTmpFile);

                Path databaseFile = geoipTmpDirectory.resolve(databaseName);
                LOGGER.debug("moving database from [{}] to [{}]", databaseTmpFile, databaseFile);
                Files.move(databaseTmpFile, databaseFile, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
                updateDatabase(databaseName, recordedMd5, databaseFile);
                Files.delete(databaseTmpGzFile);
            },
            failure -> {
                LOGGER.error((Supplier<?>) () -> new ParameterizedMessage("failed to download database [{}]", databaseName), failure);
                try {
                    Files.delete(databaseTmpFile);
                    Files.delete(databaseTmpGzFile);
                } catch (IOException ioe) {
                    ioe.addSuppressed(failure);
                    LOGGER.error("Unable to delete tmp database file after failure", ioe);
                }
            });
    }

    void updateDatabase(String databaseFileName, String recordedMd5, Path file) {
        try {
            LOGGER.info("database file changed [{}], reload database...", file);
            DatabaseReaderLazyLoader loader = new DatabaseReaderLazyLoader(cache, file);
            DatabaseReference existing = databases.put(databaseFileName, new DatabaseReference(recordedMd5, file, loader));
            if (existing != null) {
                existing.close();
            }
        } catch (Exception e) {
            LOGGER.error((Supplier<?>) () -> new ParameterizedMessage("failed to update database [{}]", databaseFileName), e);
        }
    }

    void removeStaleEntries(Collection<String> staleEntries) {
        try {
            for (String staleEntry : staleEntries) {
                LOGGER.info("database [{}] no longer exists, cleaning up...", staleEntry);
                DatabaseReference existing = databases.remove(staleEntry);
                assert existing != null;
                existing.close();
                Files.delete(existing.databaseFile);
            }
        } catch (Exception e) {
            LOGGER.error((Supplier<?>) () -> new ParameterizedMessage("failed to clean database [{}]", staleEntries), e);
        }
    }

    void retrieveDatabase(String databaseName,
                          String expectedMd5,
                          GeoIpTaskState.Metadata metadata,
                          CheckedConsumer<byte[], IOException> chunkConsumer,
                          CheckedRunnable<Exception> completedHandler,
                          Consumer<Exception> failureHandler) {
        // Need to run the search from a different thread, since this is executed from cluster state applier thread:
        genericExecutor.accept(() -> {
            MessageDigest md = MessageDigests.md5();
            Long lastSortValue = null;

            try {
                do {
                    SearchRequest searchRequest =
                        createSearchRequest(databaseName, metadata.getFirstChunk(), metadata.getLastChunk(), lastSortValue);
                    lastSortValue = null;

                    // At most once a day a few searches may be executed to fetch the new files,
                    // so it is ok if this happens in a blocking manner on a thead from generic thread pool.
                    // This makes the code easier to understand and maintain.
                    SearchResponse searchResponse = client.search(searchRequest).actionGet();
                    if (searchResponse.getHits().getHits().length == 0) {
                        String actualMd5 = MessageDigests.toHexString(md.digest());
                        if (Objects.equals(expectedMd5, actualMd5)) {
                            completedHandler.run();
                        } else {
                            failureHandler.accept(new RuntimeException("expected md5 hash [" + expectedMd5 +
                                "], but got md5 hash [" + actualMd5 + "]"));
                        }
                    } else {
                        for (SearchHit searchHit : searchResponse.getHits()) {
                            byte[] data = (byte[]) searchHit.getSourceAsMap().get("data");
                            md.update(data);
                            chunkConsumer.accept(data);
                            lastSortValue = (Long) searchHit.getSortValues()[0];
                        }
                    }
                } while (lastSortValue != null);
            } catch (Exception e) {
                failureHandler.accept(e);
            }
        });
    }

    static SearchRequest createSearchRequest(String databaseName, int firstChunk, int lastChunk, Long lastSortValue) {
        SearchRequest searchRequest = new SearchRequest(GeoIpDownloader.DATABASES_INDEX);
        BoolQueryBuilder boolQuery = new BoolQueryBuilder();
        boolQuery.filter(new TermQueryBuilder("name", databaseName));
        boolQuery.filter(new RangeQueryBuilder("chunk").from(firstChunk).to(lastChunk));
        searchRequest.source().query(boolQuery);
        searchRequest.source().sort("chunk");
        searchRequest.source().size(10);
        if (lastSortValue != null) {
            searchRequest.source().searchAfter(new Object[]{lastSortValue});
        }
        return searchRequest;
    }

    static void decompress(Path source, Path target) throws IOException {
        try (GZIPInputStream in = new GZIPInputStream(Files.newInputStream(source), 8192)) {
            Files.copy(in, target, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    static final class DatabaseReference implements Closeable {

        private final String md5;
        private final Path databaseFile;
        private final DatabaseReaderLazyLoader reader;

        DatabaseReference(String md5, Path databaseFile, DatabaseReaderLazyLoader reader) {
            this.md5 = md5;
            this.databaseFile = databaseFile;
            this.reader = reader;
        }

        @Override
        public void close() throws IOException {
            reader.close();
        }
    }

}
