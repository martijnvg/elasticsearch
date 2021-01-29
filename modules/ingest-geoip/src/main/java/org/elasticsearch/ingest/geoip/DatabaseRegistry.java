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

import com.maxmind.db.NoCache;
import com.maxmind.db.Reader;
import com.maxmind.geoip2.DatabaseReader;
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
import org.elasticsearch.common.Booleans;
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.common.CheckedRunnable;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.ingest.IngestService;
import org.elasticsearch.ingest.PipelineConfiguration;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.search.SearchHit;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;

import static org.elasticsearch.ingest.geoip.IngestGeoIpPlugin.DEFAULT_DATABASE_FILENAMES;

public class DatabaseRegistry implements Closeable {

    private static final Logger LOGGER = LogManager.getLogger(DatabaseRegistry.class);

    private static final boolean LOAD_DATABASE_ON_HEAP =
        Booleans.parseBoolean(System.getProperty("es.geoip.load_db_on_heap", "false"));

    private final Client client;
    private final Path geoipTmpDirectory;
    private final IngestService ingestService;
    private final Consumer<Runnable> genericExecutor;

    private final Map<String, DatabaseReaderLazyLoader> builtinDatabases;
    private volatile Map<String, DatabaseReference> databases = Map.of();

    public DatabaseRegistry(Environment environment, Client client, IngestService ingestService, Consumer<Runnable> genericExecutor) throws IOException {
        this(
            // In GeoIpProcessorNonIngestNodeTests, ingest-geoip is loaded on the classpath.
            // This means that the plugin is never unbundled into a directory where the database files would live.
            // Therefore, we have to copy these database files ourselves. To do this, we need the ability to specify where
            // those database files would go. We do this by adding a plugin that registers ingest.geoip.database_path as an
            // actual setting. Otherwise, in production code, this setting is not registered and the database path is not configurable.
            environment.settings().get("ingest.geoip.database_path") != null ?
                getGeoipConfigDirectory(environment) :
                environment.modulesFile().resolve("ingest-geoip"),
            environment.configFile().resolve("ingest-geoip"),
            environment.tmpFile(),
            client,
            ingestService,
            genericExecutor);
    }

    public DatabaseRegistry(Path geoipModuleDir,
                            Path geoipConfigDir,
                            Path tmpDir,
                            Client client,
                            IngestService ingestService,
                            Consumer<Runnable> genericExecutor) throws IOException {
        this.client = new OriginSettingClient(client, "geoip");
        this.geoipTmpDirectory = tmpDir.resolve("geoip-databases");
        this.ingestService = ingestService;
        this.builtinDatabases = initBuiltinDatabases(geoipModuleDir, geoipConfigDir);
        this.genericExecutor = genericExecutor;
    }

    public void initialize() throws IOException {
        if (Files.exists(geoipTmpDirectory) == false) {
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
            return builtinDatabases.get(name);
        } else {
            return null;
        }
    }

    Map<String, DatabaseReaderLazyLoader> getAllDatabases() {
        Map<String, DatabaseReaderLazyLoader> databases = new HashMap<>(builtinDatabases);
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

        GeoIpDownloaderTaskState taskState = persistentTasks.findTasks(GeoIpDownloader.GEOIP_DOWNLOADER, task -> true).stream()
            .filter(persistentTask -> persistentTask.getState() != null)
            .map(persistentTask -> (GeoIpDownloaderTaskState) persistentTask.getState())
            .findAny()
            .orElse(null);

        if (taskState == null) {
            return;
        }

        // hack:
        Date lastUpdated = new Date(taskState.getLastExecutionTime());
        Map<String, Date> lastUpdatedByDatabase = Map.of(
            "GeoLite2-ASN.mmdb", lastUpdated,
            "GeoLite2-City.mmdb", lastUpdated,
            "GeoLite2-Country.mmdb", lastUpdated
        );

        for (var entry : lastUpdatedByDatabase.entrySet()) {
            DatabaseReference reference = databases.get(entry.getKey());
            Date lastDownloaded = reference != null ? reference.lastDownloaded : null;
            if (lastDownloaded != null && lastDownloaded.compareTo(entry.getValue()) <= 0) {
                continue;
            }

            try {
                retrieveAndUpdateDatabase(entry.getKey(), state);
            } catch (IOException e) {
                // log
            }
        }

        List<String> staleEntries = new ArrayList<>();
        for (var entry : databases.entrySet()) {
            if (lastUpdatedByDatabase.get(entry.getKey()) == null) {
                staleEntries.add(entry.getKey());
            }
        }
        removeStaleEntries(staleEntries);
    }

    void retrieveAndUpdateDatabase(String databaseName, ClusterState state) throws IOException {
        final Path databaseTmpFile;
        try {
            databaseTmpFile = Files.createFile(geoipTmpDirectory.resolve(databaseName + ".tmp"));
        } catch (FileAlreadyExistsException e) {
            LOGGER.debug("database update [{}] already in progress", databaseName);
            return;
        }
        LOGGER.info("downloading geoip database [{}] to [{}]", databaseName, databaseTmpFile);
        retrieveDatabase(
            databaseName,
            bytes -> Files.write(databaseTmpFile, bytes, StandardOpenOption.APPEND),
            () -> {
                Path databaseFile = geoipTmpDirectory.resolve(databaseName);
                LOGGER.info("moving database from [{}] to [{}]", databaseTmpFile, databaseFile);
                Files.move(databaseTmpFile, databaseFile, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
                updateDatabase(databaseName, databaseFile, state);
            },
            failure -> {
                LOGGER.error((Supplier<?>) () -> new ParameterizedMessage("failed to download database [{}]", databaseName), failure);
                try {
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
            DatabaseReaderLazyLoader loader = createLoader(file, LOAD_DATABASE_ON_HEAP);
            DatabaseReference existing = databases.put(databaseFileName, new DatabaseReference(new Date(), file, loader));
            if (existing != null) {
                existing.close();
            }
            this.databases = Map.copyOf(databases);
            reloadIngestPipelines(databaseFileName, state);
        } catch (Exception e) {
            LOGGER.error((Supplier<?>) () -> new ParameterizedMessage("failed to update database [{}]", databaseFileName), e);
        }
    }

    void removeStaleEntries(List<String> staleEntries) {
        try {
            Map<String, DatabaseReference> databases = new HashMap<>(this.databases);

            for (String staleEntry : staleEntries) {
                LOGGER.info("database file removed [{}], close database...", staleEntries);
                DatabaseReference existing = databases.remove(staleEntry);
                assert existing != null;
                existing.close();
                Files.delete(existing.databaseFile);
            }

            this.databases = Map.copyOf(databases);
        } catch (IOException e) {
            LOGGER.error((Supplier<?>) () -> new ParameterizedMessage("failed to update database [{}]", staleEntries), e);
        }
    }

    void retrieveDatabase(String databaseName,
                          CheckedConsumer<byte[], IOException> chunkConsumer,
                          CheckedRunnable<Exception> completedHandler,
                          Consumer<Exception> failureHandler,
                          Integer seq) {
        SearchRequest searchRequest = new SearchRequest(GeoIpDownloader.DATABASES_INDEX);
        searchRequest.source().query(new TermQueryBuilder("name", databaseName));
//        searchRequest.source().sort("sequence");
        searchRequest.source().size(1);
        if (seq != null) {
            searchRequest.source().searchAfter(new Object[]{seq});
        }
        // Need to run the search from a different thread, since this can be executed from cluster state applier thread:
        // (forking has minimal overhead, because we immediately execute a search)
        genericExecutor.accept(() -> {
            client.search(searchRequest, ActionListener.wrap(
                searchResponse -> {
                    assert searchResponse.getHits().getHits().length == 1;
                    SearchHit searchHit = searchResponse.getHits().getHits()[0];
                    byte[] data = (byte[]) searchHit.getSourceAsMap().get("data");
                    data = sillyHackyDecompress(data);

                    chunkConsumer.accept(data);
                    completedHandler.run();
//                if (searchResponse.getHits().getHits().length == 0) {
//                    completedHandler.run();
//                } else {
//                    assert searchResponse.getHits().getHits().length == 1;
//                    SearchHit searchHit = searchResponse.getHits().getHits()[0];
//                    byte[] data = (byte[]) searchHit.getSourceAsMap().get("data");
//                    chunkConsumer.accept(data);
//                    retrieveDatabase(databaseName, chunkConsumer, completedHandler, failureHandler, (Integer) searchHit.getSortValues()[0]);
//                }
                },
                failureHandler)
            );
        });
    }

    // we should do this at least in a streaming manner:
    private byte[] sillyHackyDecompress(byte[] compressed) throws IOException {

        GZIPInputStream gzipper = new GZIPInputStream(new ByteArrayInputStream(compressed));

        byte[] buffer = new byte[1024];
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        int len;
        while ((len = gzipper.read(buffer)) > 0) {
            out.write(buffer, 0, len);
        }

        gzipper.close();
        out.close();
        return out.toByteArray();
    }

    void reloadIngestPipelines(String databaseName, ClusterState state) throws Exception {
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
        LOGGER.info("reloading pipelines [{}], because data base [{}] changed", pipelinesToReload, databaseName);
        ingestService.reloadPipelines(pipelinesToReload);
    }

    static class DatabaseReference implements Closeable {

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

    static Map<String, DatabaseReaderLazyLoader> initBuiltinDatabases(Path geoipModuleDir, Path geoipConfigDir) throws IOException {
        assertDatabaseExistence(geoipModuleDir, true);
        assertDatabaseExistence(geoipConfigDir, false);

        Map<String, DatabaseReaderLazyLoader> databases = new HashMap<>();
        List<Path> sourceFiles = new ArrayList<>();

        for (String filename : DEFAULT_DATABASE_FILENAMES) {
            Path source = geoipModuleDir.resolve(filename);
            sourceFiles.add(source);
        }

        if (geoipConfigDir != null && Files.exists(geoipConfigDir)) {
            try (Stream<Path> databaseFiles = Files.list(geoipConfigDir)) {
                PathMatcher pathMatcher = geoipConfigDir.getFileSystem().getPathMatcher("glob:**.mmdb");
                // Use iterator instead of forEach otherwise IOException needs to be caught twice...
                Iterator<Path> iterator = databaseFiles.iterator();
                while (iterator.hasNext()) {
                    Path databasePath = iterator.next();
                    if (Files.isRegularFile(databasePath) && pathMatcher.matches(databasePath)) {
                        sourceFiles.add(databasePath);
                    }
                }
            }
        }

        for (Path source : sourceFiles) {
            assert Files.exists(source);
            String databaseFileName = source.getFileName().toString();
            DatabaseReaderLazyLoader loader = createLoader(source, LOAD_DATABASE_ON_HEAP);
            databases.put(databaseFileName, loader);
        }

        return Map.copyOf(databases);
    }

    @SuppressForbidden(reason = "PathUtils#get")
    private static Path getGeoipConfigDirectory(Environment environment) {
        return PathUtils.get(environment.settings().get("ingest.geoip.database_path"));
    }

    private static DatabaseReaderLazyLoader createLoader(Path databasePath, boolean loadDatabaseOnHeap) {
        return new DatabaseReaderLazyLoader(
            databasePath,
            () -> {
                DatabaseReader.Builder builder = createDatabaseBuilder(databasePath).withCache(NoCache.getInstance());
                if (loadDatabaseOnHeap) {
                    builder.fileMode(Reader.FileMode.MEMORY);
                } else {
                    builder.fileMode(Reader.FileMode.MEMORY_MAPPED);
                }
                return builder.build();
            });
    }

    @SuppressForbidden(reason = "Maxmind API requires java.io.File")
    private static DatabaseReader.Builder createDatabaseBuilder(Path databasePath) {
        return new DatabaseReader.Builder(databasePath.toFile());
    }

    private static void assertDatabaseExistence(final Path path, final boolean exists) throws IOException {
        for (final String database : DEFAULT_DATABASE_FILENAMES) {
            if (Files.exists(path.resolve(database)) != exists) {
                final String message = "expected database [" + database + "] to " + (exists ? "" : "not ") + "exist in [" + path + "]";
                throw new IOException(message);
            }
        }
    }

}
