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
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.env.Environment;
import org.elasticsearch.watcher.FileChangesListener;
import org.elasticsearch.watcher.FileWatcher;
import org.elasticsearch.watcher.ResourceWatcherService;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.Stream;

import static org.elasticsearch.ingest.geoip.IngestGeoIpPlugin.DEFAULT_DATABASE_FILENAMES;

/**
 * Keeps track of the databases locally available to a node:
 * 1) Default databases shipped with the default distribution via ingest-geoip module
 * 2) User provided databases from the ES_HOME/config/ingest-geoip directory. These directory is monitored
 *    and files updates are picked up and may cause databases being loaded or removed at runtime.
 */
final class LocalDatabases {

    private static final Logger LOGGER = LogManager.getLogger(LocalDatabases.class);

    private final GeoIpCache cache;
    private final Path geoipConfigDir;

    private final Map<String, DatabaseReaderLazyLoader> defaultDatabases;
    private volatile Map<String, DatabaseReaderLazyLoader> configDatabases;

    LocalDatabases(Environment environment, GeoIpCache cache) {
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
            cache);
    }

    LocalDatabases(Path geoipModuleDir, Path geoipConfigDir, GeoIpCache cache) {
        this.cache = cache;
        this.geoipConfigDir = geoipConfigDir;
        this.configDatabases = Map.of();
        this.defaultDatabases = initDefaultDatabases(geoipModuleDir);
    }

    void initialize(ResourceWatcherService resourceWatcher) throws IOException {
        configDatabases = initConfigDatabases(geoipConfigDir);

        FileWatcher watcher = new FileWatcher(geoipConfigDir);
        watcher.addListener(new GeoipDirectoryListener());
        resourceWatcher.add(watcher, ResourceWatcherService.Frequency.HIGH);

        LOGGER.info("initialized default databases [{}], config databases [{}] and watching [{}] for changes",
            defaultDatabases.keySet(), configDatabases.keySet(), geoipConfigDir);
    }

    DatabaseReaderLazyLoader getDatabase(String name) {
        DatabaseReaderLazyLoader database = configDatabases.get(name);
        if (database != null) {
            return database;
        } else {
            return defaultDatabases.get(name);
        }
    }

    Map<String, DatabaseReaderLazyLoader> getAllDatabases() {
        Map<String, DatabaseReaderLazyLoader> all = new HashMap<>(defaultDatabases);
        all.putAll(configDatabases);
        return all;
    }

    void updateDatabase(Path file, boolean update) {
        String databaseFileName = file.getFileName().toString();
        try {
            Map<String, DatabaseReaderLazyLoader> databases = new HashMap<>(this.configDatabases);
            if (update) {
                LOGGER.info("database file changed [{}], reload database...", file);
                DatabaseReaderLazyLoader loader = new DatabaseReaderLazyLoader(cache, file);
                DatabaseReaderLazyLoader existing = databases.put(databaseFileName, loader);
                if (existing != null) {
                    existing.close();
                }
            } else {
                LOGGER.info("database file removed [{}], close database...", file);
                DatabaseReaderLazyLoader existing = databases.remove(databaseFileName);
                assert existing != null;
                existing.close();
            }
            this.configDatabases = Map.copyOf(databases);
        } catch (Exception e) {
            LOGGER.error((Supplier<?>) () -> new ParameterizedMessage("failed to update database [{}]", databaseFileName), e);
        }
    }

    Map<String, DatabaseReaderLazyLoader> initDefaultDatabases(Path geoipModuleDir) {
        Map<String, DatabaseReaderLazyLoader> databases = new HashMap<>();

        for (String filename : DEFAULT_DATABASE_FILENAMES) {
            Path source = geoipModuleDir.resolve(filename);
            assert Files.exists(source);
            String databaseFileName = source.getFileName().toString();
            DatabaseReaderLazyLoader loader = new DatabaseReaderLazyLoader(cache, source);
            databases.put(databaseFileName, loader);
        }

        return Map.copyOf(databases);
    }

    Map<String, DatabaseReaderLazyLoader> initConfigDatabases(Path geoipConfigDir) throws IOException {
        Map<String, DatabaseReaderLazyLoader> databases = new HashMap<>();

        if (geoipConfigDir != null && Files.exists(geoipConfigDir)) {
            try (Stream<Path> databaseFiles = Files.list(geoipConfigDir)) {
                PathMatcher pathMatcher = geoipConfigDir.getFileSystem().getPathMatcher("glob:**.mmdb");
                // Use iterator instead of forEach otherwise IOException needs to be caught twice...
                Iterator<Path> iterator = databaseFiles.iterator();
                while (iterator.hasNext()) {
                    Path databasePath = iterator.next();
                    if (Files.isRegularFile(databasePath) && pathMatcher.matches(databasePath)) {
                        assert Files.exists(databasePath);
                        String databaseFileName = databasePath.getFileName().toString();
                        DatabaseReaderLazyLoader loader = new DatabaseReaderLazyLoader(cache, databasePath);
                        databases.put(databaseFileName, loader);
                    }
                }
            }
        }

        return Map.copyOf(databases);
    }

    @SuppressForbidden(reason = "PathUtils#get")
    private static Path getGeoipConfigDirectory(Environment environment) {
        return PathUtils.get(environment.settings().get("ingest.geoip.database_path"));
    }

    private class GeoipDirectoryListener implements FileChangesListener {

        @Override
        public void onFileCreated(Path file) {
            onFileChanged(file);
        }

        @Override
        public void onFileDeleted(Path file) {
            PathMatcher pathMatcher = file.getFileSystem().getPathMatcher("glob:**.mmdb");
            if (pathMatcher.matches(file)) {
                updateDatabase(file, false);
            }
        }

        @Override
        public void onFileChanged(Path file) {
            PathMatcher pathMatcher = file.getFileSystem().getPathMatcher("glob:**.mmdb");
            if (pathMatcher.matches(file)) {
                updateDatabase(file, true);
            }
        }
    }
}
