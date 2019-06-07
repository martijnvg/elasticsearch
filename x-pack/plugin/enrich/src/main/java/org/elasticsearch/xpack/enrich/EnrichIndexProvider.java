/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.enrich;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.env.Environment;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public final class EnrichIndexProvider extends AbstractLifecycleComponent {

    private static final String LOCAL_STORAGE_DIRECTORY_NAME = "enrich-local-data";

    private final Path enrichStorageDirectory;
    private final ConcurrentMap<String, LocalEnrichIndex> allocatedStorage = new ConcurrentHashMap<>();

    EnrichIndexProvider(Environment environment) {
        Path dataDir = environment.dataFiles()[0];
        enrichStorageDirectory = dataDir.resolve(LOCAL_STORAGE_DIRECTORY_NAME);
    }

    @Override
    protected void doStart() {
        try {
            Files.createDirectories(enrichStorageDirectory);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    protected void doStop() {
    }

    @Override
    protected void doClose() throws IOException {
        IOUtils.rm(enrichStorageDirectory);
    }

    public LocalEnrichIndex createEnrichIndexPath(String policyName) throws IOException {
        return allocatedStorage.computeIfAbsent(policyName, key -> {
            try {
                Path path = enrichStorageDirectory.resolve(policyName);
                Files.createDirectories(path);
                return new LocalEnrichIndex(path);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
    }

    public LocalEnrichIndex getLocalEnrichIndex(String policyName) {
        return allocatedStorage.get(policyName);
    }

    final static class LocalEnrichIndex implements Closeable {

        private final Path basePath;

        private volatile Path currentPath;
        private volatile Directory currentDirectory;
        private volatile DirectoryReader currentReader;

        private volatile Path newPath;

        LocalEnrichIndex(Path basePath) {
            this.basePath = basePath;
        }

        void swap() throws IOException {
            Path oldPath = currentPath;
            Directory oldDirectory = currentDirectory;
            DirectoryReader oldReader = currentReader;

            currentPath = newPath;
            newPath = null;
            currentDirectory = FSDirectory.open(currentPath);
            currentReader = DirectoryReader.open(currentDirectory);


            IOUtils.close(oldReader, oldDirectory);
            IOUtils.rm(oldPath);
        }

        boolean newIndex() {
            return newPath != null;
        }

        Path prepareNewIndex(String enrichIndex) throws IOException {
            newPath = basePath.resolve(enrichIndex);
            Files.createDirectories(newPath);
            return newPath;
        }

        void clearNewIndex() throws IOException {
            IOUtils.rm(newPath);
            newPath = null;
        }

        public DirectoryReader getCurrentReader() {
            return currentReader;
        }

        @Override
        public void close() throws IOException {
            IOUtils.close(currentReader, currentDirectory);
        }
    }
}
