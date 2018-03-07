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
package org.elasticsearch.ingest.geolite2;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.plugins.IngestPlugin;
import org.elasticsearch.plugins.Plugin;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Map;

public class IngestGeoLite2Plugin extends Plugin implements IngestPlugin, Closeable {

    private Directory geoLite2Directory;
    private IndexReader geoLite2IndexReader;

    @Override
    public Map<String, Processor.Factory> getProcessors(Processor.Parameters parameters) {
        assert geoLite2Directory == null && geoLite2IndexReader == null;
        try {
            Path geoLite2ConfigDirectory = parameters.env.configFile().resolve("ingest-geolite2");
            geoLite2Directory = FSDirectory.open(geoLite2ConfigDirectory);
            geoLite2IndexReader = DirectoryReader.open(geoLite2Directory);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        IndexSearcher indexSearcher = new IndexSearcher(geoLite2IndexReader);
        return Collections.singletonMap(IngestGeoLite2Processor.TYPE, new IngestGeoLite2Processor.Factory(indexSearcher));
    }

    public void close() throws IOException {
        IOUtils.close(geoLite2Directory, geoLite2IndexReader);
    }
}
