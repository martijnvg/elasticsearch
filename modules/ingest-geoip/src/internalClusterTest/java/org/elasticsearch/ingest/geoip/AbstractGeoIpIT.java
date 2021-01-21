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

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.StreamsUtils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.test.NodeRoles.nonIngestNode;

public abstract class AbstractGeoIpIT extends ESIntegTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(IngestGeoIpPlugin.class, IngestGeoIpSettingsPlugin.class);
    }

    @Override
    protected Settings nodeSettings(final int nodeOrdinal) {
        final Path databasePath = createTempDir();
        try {
            Files.createDirectories(databasePath);
            Files.copy(
                new ByteArrayInputStream(StreamsUtils.copyToBytesFromClasspath("/GeoLite2-City.mmdb")),
                databasePath.resolve("GeoLite2-City.mmdb"));
            Files.copy(
                new ByteArrayInputStream(StreamsUtils.copyToBytesFromClasspath("/GeoLite2-Country.mmdb")),
                databasePath.resolve("GeoLite2-Country.mmdb"));
            Files.copy(
                new ByteArrayInputStream(StreamsUtils.copyToBytesFromClasspath("/GeoLite2-ASN.mmdb")),
                databasePath.resolve("GeoLite2-ASN.mmdb"));
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
        return Settings.builder()
            .put("ingest.geoip.database_path", databasePath)
            .put(super.nodeSettings(nodeOrdinal))
            .build();
    }

    public static class IngestGeoIpSettingsPlugin extends Plugin {

        @Override
        public List<Setting<?>> getSettings() {
            return Collections.singletonList(Setting.simpleString("ingest.geoip.database_path", Setting.Property.NodeScope));
        }
    }
}
