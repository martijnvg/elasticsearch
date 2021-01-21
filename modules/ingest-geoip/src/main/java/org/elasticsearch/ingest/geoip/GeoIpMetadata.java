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

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.ingest.geoip;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.EnumSet;

public class GeoIpMetadata implements Metadata.Custom {

    public static final String TYPE = "geoip";

    private final long timestamp;

    public GeoIpMetadata(long timestamp) {
        this.timestamp = timestamp;
    }

    public GeoIpMetadata(StreamInput in) throws IOException {
        this.timestamp = in.readLong();
    }

    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("timestamp", timestamp);
        return builder;
    }

    @Override
    public String getWriteableName() {
        return TYPE;
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return Version.V_8_0_0;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(timestamp);
    }

    @Override
    public Diff<Metadata.Custom> diff(Metadata.Custom previousState) {
        return new GeoIpMetadataMetadataDiff(timestamp);
    }

    @Override
    public EnumSet<Metadata.XContentContext> context() {
        return Metadata.ALL_CONTEXTS;
    }

    public static class GeoIpMetadataMetadataDiff implements NamedDiff<Metadata.Custom> {

        private final long timestamp;

        public GeoIpMetadataMetadataDiff(long timestamp) {
            this.timestamp = timestamp;
        }

        public GeoIpMetadataMetadataDiff(StreamInput in) throws IOException {
            this.timestamp = in.readLong();
        }

        @Override
        public Metadata.Custom apply(Metadata.Custom part) {
            return new GeoIpMetadata(timestamp);
        }

        @Override
        public String getWriteableName() {
            return TYPE;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeLong(timestamp);
        }
    }
}
