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

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.DiffableUtils;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Custom {@link Metadata} implementation for storing a map of {@link DataStream}s and their names.
 */
public class DataStreamMetadata implements Metadata.Custom {

    public static final String TYPE = "data_stream";
    private static final ParseField DATA_STREAM = new ParseField("data_stream");
    private static final ParseField DATA_STREAM_ALIASES = new ParseField("data_stream_aliases");
    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<DataStreamMetadata, Void> PARSER = new ConstructingObjectParser<>(TYPE, false,
        a -> new DataStreamMetadata((Map<String, DataStream>) a[0], (Map<String, DataStreamAlias>) a[1]));

    static {
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> {
            Map<String, DataStream> dataStreams = new HashMap<>();
            while (p.nextToken() != XContentParser.Token.END_OBJECT) {
                String name = p.currentName();
                dataStreams.put(name, DataStream.fromXContent(p));
            }
            return dataStreams;
        }, DATA_STREAM);
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> {
            Map<String, DataStreamAlias> dataStreams = new HashMap<>();
            while (p.nextToken() != XContentParser.Token.END_OBJECT) {
                DataStreamAlias alias = DataStreamAlias.fromXContent(p);
                dataStreams.put(alias.getName(), alias);
            }
            return dataStreams;
        }, DATA_STREAM_ALIASES);
    }

    public static final Version DATA_STREAM_ALIAS_VERSION = Version.V_8_0_0;

    private final Map<String, DataStream> dataStreams;
    private final Map<String, DataStreamAlias> dataStreamAliases;

    public DataStreamMetadata(Map<String, DataStream> dataStreams,
                              Map<String, DataStreamAlias> dataStreamAliases) {
        this.dataStreams = dataStreams;
        this.dataStreamAliases = dataStreamAliases;
    }

    public DataStreamMetadata(StreamInput in) throws IOException {
        this.dataStreams = in.readMap(StreamInput::readString, DataStream::new);
        if (in.getVersion().onOrAfter(DATA_STREAM_ALIAS_VERSION)) {
            this.dataStreamAliases = in.readMap(StreamInput::readString, DataStreamAlias::new);
        } else {
            this.dataStreamAliases = null;
        }
    }

    public Map<String, DataStream> dataStreams() {
        return this.dataStreams;
    }

    public Map<String, DataStreamAlias> getDataStreamAliases() {
        return dataStreamAliases;
    }

    @Override
    public Diff<Metadata.Custom> diff(Metadata.Custom before) {
        return new DataStreamMetadata.DataStreamMetadataDiff((DataStreamMetadata) before, this);
    }

    public static NamedDiff<Metadata.Custom> readDiffFrom(StreamInput in) throws IOException {
        return new DataStreamMetadata.DataStreamMetadataDiff(in);
    }

    @Override
    public EnumSet<Metadata.XContentContext> context() {
        return Metadata.ALL_CONTEXTS;
    }

    @Override
    public String getWriteableName() {
        return TYPE;
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return Version.V_7_7_0;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(this.dataStreams, StreamOutput::writeString, (stream, val) -> val.writeTo(stream));
        if (out.getVersion().onOrAfter(DATA_STREAM_ALIAS_VERSION)) {
            out.writeMap(this.dataStreamAliases, StreamOutput::writeString, (stream, val) -> val.writeTo(stream));
        }
    }

    public static DataStreamMetadata fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(DATA_STREAM.getPreferredName());
        for (Map.Entry<String, DataStream> dataStream : dataStreams.entrySet()) {
            builder.field(dataStream.getKey(), dataStream.getValue());
        }
        builder.endObject();
        builder.startObject(DATA_STREAM_ALIASES.getPreferredName());
        for (Map.Entry<String, DataStreamAlias> dataStream : dataStreamAliases.entrySet()) {
            dataStream.getValue().toXContent(builder, params);
        }
        builder.endObject();
        return builder;
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.dataStreams, dataStreamAliases);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj.getClass() != getClass()) {
            return false;
        }
        DataStreamMetadata other = (DataStreamMetadata) obj;
        return Objects.equals(this.dataStreams, other.dataStreams) &&
            Objects.equals(this.dataStreamAliases, other.dataStreamAliases);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    public static class Builder {

        private final Map<String, DataStream> dataStreams;
        private final Map<String, DataStreamAlias> dataStreamAliases;

        public Builder(Map<String, DataStream> dataStreams, Map<String, DataStreamAlias> dataStreamAliases) {
            this.dataStreams = dataStreams;
            this.dataStreamAliases = dataStreamAliases;
        }

        public Builder() {
            this.dataStreams = new HashMap<>();
            this.dataStreamAliases = new HashMap<>();
        }

        public Builder putDataStream(DataStream dataStream) {
            dataStreams.put(dataStream.getName(), dataStream);
            return this;
        }

        public Builder putDataStreamAlias(String name, List<String> dataStreams, String writeDataStream) {
            dataStreamAliases.put(name, new DataStreamAlias(name, dataStreams, writeDataStream));
            return this;
        }

        public DataStreamMetadata build() {
            return new DataStreamMetadata(dataStreams, dataStreamAliases);
        }
    }

    static class DataStreamMetadataDiff implements NamedDiff<Metadata.Custom> {

        final Diff<Map<String, DataStream>> dataStreamDiff;
        final Diff<Map<String, DataStreamAlias>> dataStreamAliasDiff;

        DataStreamMetadataDiff(DataStreamMetadata before, DataStreamMetadata after) {
            this.dataStreamDiff = DiffableUtils.diff(before.dataStreams, after.dataStreams,
                DiffableUtils.getStringKeySerializer());
            this.dataStreamAliasDiff = DiffableUtils.diff(before.dataStreamAliases, after.dataStreamAliases,
                DiffableUtils.getStringKeySerializer());
        }

        DataStreamMetadataDiff(StreamInput in) throws IOException {
            this.dataStreamDiff = DiffableUtils.readJdkMapDiff(in, DiffableUtils.getStringKeySerializer(),
                DataStream::new, DataStream::readDiffFrom);
            if (in.getVersion().onOrAfter(DATA_STREAM_ALIAS_VERSION)) {
                this.dataStreamAliasDiff = DiffableUtils.readJdkMapDiff(in, DiffableUtils.getStringKeySerializer(),
                    DataStreamAlias::new, DataStreamAlias::readDiffFrom);
            } else {
                this.dataStreamAliasDiff = null;
            }
        }

        @Override
        public Metadata.Custom apply(Metadata.Custom part) {
            return new DataStreamMetadata(dataStreamDiff.apply(((DataStreamMetadata) part).dataStreams),
                dataStreamAliasDiff.apply(((DataStreamMetadata) part).dataStreamAliases));
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            dataStreamDiff.writeTo(out);
            if (out.getVersion().onOrAfter(DATA_STREAM_ALIAS_VERSION)) {
                dataStreamAliasDiff.writeTo(out);
            }
        }

        @Override
        public String getWriteableName() {
            return TYPE;
        }
    }
}
