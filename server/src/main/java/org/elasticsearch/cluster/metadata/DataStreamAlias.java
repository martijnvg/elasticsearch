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
package org.elasticsearch.cluster.metadata;

import org.elasticsearch.cluster.AbstractDiffable;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class DataStreamAlias extends AbstractDiffable<DataStreamAlias> implements ToXContentObject {

    public static final ParseField DATA_STREAMS_FIELD = new ParseField("data_streams");
    public static final ParseField WRITE_DATA_STREAM_FIELD = new ParseField("write_data_stream");

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<DataStreamAlias, String> PARSER = new ConstructingObjectParser<>(
        "data_stream_alias",
        false,
        (args, name) -> new DataStreamAlias(name, (List<String>) args[0], (String) args[1])
    );

    static {
        PARSER.declareStringArray(ConstructingObjectParser.constructorArg(), DATA_STREAMS_FIELD);
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), WRITE_DATA_STREAM_FIELD);
    }

    private final String name;
    private final List<String> dataStreams;
    private final String writeDataStream;

    public DataStreamAlias(String name, List<String> dataStreams, String writeDataStream) {
        this.name = name;
        this.dataStreams = dataStreams;
        this.writeDataStream = writeDataStream;
    }

    public DataStreamAlias(StreamInput in) throws IOException {
        this.name = in.readString();
        this.dataStreams = in.readStringList();
        this.writeDataStream = in.readOptionalString();
    }

    public String getName() {
        return name;
    }

    public List<String> getDataStreams() {
        return dataStreams;
    }

    public String getWriteDataStream() {
        return writeDataStream;
    }

    public static Diff<DataStreamAlias> readDiffFrom(StreamInput in) throws IOException {
        return readDiffFrom(DataStreamAlias::new, in);
    }

    public static DataStreamAlias fromXContent(XContentParser parser) throws IOException {
        XContentParser.Token token = parser.currentToken();
        if (token != XContentParser.Token.FIELD_NAME) {
            throw new ParsingException(parser.getTokenLocation(), "unexpected token");
        }
        String name = parser.currentName();
        DataStreamAlias alias = PARSER.parse(parser, name);
        return alias;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(name);
        builder.field(DATA_STREAMS_FIELD.getPreferredName(), dataStreams);
        if (writeDataStream != null) {
            builder.field(WRITE_DATA_STREAM_FIELD.getPreferredName(), writeDataStream);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeStringCollection(dataStreams);
        out.writeOptionalString(writeDataStream);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DataStreamAlias that = (DataStreamAlias) o;
        return Objects.equals(name, that.name) &&
            Objects.equals(dataStreams, that.dataStreams) &&
            Objects.equals(writeDataStream, that.writeDataStream);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, dataStreams, writeDataStream);
    }
}
