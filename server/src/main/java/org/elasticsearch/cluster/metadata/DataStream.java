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
import org.elasticsearch.cluster.AbstractDiffable;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.Index;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

public final class DataStream extends AbstractDiffable<DataStream> implements ToXContentObject {

    private final String name;
    private final TimestampField timeStampField;
    private final List<Index> indices;
    private final long generation;

    public DataStream(String name, TimestampField timeStampField, List<Index> indices, long generation) {
        this.name = name;
        this.timeStampField = timeStampField;
        this.indices = indices;
        this.generation = generation;
        assert indices.size() > 0;
        assert indices.get(indices.size() - 1).getName().equals(getBackingIndexName(name, generation));
    }

    public DataStream(String name, TimestampField timeStampField, List<Index> indices) {
        this(name, timeStampField, indices, indices.size());
    }

    public String getName() {
        return name;
    }

    public TimestampField getTimeStampField() {
        return timeStampField;
    }

    public List<Index> getIndices() {
        return indices;
    }

    public long getGeneration() {
        return generation;
    }

    /**
     * Performs a rollover on a {@code DataStream} instance and returns a new instance containing
     * the updated list of backing indices and incremented generation.
     *
     * @param newWriteIndex the new write backing index. Must conform to the naming convention for
     *                      backing indices on data streams. See {@link #getBackingIndexName}.
     * @return new {@code DataStream} instance with the rollover operation applied
     */
    public DataStream rollover(Index newWriteIndex) {
        assert newWriteIndex.getName().equals(getBackingIndexName(name, generation + 1));
        List<Index> backingIndices = new ArrayList<>(indices);
        backingIndices.add(newWriteIndex);
        return new DataStream(name, timeStampField, backingIndices, generation + 1);
    }

    /**
     * Removes the specified backing index and returns a new {@code DataStream} instance with
     * the remaining backing indices.
     *
     * @param index the backing index to remove
     * @return new {@code DataStream} instance with the remaining backing indices
     */
    public DataStream removeBackingIndex(Index index) {
        List<Index> backingIndices = new ArrayList<>(indices);
        backingIndices.remove(index);
        assert backingIndices.size() == indices.size() - 1;
        return new DataStream(name, timeStampField, backingIndices, generation);
    }

    /**
     * Generates the name of the index that conforms to the naming convention for backing indices
     * on data streams given the specified data stream name and generation.
     *
     * @param dataStreamName name of the data stream
     * @param generation generation of the data stream
     * @return backing index name
     */
    public static String getBackingIndexName(String dataStreamName, long generation) {
        return String.format(Locale.ROOT, "%s-%06d", dataStreamName, generation);
    }

    public DataStream(StreamInput in) throws IOException {
        this(in.readString(), new TimestampField(in), in.readList(Index::new), in.readVLong());
    }

    public static Diff<DataStream> readDiffFrom(StreamInput in) throws IOException {
        return readDiffFrom(DataStream::new, in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        timeStampField.writeTo(out);
        out.writeList(indices);
        out.writeVLong(generation);
    }

    public static final ParseField NAME_FIELD = new ParseField("name");
    public static final ParseField TIMESTAMP_FIELD_FIELD = new ParseField("timestamp_field");
    public static final ParseField INDICES_FIELD = new ParseField("indices");
    public static final ParseField GENERATION_FIELD = new ParseField("generation");

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<DataStream, Void> PARSER = new ConstructingObjectParser<>("data_stream",
        args -> new DataStream((String) args[0], (TimestampField) args[1], (List<Index>) args[2], (Long) args[3]));

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), NAME_FIELD);
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), TimestampField.PARSER, TIMESTAMP_FIELD_FIELD);
        PARSER.declareObjectArray(ConstructingObjectParser.constructorArg(), (p, c) -> Index.fromXContent(p), INDICES_FIELD);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), GENERATION_FIELD);
    }

    public static DataStream fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(NAME_FIELD.getPreferredName(), name);
        builder.field(TIMESTAMP_FIELD_FIELD.getPreferredName(), timeStampField);
        builder.field(INDICES_FIELD.getPreferredName(), indices);
        builder.field(GENERATION_FIELD.getPreferredName(), generation);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DataStream that = (DataStream) o;
        return name.equals(that.name) &&
            timeStampField.equals(that.timeStampField) &&
            indices.equals(that.indices) &&
            generation == that.generation;
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, timeStampField, indices, generation);
    }

    public static class TimestampField implements Writeable, ToXContentObject {

        static ParseField FIELD_NAME_FIELD = new ParseField("field_name");
        static ParseField FIELD_MAPPING_FIELD = new ParseField("field_mapping");

        private static final ConstructingObjectParser<TimestampField, Void> PARSER = new ConstructingObjectParser<>(
            "timestamp_field",
            args -> new TimestampField((String) args[0], (String) args[1])
        );

        static {
            PARSER.declareString(ConstructingObjectParser.constructorArg(), FIELD_NAME_FIELD);
            PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), (p, c) ->
                Strings.toString(XContentFactory.jsonBuilder().map(p.mapOrdered())), FIELD_MAPPING_FIELD);
        }

        private final String fieldName;
        private final String fieldMapping;

        public TimestampField(String fieldName, String fieldMapping) {
            this.fieldName = fieldName;
            this.fieldMapping = fieldMapping;
        }

        public TimestampField(StreamInput in) throws IOException {
            // TODO: remove bwc logic when backporting:
            if (in.getVersion().before(Version.V_8_0_0)) {
                this.fieldName = in.readString();
                this.fieldMapping = null;
            } else {
                this.fieldName = in.readString();
                this.fieldMapping = in.readString();
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(fieldName);
            out.writeString(fieldMapping);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(FIELD_NAME_FIELD.getPreferredName(), fieldName);
            Map<String, Object> uncompressedMapping =
                XContentHelper.convertToMap(new BytesArray(fieldMapping), true, XContentType.JSON).v2();
            builder.field(FIELD_MAPPING_FIELD.getPreferredName(), uncompressedMapping);
            builder.endObject();
            return builder;
        }

        public String getFieldName() {
            return fieldName;
        }

        public String getFieldMapping() {
            return fieldMapping;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TimestampField that = (TimestampField) o;
            return fieldName.equals(that.fieldName) &&
                fieldMapping.equals(that.fieldMapping);
        }

        @Override
        public int hashCode() {
            return Objects.hash(fieldName, fieldMapping);
        }
    }
}
