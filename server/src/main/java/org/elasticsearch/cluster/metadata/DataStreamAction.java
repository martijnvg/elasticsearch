/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.time.Instant;
import java.util.Objects;
import java.util.function.Supplier;

import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * Operations on data streams. Currently supports adding and removing backing indices.
 */
public class DataStreamAction implements Writeable, ToXContentObject {

    private static final ParseField DATA_STREAM = new ParseField("data_stream");
    private static final ParseField INDEX = new ParseField("index");
    private static final ParseField TIME_SERIES_START = new ParseField("time_series_start");
    private static final ParseField TIME_SERIES_END = new ParseField("time_series_end");

    private static final ParseField ADD_BACKING_INDEX = new ParseField("add_backing_index");
    private static final ParseField REMOVE_BACKING_INDEX = new ParseField("remove_backing_index");
    private static final ParseField CREATE_TIME_SERIES_INDEX = new ParseField("create_time_series_index");

    public enum Type {
        ADD_BACKING_INDEX((byte) 0, DataStreamAction.ADD_BACKING_INDEX),
        REMOVE_BACKING_INDEX((byte) 1, DataStreamAction.REMOVE_BACKING_INDEX),
        CREATE_TIME_SERIES_INDEX((byte) 2, DataStreamAction.CREATE_TIME_SERIES_INDEX);

        private final byte value;
        private final String fieldName;

        Type(byte value, ParseField field) {
            this.value = value;
            this.fieldName = field.getPreferredName();
        }

        public byte value() {
            return value;
        }

        public static Type fromValue(byte value) {
            return switch (value) {
                case 0 -> ADD_BACKING_INDEX;
                case 1 -> REMOVE_BACKING_INDEX;
                case 2 -> CREATE_TIME_SERIES_INDEX;
                default -> throw new IllegalArgumentException("no data stream action type for [" + value + "]");
            };
        }
    }

    private final Type type;
    private String dataStream;
    private String index;
    private Instant timeSeriesStart;
    private Instant timeSeriesEnd;

    public static DataStreamAction addBackingIndex(String dataStream, String index) {
        return new DataStreamAction(Type.ADD_BACKING_INDEX, dataStream, index);
    }

    public static DataStreamAction removeBackingIndex(String dataStream, String index) {
        return new DataStreamAction(Type.REMOVE_BACKING_INDEX, dataStream, index);
    }

    public DataStreamAction(StreamInput in) throws IOException {
        this.type = Type.fromValue(in.readByte());
        this.dataStream = in.readString();
        this.index = in.readString();
        if (in.getTransportVersion().onOrAfter(TransportVersion.V_8_8_0)) {
            this.timeSeriesStart = in.readOptionalInstant();
            this.timeSeriesEnd = in.readOptionalInstant();
        }
    }

    private DataStreamAction(Type type, String dataStream, String index) {
        if (false == Strings.hasText(dataStream)) {
            throw new IllegalArgumentException("[data_stream] is required");
        }
        if (false == Strings.hasText(index)) {
            throw new IllegalArgumentException("[index] is required");
        }
        this.type = Objects.requireNonNull(type, "[type] must not be null");
        this.dataStream = dataStream;
        this.index = index;
    }

    DataStreamAction(Type type) {
        this.type = type;
    }

    public String getDataStream() {
        return dataStream;
    }

    public void setDataStream(String datastream) {
        this.dataStream = datastream;
    }

    public String getIndex() {
        if (timeSeriesStart != null && timeSeriesEnd != null) {
            DateFormatter DATE_FORMATTER = DateFormatter.forPattern("uuuu.MM.dd.hh.mm.ss.SSS");
            var start = DATE_FORMATTER.format(timeSeriesStart);
            return DataStream.BACKING_INDEX_PREFIX + getDataStream() + "-" + start;
        } else {
            return index;
        }
    }

    public void setIndex(String index) {
        this.index = index;
    }

    public Type getType() {
        return type;
    }

    public Instant getTimeSeriesStart() {
        return timeSeriesStart;
    }

    public void setTimeSeriesStart(Instant timeSeriesStart) {
        this.timeSeriesStart = timeSeriesStart;
    }

    public void setTimeSeriesStart(String timeSeriesStart) {
        setTimeSeriesStart(Instant.from(MetadataDataStreamsService.FORMATTER.parse(timeSeriesStart)));
    }

    public Instant getTimeSeriesEnd() {
        return timeSeriesEnd;
    }

    public void setTimeSeriesEnd(Instant timeSeriesEnd) {
        this.timeSeriesEnd = timeSeriesEnd;
    }

    public void setTimeSeriesEnd(String timeSeriesEnd) {
        setTimeSeriesEnd(Instant.from(MetadataDataStreamsService.FORMATTER.parse(timeSeriesEnd)));
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.startObject(type.fieldName);
        builder.field(DATA_STREAM.getPreferredName(), dataStream);
        builder.field(INDEX.getPreferredName(), index);
        if (timeSeriesStart != null) {
            builder.field(TIME_SERIES_START.getPreferredName(), MetadataDataStreamsService.FORMATTER.format(timeSeriesStart));
        }
        if (timeSeriesEnd != null) {
            builder.field(TIME_SERIES_END.getPreferredName(), MetadataDataStreamsService.FORMATTER.format(timeSeriesEnd));
        }
        builder.endObject();
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeByte(type.value());
        out.writeString(dataStream);
        out.writeString(index);
        if (out.getTransportVersion().onOrAfter(TransportVersion.V_8_8_0)) {
            out.writeOptionalInstant(timeSeriesStart);
            out.writeOptionalInstant(timeSeriesEnd);
        }
    }

    public static DataStreamAction fromXContent(XContentParser parser) throws IOException {
        return PARSER.apply(parser, null);
    }

    private static final ObjectParser<DataStreamAction, Void> ADD_BACKING_INDEX_PARSER = parser(
        ADD_BACKING_INDEX.getPreferredName(),
        () -> new DataStreamAction(Type.ADD_BACKING_INDEX)
    );
    private static final ObjectParser<DataStreamAction, Void> REMOVE_BACKING_INDEX_PARSER = parser(
        REMOVE_BACKING_INDEX.getPreferredName(),
        () -> new DataStreamAction(Type.REMOVE_BACKING_INDEX)
    );
    private static final ObjectParser<DataStreamAction, Void> CREATE_TIME_SERIES_INDEX_PARSER = parser(
        CREATE_TIME_SERIES_INDEX.getPreferredName(),
        () -> new DataStreamAction(Type.CREATE_TIME_SERIES_INDEX)
    );
    static {
        ADD_BACKING_INDEX_PARSER.declareField(
            DataStreamAction::setDataStream,
            XContentParser::text,
            DATA_STREAM,
            ObjectParser.ValueType.STRING
        );
        ADD_BACKING_INDEX_PARSER.declareField(DataStreamAction::setIndex, XContentParser::text, INDEX, ObjectParser.ValueType.STRING);
        REMOVE_BACKING_INDEX_PARSER.declareField(
            DataStreamAction::setDataStream,
            XContentParser::text,
            DATA_STREAM,
            ObjectParser.ValueType.STRING
        );
        REMOVE_BACKING_INDEX_PARSER.declareField(DataStreamAction::setIndex, XContentParser::text, INDEX, ObjectParser.ValueType.STRING);
        CREATE_TIME_SERIES_INDEX_PARSER.declareString(DataStreamAction::setDataStream, DATA_STREAM);
        CREATE_TIME_SERIES_INDEX_PARSER.declareString(DataStreamAction::setTimeSeriesStart, TIME_SERIES_START);
        CREATE_TIME_SERIES_INDEX_PARSER.declareString(DataStreamAction::setTimeSeriesEnd, TIME_SERIES_END);
    }

    private static ObjectParser<DataStreamAction, Void> parser(String name, Supplier<DataStreamAction> supplier) {
        ObjectParser<DataStreamAction, Void> parser = new ObjectParser<>(name, supplier);
        return parser;
    }

    public static final ConstructingObjectParser<DataStreamAction, Void> PARSER = new ConstructingObjectParser<>(
        "data_stream_action",
        a -> {
            // Take the first action and error if there is more than one action
            DataStreamAction action = null;
            for (Object o : a) {
                if (o != null) {
                    if (action == null) {
                        action = (DataStreamAction) o;
                    } else {
                        throw new IllegalArgumentException("too many data stream operations declared on operation entry");
                    }
                }
            }
            return action;
        }
    );
    static {
        PARSER.declareObject(optionalConstructorArg(), ADD_BACKING_INDEX_PARSER, ADD_BACKING_INDEX);
        PARSER.declareObject(optionalConstructorArg(), REMOVE_BACKING_INDEX_PARSER, REMOVE_BACKING_INDEX);
        PARSER.declareObject(optionalConstructorArg(), CREATE_TIME_SERIES_INDEX_PARSER, CREATE_TIME_SERIES_INDEX);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }
        DataStreamAction other = (DataStreamAction) obj;
        return Objects.equals(type, other.type)
            && Objects.equals(dataStream, other.dataStream)
            && Objects.equals(index, other.index)
            && Objects.equals(timeSeriesStart, other.timeSeriesStart)
            && Objects.equals(timeSeriesEnd, other.timeSeriesEnd);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, dataStream, index, timeSeriesStart, timeSeriesEnd);
    }

}
