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
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.MapperService;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.cluster.metadata.DataStream.TimestampField.FIXED_TIMESTAMP_FIELD;

/**
 * An index template is comprised of a set of index patterns, an optional template, and a list of
 * ids corresponding to component templates that should be composed in order when creating a new
 * index.
 */
public class ComposableIndexTemplate extends AbstractDiffable<ComposableIndexTemplate> implements ToXContentObject {
    private static final ParseField INDEX_PATTERNS = new ParseField("index_patterns");
    private static final ParseField TEMPLATE = new ParseField("template");
    private static final ParseField PRIORITY = new ParseField("priority");
    private static final ParseField COMPOSED_OF = new ParseField("composed_of");
    private static final ParseField VERSION = new ParseField("version");
    private static final ParseField METADATA = new ParseField("_meta");
    private static final ParseField DATA_STREAM = new ParseField("data_stream");
    private static final ParseField ALLOW_AUTO_CREATE = new ParseField("allow_auto_create");

    private static final Version ALLOW_AUTO_CREATE_VERSION = Version.V_7_11_0;

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<ComposableIndexTemplate, Void> PARSER = new ConstructingObjectParser<>("index_template",
        false,
        a -> new ComposableIndexTemplate((List<String>) a[0],
            (Template) a[1],
            (List<String>) a[2],
            (Long) a[3],
            (Long) a[4],
            (Map<String, Object>) a[5],
            (DataStreamTemplate) a[6],
            (Boolean) a[7]));

    static {
        PARSER.declareStringArray(ConstructingObjectParser.constructorArg(), INDEX_PATTERNS);
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), Template.PARSER, TEMPLATE);
        PARSER.declareStringArray(ConstructingObjectParser.optionalConstructorArg(), COMPOSED_OF);
        PARSER.declareLong(ConstructingObjectParser.optionalConstructorArg(), PRIORITY);
        PARSER.declareLong(ConstructingObjectParser.optionalConstructorArg(), VERSION);
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> p.map(), METADATA);
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), DataStreamTemplate.PARSER, DATA_STREAM);
        PARSER.declareBoolean(ConstructingObjectParser.optionalConstructorArg(), ALLOW_AUTO_CREATE);
    }

    private final List<String> indexPatterns;
    @Nullable
    private final Template template;
    @Nullable
    private final List<String> componentTemplates;
    @Nullable
    private final Long priority;
    @Nullable
    private final Long version;
    @Nullable
    private final Map<String, Object> metadata;
    @Nullable
    private final DataStreamTemplate dataStreamTemplate;
    @Nullable
    private final Boolean allowAutoCreate;

    static Diff<ComposableIndexTemplate> readITV2DiffFrom(StreamInput in) throws IOException {
        return AbstractDiffable.readDiffFrom(ComposableIndexTemplate::new, in);
    }

    public static ComposableIndexTemplate parse(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    public ComposableIndexTemplate(List<String> indexPatterns, @Nullable Template template, @Nullable List<String> componentTemplates,
                                   @Nullable Long priority, @Nullable Long version, @Nullable Map<String, Object> metadata) {
        this(indexPatterns, template, componentTemplates, priority, version, metadata, null, null);
    }

    public ComposableIndexTemplate(List<String> indexPatterns, @Nullable Template template, @Nullable List<String> componentTemplates,
        @Nullable Long priority, @Nullable Long version, @Nullable Map<String, Object> metadata,
        @Nullable DataStreamTemplate dataStreamTemplate) {
        this(indexPatterns, template, componentTemplates, priority, version, metadata, dataStreamTemplate, null);
    }

    public ComposableIndexTemplate(List<String> indexPatterns, @Nullable Template template, @Nullable List<String> componentTemplates,
                                   @Nullable Long priority, @Nullable Long version, @Nullable Map<String, Object> metadata,
                                   @Nullable DataStreamTemplate dataStreamTemplate, @Nullable Boolean allowAutoCreate) {
        this.indexPatterns = indexPatterns;
        this.template = template;
        this.componentTemplates = componentTemplates;
        this.priority = priority;
        this.version = version;
        this.metadata = metadata;
        this.dataStreamTemplate = dataStreamTemplate;
        this.allowAutoCreate = allowAutoCreate;
    }

    public ComposableIndexTemplate(StreamInput in) throws IOException {
        this.indexPatterns = in.readStringList();
        if (in.readBoolean()) {
            this.template = new Template(in);
        } else {
            this.template = null;
        }
        this.componentTemplates = in.readOptionalStringList();
        this.priority = in.readOptionalVLong();
        this.version = in.readOptionalVLong();
        this.metadata = in.readMap();
        if (in.getVersion().onOrAfter(Version.V_7_9_0)) {
            this.dataStreamTemplate = in.readOptionalWriteable(DataStreamTemplate::new);
        } else {
            this.dataStreamTemplate = null;
        }
        if (in.getVersion().onOrAfter(ALLOW_AUTO_CREATE_VERSION)) {
            this.allowAutoCreate = in.readOptionalBoolean();
        } else {
            this.allowAutoCreate = null;
        }
    }

    public List<String> indexPatterns() {
        return indexPatterns;
    }

    @Nullable
    public Template template() {
        return template;
    }

    public List<String> composedOf() {
        if (componentTemplates == null) {
            return List.of();
        }
        return componentTemplates;
    }

    @Nullable
    public Long priority() {
        return priority;
    }

    public long priorityOrZero() {
        if (priority == null) {
            return 0L;
        }
        return priority;
    }

    @Nullable
    public Long version() {
        return version;
    }

    @Nullable
    public Map<String, Object> metadata() {
        return metadata;
    }

    @Nullable
    public DataStreamTemplate getDataStreamTemplate() {
        return dataStreamTemplate;
    }

    @Nullable
    public Boolean getAllowAutoCreate() {
        return this.allowAutoCreate;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeStringCollection(this.indexPatterns);
        if (this.template == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            this.template.writeTo(out);
        }
        out.writeOptionalStringCollection(this.componentTemplates);
        out.writeOptionalVLong(this.priority);
        out.writeOptionalVLong(this.version);
        out.writeMap(this.metadata);
        if (out.getVersion().onOrAfter(Version.V_7_9_0)) {
            out.writeOptionalWriteable(dataStreamTemplate);
        }
        if (out.getVersion().onOrAfter(ALLOW_AUTO_CREATE_VERSION)) {
            out.writeOptionalBoolean(allowAutoCreate);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(INDEX_PATTERNS.getPreferredName(), this.indexPatterns);
        if (this.template != null) {
            builder.field(TEMPLATE.getPreferredName(), this.template);
        }
        if (this.componentTemplates != null) {
            builder.field(COMPOSED_OF.getPreferredName(), this.componentTemplates);
        }
        if (this.priority != null) {
            builder.field(PRIORITY.getPreferredName(), priority);
        }
        if (this.version != null) {
            builder.field(VERSION.getPreferredName(), version);
        }
        if (this.metadata != null) {
            builder.field(METADATA.getPreferredName(), metadata);
        }
        if (this.dataStreamTemplate != null) {
            builder.field(DATA_STREAM.getPreferredName(), dataStreamTemplate);
        }
        if (this.allowAutoCreate != null) {
            builder.field(ALLOW_AUTO_CREATE.getPreferredName(), allowAutoCreate);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.indexPatterns, this.template, this.componentTemplates, this.priority, this.version,
            this.metadata, this.dataStreamTemplate, this.allowAutoCreate);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        ComposableIndexTemplate other = (ComposableIndexTemplate) obj;
        return Objects.equals(this.indexPatterns, other.indexPatterns) &&
            Objects.equals(this.template, other.template) &&
            Objects.equals(this.componentTemplates, other.componentTemplates) &&
            Objects.equals(this.priority, other.priority) &&
            Objects.equals(this.version, other.version) &&
            Objects.equals(this.metadata, other.metadata) &&
            Objects.equals(this.dataStreamTemplate, other.dataStreamTemplate) &&
            Objects.equals(this.allowAutoCreate, other.allowAutoCreate);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    public static class DataStreamTemplate implements Writeable, ToXContentObject {

        private static final ParseField HIDDEN = new ParseField("hidden");
        private static final ParseField ALIASES = new ParseField("aliases");

        public static final ConstructingObjectParser<DataStreamTemplate, Void> PARSER = new ConstructingObjectParser<>(
            "data_stream_template",
            false,
            args -> {
                boolean hidden = args[0] != null && (boolean) args[0];
                @SuppressWarnings("unchecked")
                Map<String, DataStreamAliasTemplate> aliases = (Map<String, DataStreamAliasTemplate>) args[1];
                return new DataStreamTemplate(hidden, aliases);
            });

        static {
            PARSER.declareBoolean(ConstructingObjectParser.optionalConstructorArg(), HIDDEN);
            PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> {
                Map<String, DataStreamAliasTemplate> aliasMap = new HashMap<>();
                while ((p.nextToken()) != XContentParser.Token.END_OBJECT) {
                    XContentParser.Token token = p.currentToken();
                    if (token != XContentParser.Token.FIELD_NAME) {
                        throw new ParsingException(p.getTokenLocation(), "unexpected token");
                    }
                    String name = p.currentName();
                    DataStreamAliasTemplate alias = DataStreamAliasTemplate.PARSER.parse(p, name);
                    aliasMap.put(alias.getAlias(), alias);
                }
                return aliasMap;
            }, ALIASES);
        }

        private final boolean hidden;

        @Nullable
        private final Map<String, DataStreamAliasTemplate> aliases;

        public DataStreamTemplate() {
            this(false, null);
        }

        public DataStreamTemplate(boolean hidden, Map<String, DataStreamAliasTemplate> aliases) {
            this.hidden = hidden;
            this.aliases = aliases;
        }

        DataStreamTemplate(StreamInput in) throws IOException {
            hidden = in.getVersion().onOrAfter(DataStream.NEW_FEATURES_VERSION) && in.readBoolean();
            aliases = in.getVersion().onOrAfter(DataStreamMetadata.DATA_STREAM_ALIAS_VERSION) ?
                in.readBoolean() ? in.readMap(StreamInput::readString, DataStreamAliasTemplate::new) : null : null;
        }

        public String getTimestampField() {
            return FIXED_TIMESTAMP_FIELD;
        }

        /**
         * @return a mapping snippet for a backing index with `_data_stream_timestamp` meta field mapper properly configured.
         */
        public Map<String, Object> getDataStreamMappingSnippet() {
            // _data_stream_timestamp meta fields default to @timestamp:
            return Map.of(MapperService.SINGLE_MAPPING_NAME, Map.of("_data_stream_timestamp", Map.of("enabled", true)));
        }

        public boolean isHidden() {
            return hidden;
        }

        public Map<String, DataStreamAliasTemplate> getAliases() {
            return aliases;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            if (out.getVersion().onOrAfter(DataStream.NEW_FEATURES_VERSION)) {
                out.writeBoolean(hidden);
            }
            if (out.getVersion().onOrAfter(DataStreamMetadata.DATA_STREAM_ALIAS_VERSION)) {
                if (aliases != null) {
                    out.writeBoolean(true);
                    out.writeMap(aliases, StreamOutput::writeString, (innerOut, alias) -> alias.writeTo(innerOut));
                } else {
                    out.writeBoolean(false);
                }
            }
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("hidden", hidden);
            if (aliases != null) {
                builder.startObject("aliases");
                for (var alias : aliases.values()) {
                    alias.toXContent(builder, params);
                }
                builder.endObject();
            }
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            DataStreamTemplate that = (DataStreamTemplate) o;
            return hidden == that.hidden &&
                Objects.equals(aliases, that.aliases);
        }

        @Override
        public int hashCode() {
            return Objects.hash(hidden, aliases);
        }
    }

    public static class DataStreamAliasTemplate implements Writeable, ToXContentObject {

        private static final ParseField WRITE_ALIAS = new ParseField("write_alias");

        public static final ConstructingObjectParser<DataStreamAliasTemplate, String> PARSER = new ConstructingObjectParser<>(
            "data_stream_alias_template",
            false,
            (args, name)-> new DataStreamAliasTemplate(name, (Boolean) args[0]));

        static {
            PARSER.declareBoolean(ConstructingObjectParser.optionalConstructorArg(), WRITE_ALIAS);
        }

        private final String alias;
        private final Boolean writeAlias;

        public DataStreamAliasTemplate(String alias, @Nullable Boolean writeAlias) {
            this.alias = alias;
            this.writeAlias = writeAlias;
        }

        public DataStreamAliasTemplate(StreamInput in) throws IOException {
            this(in.readString(), in.readOptionalBoolean());
        }

        public String getAlias() {
            return alias;
        }

        @Nullable
        public Boolean getWriteAlias() {
            return writeAlias;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject(alias);
            if (writeAlias != null) {
                builder.field(WRITE_ALIAS.getPreferredName(), writeAlias);
            }
            builder.endObject();
            return builder;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(alias);
            out.writeOptionalBoolean(writeAlias);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            DataStreamAliasTemplate that = (DataStreamAliasTemplate) o;
            return alias.equals(that.alias) && Objects.equals(writeAlias, that.writeAlias);
        }

        @Override
        public int hashCode() {
            return Objects.hash(alias, writeAlias);
        }
    }

    public static class Builder{
        private List<String> indexPatterns;
        private Template template;
        private List<String> componentTemplates;
        private Long priority;
        private Long version;
        private Map<String, Object> metadata;
        private DataStreamTemplate dataStreamTemplate;
        private Boolean allowAutoCreate;

        public Builder() {
        }

        public Builder indexPatterns(List<String> indexPatterns) {
            this.indexPatterns = indexPatterns;
            return this;
        }

        public Builder template(Template template) {
            this.template = template;
            return this;
        }

        public Builder componentTemplates(List<String> componentTemplates) {
            this.componentTemplates = componentTemplates;
            return this;
        }

        public Builder priority(Long priority) {
            this.priority = priority;
            return this;
        }

        public Builder version(Long version) {
            this.version = version;
            return this;
        }

        public Builder metadata(Map<String, Object> metadata) {
            this.metadata = metadata;
            return this;
        }

        public Builder dataStreamTemplate(DataStreamTemplate dataStreamTemplate) {
            this.dataStreamTemplate = dataStreamTemplate;
            return this;
        }

        public Builder allowAutoCreate(Boolean allowAutoCreate) {
            this.allowAutoCreate = allowAutoCreate;
            return this;
        }

        public ComposableIndexTemplate build() {
            return new ComposableIndexTemplate(this.indexPatterns,this.template,this.componentTemplates,
                    this.priority,this.version,this.metadata,this.dataStreamTemplate,this.allowAutoCreate);
        }
    }
}
