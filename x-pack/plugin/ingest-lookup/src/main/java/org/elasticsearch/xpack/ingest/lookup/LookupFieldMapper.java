/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ingest.lookup;

import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.query.QueryShardContext;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class LookupFieldMapper extends FieldMapper {

    public static final String CONTENT_TYPE = "lookup";

    public static final MappedFieldType FIELD_TYPE = new LookupFieldType();
    static {
        FIELD_TYPE.setOmitNorms(true);
        FIELD_TYPE.freeze();
    }

    public static class TypeParser implements Mapper.TypeParser {

        @Override
        public Mapper.Builder<?, ?> parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            return new Builder(name);
        }
    }

    public static class Builder extends FieldMapper.Builder<Builder, LookupFieldMapper> {

        Builder(String name) {
            super(name, FIELD_TYPE, FIELD_TYPE);
        }

        @Override
        public LookupFieldMapper build(BuilderContext context) {
            setupFieldType(context);
            return new LookupFieldMapper(name, fieldType, context.indexSettings());
        }
    }

    public static final class LookupFieldType extends MappedFieldType {

        private static final LookupPostingsFormat POSTINGS_FORMAT = new LookupPostingsFormat();

        LookupFieldType() {
        }

        LookupFieldType(LookupFieldType ref) {
            super(ref);
        }

        @Override
        public MappedFieldType clone() {
            return new LookupFieldType(this);
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public Query termQuery(Object value, QueryShardContext context) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Query existsQuery(QueryShardContext context) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean hasCustomPostingsFormat() {
            return true;
        }

        @Override
        public PostingsFormat customPostingsFormat() {
            return POSTINGS_FORMAT;
        }
    }

    LookupFieldMapper(String simpleName, MappedFieldType fieldType, Settings indexSettings) {
        super(simpleName, fieldType, fieldType, indexSettings, MultiFields.empty(), CopyTo.empty());
    }

    @Override
    protected void parseCreateField(ParseContext context, List<IndexableField> fields) throws IOException {
        XContentParser parser = context.parser();
        Map<String, ?> map = parser.map();
        BytesRef in = new BytesRef((String) map.get("key"));

        Object value = map.get("value");
        BytesRef out;
        if (value instanceof String) {
            out = new BytesRef((String) value);
        } else if (value instanceof Integer) {
            out = new BytesRef(String.valueOf((int) value));
        } else if (value instanceof Long) {
            out = new BytesRef(String.valueOf((long) value));
        } else if (value instanceof Float) {
            out = new BytesRef(String.valueOf((float) value));
        } else if (value instanceof Double) {
            out = new BytesRef(String.valueOf((double) value));
        } else {
            throw new RuntimeException();
        }

        fields.add(new LookupField(fieldType().name(), in, out));
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }
}
