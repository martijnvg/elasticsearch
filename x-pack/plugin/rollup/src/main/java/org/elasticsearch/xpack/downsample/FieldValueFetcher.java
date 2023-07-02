/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.index.fielddata.FormattedDocValues;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.xpack.aggregatemetric.mapper.AggregateDoubleMetricFieldMapper;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Utility class used for fetching field values by reading field data.
 * For fields whose type is multi-valued the 'name' matches the parent field
 * name (normally used for indexing data), while the actual sub-field
 * name is accessible by means of {@link MappedFieldType#name()}.
 */
class FieldValueFetcher {

    protected final String name;
    protected final MappedFieldType fieldType;
    protected final IndexFieldData<?> fieldData;
    protected final AbstractDownsampleFieldProducer rollupFieldProducer;

    protected FieldValueFetcher(String name, MappedFieldType fieldType, IndexFieldData<?> fieldData, int numSegments) {
        this.name = name;
        this.fieldType = fieldType;
        this.fieldData = fieldData;
        this.rollupFieldProducer = createRollupFieldProducer(numSegments);
    }

    public String name() {
        return name;
    }

    public FormattedDocValues getFormattedDocValues(LeafReaderContext context) {
        DocValueFormat format = fieldType.docValueFormat(null, null);
        return fieldData.load(context).getFormattedValues(format);
    }

    public SortedNumericDoubleValues getSortedNumericDoubleValues(LeafReaderContext context) {
        if (fieldData instanceof IndexNumericFieldData numericFieldData) {
            return numericFieldData.load(context).getDoubleValues();
        } else {
            assert false;
            return null;
        }
    }

    public AbstractDownsampleFieldProducer rollupFieldProducer() {
        return rollupFieldProducer;
    }

    private AbstractDownsampleFieldProducer createRollupFieldProducer(int numSegments) {
        if (fieldType.getMetricType() != null) {
            return switch (fieldType.getMetricType()) {
                case GAUGE -> MetricFieldProducer.gauge(name(), numSegments);
                case COUNTER -> MetricFieldProducer.counter(name(), numSegments);
                // TODO: Support POSITION in downsampling
                case POSITION -> throw new IllegalArgumentException("Unsupported metric type [position] for down-sampling");
            };
        } else {
            // If field is not a metric, we downsample it as a label
            return new LabelFieldProducer(name(), numSegments);
        }
    }

    /**
     * Retrieve field value fetchers for a list of fields.
     */
    static List<FieldValueFetcher> create(SearchExecutionContext context, String[] fields) {
        int numSegments = context.searcher().getLeafContexts().size();
        List<FieldValueFetcher> fetchers = new ArrayList<>();
        for (String field : fields) {
            MappedFieldType fieldType = context.getFieldType(field);
            assert fieldType != null : "Unknown field type for field: [" + field + "]";

            if (fieldType instanceof AggregateDoubleMetricFieldMapper.AggregateDoubleMetricFieldType aggMetricFieldType) {
                // If the field is an aggregate_metric_double field, we should load all its subfields
                // This is a rollup-of-rollup case
                for (NumberFieldMapper.NumberFieldType metricSubField : aggMetricFieldType.getMetricFields().values()) {
                    if (context.fieldExistsInIndex(metricSubField.name())) {
                        IndexFieldData<?> fieldData = context.getForField(metricSubField, MappedFieldType.FielddataOperation.SEARCH);
                        fetchers.add(new AggregateMetricFieldValueFetcher(metricSubField, aggMetricFieldType, fieldData, numSegments));
                    }
                }
            } else {
                if (context.fieldExistsInIndex(field)) {
                    final IndexFieldData<?> fieldData = context.getForField(fieldType, MappedFieldType.FielddataOperation.SEARCH);
                    final String fieldName = context.isMultiField(field)
                        ? fieldType.name().substring(0, fieldType.name().lastIndexOf('.'))
                        : fieldType.name();
                    fetchers.add(new FieldValueFetcher(fieldName, fieldType, fieldData, numSegments));
                }
            }
        }
        return Collections.unmodifiableList(fetchers);
    }
}
