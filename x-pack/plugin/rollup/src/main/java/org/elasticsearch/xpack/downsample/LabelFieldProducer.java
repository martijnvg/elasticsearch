/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.index.fielddata.FormattedDocValues;
import org.elasticsearch.index.fielddata.HistogramValue;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.aggregatemetric.mapper.AggregateDoubleMetricFieldMapper.Metric;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Class that produces values for a last label field.
 */
final class LabelFieldProducer extends AbstractDownsampleFieldProducer {

    final Label label;
    final FormattedDocValues[] formattedDocValues;

    private LabelFieldProducer(String name, Label label, int numSegments) {
        super(name);
        this.label = label;
        this.formattedDocValues = new FormattedDocValues[numSegments];
    }

    LabelFieldProducer(String name, int numSegments) {
        this(name, new Label(), numSegments);
    }

    // for aggregate metric field
    LabelFieldProducer(String name, Metric metric, int numSegments) {
        this(name, new Label(metric.name()), numSegments);
    }

    Label label() {
        return label;
    }

    @Override
    public void write(XContentBuilder builder) throws IOException {
        if (isEmpty() == false) {
            Object labelValue = label.get();
            if (labelValue instanceof HistogramValue histogramValue) {
                final List<Double> values = new ArrayList<>();
                final List<Integer> counts = new ArrayList<>();
                while (histogramValue.next()) {
                    values.add(histogramValue.value());
                    counts.add(histogramValue.count());
                }
                builder.startObject(name()).field("counts", counts).field("values", values).endObject();
            } else {
                builder.field(name(), labelValue);
            }
        }
    }

    @Override
    public void setLeafReaderContext(LeafReaderContext context, FieldValueFetcher fieldValueFetcher) {
        formattedDocValues[context.ord] = fieldValueFetcher.getFormattedDocValues(context);
    }

    @Override
    public void collect(int leafOrdinal, int docId) throws IOException {
        if (isEmpty() == false) {
            return;
        }
        FormattedDocValues docValues = formattedDocValues[leafOrdinal];
        if (docValues.advanceExact(docId) == false) {
            return;
        }

        int docValuesCount = docValues.docValueCount();
        assert docValuesCount > 0;
        isEmpty = false;
        if (docValuesCount == 1) {
            label.collect(docValues.nextValue());
        } else {
            Object[] values = new Object[docValuesCount];
            for (int i = 0; i < docValuesCount; i++) {
                values[i] = docValues.nextValue();
            }
            label.collect(values);
        }
    }

    @Override
    public void reset() {
        label.reset();
        isEmpty = true;
    }

    /**
     * Label that stores the last value over time for a label. This class
     * assumes that field values are collected sorted by descending order by time. In this case,
     * it assumes that the last value of the time is the first value collected. Eventually,
     * this class of this class end up storing the first value it is empty and then
     * ignoring everything else.
     */
    static final class Label {

        private final String name;
        private Object lastValue;

        /**
         * Abstract class that defines how a label is downsampled.
         * @param name the name of the field as it will be stored in the downsampled document
         */
        Label(String name) {
            this.name = name;
        }

        Label() {
            this("last_value");
        }

        public String name() {
            return name;
        }

        Object get() {
            return lastValue;
        }

        void reset() {
            lastValue = null;
        }

        void collect(Object value) {
            if (lastValue == null) {
                lastValue = value;
            }
        }
    }

}
