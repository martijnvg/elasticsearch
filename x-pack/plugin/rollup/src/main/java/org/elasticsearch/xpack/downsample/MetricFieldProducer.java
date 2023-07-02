/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.search.aggregations.metrics.CompensatedSum;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Class that collects all raw values for a metric field and computes its aggregate (downsampled)
 * values. Based on the supported metric types, the subclasses of this class compute values for
 * gauge and metric types.
 */
final class MetricFieldProducer extends AbstractDownsampleFieldProducer {

    static MetricFieldProducer gauge(String name, int numSegments) {
        return new MetricFieldProducer(name, new Metric[] { new Min(), new Max(), new Sum(), new ValueCount() }, numSegments);
    }

    static MetricFieldProducer gauge(String name, Metric[] metrics, int numSegments) {
        return new MetricFieldProducer(name, metrics, numSegments);
    }

    static MetricFieldProducer counter(String name, int numSegments) {
        return new MetricFieldProducer(name, new LastValue(), numSegments);
    }

    private final boolean counter;
    /**
     * a list of metrics that will be computed for the field
     */
    private final Metric[] metrics;
    private final SortedNumericDoubleValues[] doubleValues;

    private MetricFieldProducer(String name, Metric[] metrics, int numSegments) {
        super(name);
        this.counter = false;
        this.metrics = metrics;
        this.doubleValues = new SortedNumericDoubleValues[numSegments];
    }

    private MetricFieldProducer(String name, LastValue lastValue, int numSegments) {
        super(name);
        this.counter = true;
        this.metrics = new Metric[] { lastValue };
        this.doubleValues = new SortedNumericDoubleValues[numSegments];
    }

    /**
     * Reset all values collected for the field
     */
    public void reset() {
        for (Metric metric : metrics) {
            metric.reset();
        }
        isEmpty = true;
    }

    /** return the list of metrics that are computed for the field */
    public Metric[] metrics() {
        return metrics;
    }

    /** Collect the value of a raw field and compute all downsampled metrics */
    void collect(double value) {
        for (MetricFieldProducer.Metric metric : metrics()) {
            metric.collect(value);
        }
        isEmpty = false;
    }

    public void collect(int leafOrd, int docId) throws IOException {
        // Counter producers only collect the last_value. Since documents are
        // collected by descending timestamp order, the producer should only
        // process the first value for every tsid. So, it will only collect the
        // field if no value has been set before.
        if (counter && isEmpty == false) {
            return;
        }

        SortedNumericDoubleValues docValues = doubleValues[leafOrd];
        if (docValues.advanceExact(docId) == false) {
            return;
        }
        int docValuesCount = docValues.docValueCount();
        for (int i = 0; i < docValuesCount; i++) {
            double num = docValues.nextValue();
            collect(num);
        }
    }

    public void setLeafReaderContext(LeafReaderContext context, FieldValueFetcher fieldValueFetcher) {
        doubleValues[context.ord] = fieldValueFetcher.getSortedNumericDoubleValues(context);
    }

    @Override
    public void write(XContentBuilder builder) throws IOException {
        if (isEmpty() == false) {
            if (counter) {
                builder.field(name(), ((LastValue) metrics[0]).lastValue);
            } else {
                builder.startObject(name());
                for (MetricFieldProducer.Metric metric : metrics()) {
                    metric.toXContent(builder);
                }
                builder.endObject();
            }
        }
    }

    /**
     * Abstract class that defines how a metric is computed.
     */
    sealed interface Metric permits Max, Min, Sum, LastValue, ValueCount {

        void collect(double number);

        double get();

        void reset();

        void toXContent(XContentBuilder builder) throws IOException;
    }

    /**
     * Metric implementation that computes the maximum of all values of a field
     */
    static final class Max implements Metric {
        boolean isSet = false;
        private double max = Double.NEGATIVE_INFINITY;

        Max() {}

        @Override
        public void collect(double value) {
            this.isSet = true;
            this.max = Math.max(value, max);
        }

        @Override
        public double get() {
            return max;
        }

        @Override
        public void reset() {
            this.max = Double.NEGATIVE_INFINITY;
            this.isSet = false;
        }

        public void toXContent(XContentBuilder builder) throws IOException {
            if (isSet) {
                builder.field("max", max);
            }
        }
    }

    /**
     * Metric implementation that computes the minimum of all values of a field
     */
    static final class Min implements Metric {
        boolean isSet = false;
        private double min = Double.POSITIVE_INFINITY;

        Min() {}

        @Override
        public void collect(double value) {
            this.isSet = true;
            this.min = Math.min(value, min);
        }

        @Override
        public double get() {
            return min;
        }

        @Override
        public void reset() {
            this.min = Double.POSITIVE_INFINITY;
            this.isSet = false;
        }

        public void toXContent(XContentBuilder builder) throws IOException {
            if (isSet) {
                builder.field("min", min);
            }
        }
    }

    /**
     * Metric implementation that computes the sum of all values of a field
     */
    static final class Sum implements Metric {

        boolean isSet = false;
        private final String name;
        private final CompensatedSum kahanSummation = new CompensatedSum();

        Sum() {
            name = "sum";
        }

        Sum(String name) {
            this.name = name;
        }

        @Override
        public void collect(double value) {
            this.isSet = true;
            kahanSummation.add(value);
        }

        @Override
        public double get() {
            return kahanSummation.value();
        }

        @Override
        public void reset() {
            kahanSummation.reset(0, 0);
            this.isSet = false;
        }

        public void toXContent(XContentBuilder builder) throws IOException {
            if (isSet) {
                builder.field(name, kahanSummation.value());
            }
        }
    }

    /**
     * Metric implementation that counts all values collected for a metric field
     */
    static final class ValueCount implements Metric {
        private long count;
        boolean isSet = false;

        ValueCount() {}

        @Override
        public void collect(double value) {
            this.isSet = true;
            count++;
        }

        @Override
        public double get() {
            return count;
        }

        @Override
        public void reset() {
            count = 0;
            this.isSet = false;
        }

        @Override
        public void toXContent(XContentBuilder builder) throws IOException {
            if (isSet) {
                builder.field("value_count", count);
            }
        }
    }

    /**
     * Metric implementation that stores the last value over time for a metric. This implementation
     * assumes that field values are collected sorted by descending order by time. In this case,
     * it assumes that the last value of the time is the first value collected. Eventually,
     * the implementation of this class end up storing the first value it is empty and then
     * ignoring everything else.
     */
    static final class LastValue implements Metric {
        boolean isSet = false;
        private double lastValue;

        LastValue() {}

        @Override
        public void collect(double value) {
            if (isSet == false) {
                lastValue = value;
            }
            this.isSet = true;
        }

        @Override
        public double get() {
            return lastValue;
        }

        @Override
        public void reset() {
            this.lastValue = 0;
            this.isSet = false;
        }

        @Override
        public void toXContent(XContentBuilder builder) throws IOException {
            if (isSet) {
                builder.field("last_value", lastValue);
            }
        }
    }

}
