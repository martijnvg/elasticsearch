/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.aggregations.bucket.timeseries;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.BaseAggregationBuilder;
import org.elasticsearch.search.aggregations.PipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.xcontent.InstantiatingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ParserConstructor;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class TimeSeriesAggregationBuilder extends AbstractAggregationBuilder<TimeSeriesAggregationBuilder> {
    public static final String NAME = "time_series";
    public static final ParseField KEYED_FIELD = new ParseField("keyed");
    public static final ParseField PIPELINE_FIELD = new ParseField("pipeline");
    public static final InstantiatingObjectParser<TimeSeriesAggregationBuilder, String> PARSER;

    private boolean keyed;
    private PipelineAggregationBuilder pipeline;

    static {
        InstantiatingObjectParser.Builder<TimeSeriesAggregationBuilder, String> parser = InstantiatingObjectParser.builder(
            NAME,
            false,
            TimeSeriesAggregationBuilder.class
        );
        parser.declareBoolean(optionalConstructorArg(), KEYED_FIELD);
        parser.declareField(
            TimeSeriesAggregationBuilder::setPipeline,
            (p, c) -> {
                var token  = p.nextToken();
                String type =  p.currentName();
                token = p.nextToken();
                PipelineAggregationBuilder pipeline = (PipelineAggregationBuilder) p.namedObject(BaseAggregationBuilder.class, type, c);
                token = p.nextToken();
                return pipeline;
            },
            PIPELINE_FIELD,
            ObjectParser.ValueType.OBJECT
        );
        PARSER = parser.build();
    }

    public TimeSeriesAggregationBuilder(String name) {
        this(name, true);
    }

    @ParserConstructor
    public TimeSeriesAggregationBuilder(String name, Boolean keyed) {
        super(name);
        this.keyed = keyed != null ? keyed : true;
    }

    protected TimeSeriesAggregationBuilder(
        TimeSeriesAggregationBuilder clone,
        AggregatorFactories.Builder factoriesBuilder,
        Map<String, Object> metadata
    ) {
        super(clone, factoriesBuilder, metadata);
        this.keyed = clone.keyed;
    }

    public TimeSeriesAggregationBuilder(StreamInput in) throws IOException {
        super(in);
        keyed = in.readBoolean();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeBoolean(keyed);
        out.writeNamedWriteable(pipeline);
    }

    @Override
    protected AggregatorFactory doBuild(
        AggregationContext context,
        AggregatorFactory parent,
        AggregatorFactories.Builder subFactoriesBuilder
    ) throws IOException {
        return new TimeSeriesAggregationFactory(name, keyed, pipeline, context, parent, subFactoriesBuilder, metadata);
    }

    @Override
    protected XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(KEYED_FIELD.getPreferredName(), keyed);
        builder.field(PIPELINE_FIELD.getPreferredName(), pipeline);
        builder.endObject();
        return builder;
    }

    @Override
    protected AggregationBuilder shallowCopy(AggregatorFactories.Builder factoriesBuilder, Map<String, Object> metadata) {
        return new TimeSeriesAggregationBuilder(this, factoriesBuilder, metadata);
    }

    @Override
    public BucketCardinality bucketCardinality() {
        return BucketCardinality.MANY;
    }

    @Override
    public String getType() {
        return NAME;
    }

    @Override
    public boolean isInSortOrderExecutionRequired() {
        return true;
    }

    public boolean isKeyed() {
        return keyed;
    }

    public void setKeyed(boolean keyed) {
        this.keyed = keyed;
    }

    public PipelineAggregationBuilder getPipeline() {
        return pipeline;
    }

    public void setPipeline(PipelineAggregationBuilder pipeline) {
        this.pipeline = pipeline;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (super.equals(o) == false) return false;
        TimeSeriesAggregationBuilder that = (TimeSeriesAggregationBuilder) o;
        return keyed == that.keyed && Objects.equals(pipeline, that.pipeline);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), keyed, pipeline);
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return Version.V_8_1_0;
    }
}
