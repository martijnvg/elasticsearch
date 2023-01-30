/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.results;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class SlimResults extends NlpInferenceResults {

    public static final String NAME = "slim_result";

    public record WeightedToken(int token, float weight) implements Writeable, ToXContentObject {

        public static final String TOKEN = "token";
        public static final String WEIGHT = "weight";

        public WeightedToken(StreamInput in) throws IOException {
            this(in.readVInt(), in.readFloat());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(token);
            out.writeFloat(weight);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(TOKEN, token);
            builder.field(WEIGHT, weight);
            builder.endObject();
            return builder;
        }

        public Map<String, Object> asMap() {
            return Map.of(TOKEN, token, WEIGHT, weight);
        }

        @Override
        public String toString() {
            return Strings.toString(this);
        }
    }

    private final String resultsField;
    private final List<WeightedToken> weightedTokens;

    public SlimResults(String resultField, List<WeightedToken> weightedTokens, boolean isTruncated) {
        super(isTruncated);
        this.resultsField = resultField;
        this.weightedTokens = weightedTokens;
    }

    public SlimResults(StreamInput in) throws IOException {
        super(in);
        this.resultsField = in.readString();
        this.weightedTokens = in.readList(WeightedToken::new);
    }

    public List<WeightedToken> getWeightedTokens() {
        return weightedTokens;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public String getResultsField() {
        return resultsField;
    }

    @Override
    public Object predictedValue() {
        throw new UnsupportedOperationException("[" + NAME + "] does not support a single predicted value");
    }

    @Override
    void doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.startArray(resultsField);
        for (var weightedToken : weightedTokens) {
            weightedToken.toXContent(builder, params);
        }
        builder.endArray();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (super.equals(o) == false) return false;
        SlimResults that = (SlimResults) o;
        return Objects.equals(resultsField, that.resultsField) && Objects.equals(weightedTokens, that.weightedTokens);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), resultsField, weightedTokens);
    }

    @Override
    void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(resultsField);
        out.writeList(weightedTokens);
    }

    @Override
    void addMapFields(Map<String, Object> map) {
        map.put(resultsField, weightedTokens.stream().map(WeightedToken::asMap).collect(Collectors.toList()));
    }
}
