/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.compute.ann.Aggregator;
import org.elasticsearch.compute.ann.GroupingAggregator;
import org.elasticsearch.compute.ann.IntermediateState;

@Aggregator({ @IntermediateState(name = "last_value", type = "DOUBLE"), @IntermediateState(name = "seen", type = "BOOLEAN") })
@GroupingAggregator
class LastValueDoubleAggregator {

    public static double init() {
        return 0;
    }

    public static double combine(double current, double v) {
        return current;
    }

    public static double combine(long currentTS, long ts, double currentValue, double value) {
        return currentTS > ts ? currentValue : value;
    }
}

