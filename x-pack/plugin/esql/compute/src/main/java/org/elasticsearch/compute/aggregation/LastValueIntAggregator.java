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

@Aggregator({ @IntermediateState(name = "last_value", type = "INT"), @IntermediateState(name = "seen", type = "BOOLEAN") })
@GroupingAggregator
class LastValueIntAggregator {

    public static int init() {
        return 0;
    }

    public static int combine(int current, int v) {
        return current;
    }

    public static int combine(long currentTS, long ts, int currentValue, int value) {
        return currentTS > ts ? currentValue : value;
    }
}

