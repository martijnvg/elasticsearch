/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.aggregations.bucket.timeseries;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.TimestampBounds;
import org.elasticsearch.search.aggregations.AggregationExecutionContext;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.bucket.BucketsAggregator;
import org.elasticsearch.search.aggregations.bucket.histogram.TimestampBoundsAware;
import org.elasticsearch.search.aggregations.support.AggregationContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TimeSeriesAggregator2 extends BucketsAggregator {

    private final TimestampBounds timestampBounds;
    private final TimestampBoundsAware parent;
    private final Map<Long, List<InternalTimeSeries.InternalBucket>> results;
    private final boolean keyed;

    public TimeSeriesAggregator2(
        String name,
        AggregatorFactories factories,
        boolean keyed,
        AggregationContext context,
        Aggregator parent,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, factories, context, parent, CardinalityUpperBound.ONE, metadata);
        this.keyed = keyed;
        this.timestampBounds = context.getIndexSettings().getTimestampBounds();
        this.parent = (TimestampBoundsAware) parent;
        this.results = new HashMap<>();
    }

    @Override
    public InternalAggregation[] buildAggregations(long[] owningBucketOrds) throws IOException {
        // figure out running pipeline aggs here
        // context.pipelineTreeRoot().subTree(agg.getName())
        completeBucket();
        InternalAggregation[] result = new InternalAggregation[owningBucketOrds.length];
        for (int ordIdx = 0; ordIdx < owningBucketOrds.length; ordIdx++) {
            long owningOrdinal = owningBucketOrds[ordIdx];
            List<InternalTimeSeries.InternalBucket> buckets = results.get(owningOrdinal);
            if (buckets == null) {
                continue;
            }
            result[ordIdx] = new InternalTimeSeries(name, buckets, keyed, metadata());
        }
        return result;
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalTimeSeries(name, new ArrayList<>(), false, metadata());
    }

    BytesRef currentTsid;
    int currentTsidOrd = -1;
    long currentParentBucket;
    long docCount;

    @Override
    protected LeafBucketCollector getLeafCollector(AggregationExecutionContext aggCtx, LeafBucketCollector sub) throws IOException {
        return new LeafBucketCollectorBase(sub, null) {

            @Override
            public void collect(int doc, long bucket) throws IOException {
                // System.out.println("bucketId=" + bucket);
                // System.out.println("tsid=" + TimeSeriesIdFieldMapper.decodeTsid(aggCtx.getTsid()));

                if (currentTsidOrd == aggCtx.getTsidOrd() && currentParentBucket == bucket) {
                    docCount++;
                    sub.collect(doc, 0L);
                    return;
                }
                if (currentTsid != null) {
                    completeBucket();
                }
                if (currentTsidOrd != aggCtx.getTsidOrd()) {
                    currentTsidOrd = aggCtx.getTsidOrd();
                    currentTsid = BytesRef.deepCopyOf(aggCtx.getTsid());
                }
                if (currentParentBucket != bucket) {
                    currentParentBucket = bucket;
                }

                sub.clear();
                docCount = 1;
                sub.collect(doc, 0L);
            }
        };
    }

    private void completeBucket() throws IOException {
        InternalTimeSeries.InternalBucket bucket = new InternalTimeSeries.InternalBucket(
            currentTsid,
            docCount,
            buildSubAggsForBuckets(new long[] { 0L })[0],
            keyed
        );
        // System.out.println("complete bucket=" + currentParentBucket);
        List<InternalTimeSeries.InternalBucket> result = results.get(currentParentBucket);
        if (result == null) {
            result = new ArrayList<>();
            results.put(currentParentBucket, result);
        }
        result.add(bucket);
    }

}
