/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.timeseries;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.TimestampBounds;
import org.elasticsearch.search.aggregations.AggregationExecutionContext;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.bucket.BucketsAggregator;
import org.elasticsearch.search.aggregations.bucket.terms.BytesKeyedBucketOrds;
import org.elasticsearch.search.aggregations.support.AggregationContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

public class TimeSeriesAggregator extends BucketsAggregator {

    private final BytesKeyedBucketOrds bucketOrds;
    private final boolean keyed;
    private final int size;
    private final BucketOrder order;

    @SuppressWarnings("unchecked")
    public TimeSeriesAggregator(
        String name,
        AggregatorFactories factories,
        boolean keyed,
        int size,
        BucketOrder order,
        AggregationContext context,
        Aggregator parent,
        CardinalityUpperBound bucketCardinality,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, factories, context, parent, CardinalityUpperBound.MANY, metadata);
        this.keyed = keyed;
        this.size = size;
        this.order = order;
        this.bucketOrds = BytesKeyedBucketOrds.build(bigArrays(), bucketCardinality);
        TimestampBounds timestampBounds = context.getIndexSettings().getTimestampBounds();
    }

    @Override
    public InternalAggregation[] buildAggregations(long[] owningBucketOrds) throws IOException {
        InternalTimeSeries.InternalBucket[][] allBucketsPerOrd = new InternalTimeSeries.InternalBucket[owningBucketOrds.length][];

        Comparator<InternalTimeSeries.InternalBucket> comparator = order == null
            ? Comparator.comparingLong(o -> o.bucketOrd) // ordering not important if no order was provided, just consistency.
            : order.partiallyBuiltBucketComparator(b -> b.bucketOrd, this);
        for (int ordIdx = 0; ordIdx < owningBucketOrds.length; ordIdx++) {
            BytesKeyedBucketOrds.BucketOrdsEnum ordsEnum = bucketOrds.ordsEnum(owningBucketOrds[ordIdx]);
            // TODO: use PriorityQueue here if we know we execute on a single index.
            // (perhaps we check the whether there is a main range query and compare that to context.getIndexSettings().getTimestampBounds()
            // (or somewhere record whether the coordinator send shard level requests for multiple indices)
            SortedSet<InternalTimeSeries.InternalBucket> ordered = new TreeSet<>(comparator);
            while (ordsEnum.next()) {
                // TODO: make doc_count optional for the time_series agg. Maybe add a dedicated doc_count metric agg for this?
                long docCount = bucketDocCount(ordsEnum.ord());
                BytesRef key = new BytesRef();
                ordsEnum.readValue(key);
                InternalTimeSeries.InternalBucket bucket = new InternalTimeSeries.InternalBucket(
                    key,
                    docCount,
                    null,
                    keyed
                );
                bucket.bucketOrd = ordsEnum.ord();
                ordered.add(bucket);
            }
            allBucketsPerOrd[ordIdx] = ordered.toArray(new InternalTimeSeries.InternalBucket[0]);
        }
        buildSubAggsForAllBuckets(allBucketsPerOrd, b -> b.bucketOrd, (b, a) -> b.aggregations = a);

        InternalAggregation[] result = new InternalAggregation[owningBucketOrds.length];
        for (int ordIdx = 0; ordIdx < owningBucketOrds.length; ordIdx++) {
            result[ordIdx] = buildResult(allBucketsPerOrd[ordIdx]);
        }
        return result;
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalTimeSeries(name, new ArrayList<>(), false, size, order, metadata());
    }

    @Override
    protected void doClose() {
        Releasables.close(bucketOrds);
    }

    @Override
    protected LeafBucketCollector getLeafCollector(AggregationExecutionContext aggCtx, LeafBucketCollector sub) throws IOException {
        return new LeafBucketCollectorBase(sub, null) {

            @Override
            public void collect(int doc, long bucket) throws IOException {
                // TODO: If tsid changes here and have seen more then SIZE tsids then we stop collecting
                // With sorting enabled we need to keep collecting, but we only need to keep SIZE buckets around

                long bucketOrdinal = bucketOrds.add(bucket, aggCtx.getTsid());
                if (bucketOrdinal < 0) { // already seen
                    bucketOrdinal = -1 - bucketOrdinal;
                    collectExistingBucket(sub, doc, bucketOrdinal);
                } else {
                    collectBucket(sub, doc, bucketOrdinal);
                }
            }
        };
    }

    InternalTimeSeries buildResult(InternalTimeSeries.InternalBucket[] topBuckets) {
        return new InternalTimeSeries(name, List.of(topBuckets), keyed, size, order, metadata());
    }
}
