/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.timeseries;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.BytesRefHash;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.mapper.TimeSeriesIdFieldMapper;
import org.elasticsearch.search.aggregations.AggregationReduceContext;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.InternalMultiBucketAggregation;
import org.elasticsearch.search.aggregations.InternalOrder;
import org.elasticsearch.search.aggregations.bucket.IteratorAndCurrent;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.search.aggregations.ParsedMultiBucketAggregation.declareMultiBucketAggregationFields;

public class InternalTimeSeries extends InternalMultiBucketAggregation<InternalTimeSeries, InternalTimeSeries.InternalBucket>
    implements
        TimeSeries {

    private static final ObjectParser<ParsedTimeSeries, Void> PARSER = new ObjectParser<>(
        ParsedTimeSeries.class.getSimpleName(),
        true,
        ParsedTimeSeries::new
    );
    static {
        declareMultiBucketAggregationFields(
            PARSER,
            parser -> ParsedTimeSeries.ParsedBucket.fromXContent(parser, false),
            parser -> ParsedTimeSeries.ParsedBucket.fromXContent(parser, true)
        );
    }

    public static class InternalBucket extends InternalMultiBucketAggregation.InternalBucket implements TimeSeries.Bucket {
        protected long bucketOrd;
        protected final boolean keyed;
        protected final BytesRef key;
        protected long docCount;
        protected InternalAggregations aggregations;

        public InternalBucket(BytesRef key, long docCount, InternalAggregations aggregations, boolean keyed) {
            this.key = key;
            this.docCount = docCount;
            this.aggregations = aggregations;
            this.keyed = keyed;
        }

        /**
         * Read from a stream.
         */
        public InternalBucket(StreamInput in, boolean keyed) throws IOException {
            this.keyed = keyed;
            key = in.readBytesRef();
            docCount = in.readVLong();
            aggregations = InternalAggregations.readFrom(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeBytesRef(key);
            out.writeVLong(docCount);
            aggregations.writeTo(out);
        }

        @Override
        public BytesRef getKey() {
            return key;
        }

        public Map<?, ?> getKeyAsMap() {
            return TimeSeriesIdFieldMapper.decodeTsid(key);
        }

        @Override
        public String getKeyAsString() {
            return key.toString();
        }

        @Override
        public long getDocCount() {
            return docCount;
        }

        @Override
        public InternalAggregations getAggregations() {
            return aggregations;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            if (keyed) {
                builder.startObject(getKeyAsString());
            } else {
                builder.startObject();
            }
            builder.field(CommonFields.KEY.getPreferredName(), getKeyAsMap());
            builder.field(CommonFields.DOC_COUNT.getPreferredName(), docCount);
            aggregations.toXContentInternal(builder, params);
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (other == null || getClass() != other.getClass()) {
                return false;
            }
            InternalTimeSeries.InternalBucket that = (InternalTimeSeries.InternalBucket) other;
            return Objects.equals(key, that.key)
                && Objects.equals(keyed, that.keyed)
                && Objects.equals(docCount, that.docCount)
                && Objects.equals(aggregations, that.aggregations);
        }

        @Override
        public int hashCode() {
            return Objects.hash(getClass(), key, keyed, docCount, aggregations);
        }
    }

    private final List<InternalTimeSeries.InternalBucket> buckets;
    private final boolean keyed;
    private final int size;
    private final BucketOrder order;
    // bucketMap gets lazily initialized from buckets in getBucketByKey()
    private transient Map<String, InternalTimeSeries.InternalBucket> bucketMap;

    public InternalTimeSeries(
        String name,
        List<InternalTimeSeries.InternalBucket> buckets,
        boolean keyed,
        int size,
        BucketOrder order,
        Map<String, Object> metadata
    ) {
        super(name, metadata);
        this.buckets = buckets;
        this.keyed = keyed;
        this.size = size;
        this.order = order;
    }

    /**
     * Read from a stream.
     */
    public InternalTimeSeries(StreamInput in) throws IOException {
        super(in);
        keyed = in.readBoolean();
        size = in.readVInt();
        order = in.readOptionalWriteable(InternalOrder.Streams::readOrder);
        int size = in.readVInt();
        List<InternalTimeSeries.InternalBucket> buckets = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            buckets.add(new InternalTimeSeries.InternalBucket(in, keyed));
        }
        this.buckets = buckets;
        this.bucketMap = null;
    }

    @Override
    public String getWriteableName() {
        return TimeSeriesAggregationBuilder.NAME;
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        if (keyed) {
            builder.startObject(CommonFields.BUCKETS.getPreferredName());
        } else {
            builder.startArray(CommonFields.BUCKETS.getPreferredName());
        }
        for (InternalBucket bucket : buckets) {
            bucket.toXContent(builder, params);
        }
        if (keyed) {
            builder.endObject();
        } else {
            builder.endArray();
        }
        return builder;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeBoolean(keyed);
        out.writeVInt(size);
        out.writeOptionalWriteable(order);
        out.writeCollection(buckets);
    }

    @Override
    public InternalAggregation reduce(List<InternalAggregation> aggregations, AggregationReduceContext reduceContext) {
        if (aggregations.size() == 1) {
            List<InternalBucket> buckets = ((InternalTimeSeries) aggregations.get(0)).getBuckets();
            if (buckets.size() > size) {
                buckets = buckets.subList(0, size);
            }
            return new InternalTimeSeries(name, buckets, keyed, size, order, getMetadata());
        }

        Map<BytesRef, List<Tuple<Integer, Integer>>> duplicates = new HashMap<>();
        try (BytesRefHash tsids = new BytesRefHash(1, reduceContext.bigArrays())) {
            for (int i = 0; i < aggregations.size(); i++) {
                InternalTimeSeries timeSeries = (InternalTimeSeries) aggregations.get(i);
                for (int j = 0; j < timeSeries.getBuckets().size(); j++) {
                    InternalBucket bucket = timeSeries.getBuckets().get(j);
                    long key = tsids.add(bucket.getKey());
                    if (key < 0) {
                        int responseIndex = i;
                        int bucketIndex = j;
                        duplicates.compute(bucket.getKey(), (bytesRef, tuples) -> {
                            if (tuples == null) {
                                tuples = new ArrayList<>();
                            }
                            tuples.add(new Tuple<>(responseIndex, bucketIndex));
                            return tuples;
                        });
                    }
                }
            }
        }

        for (Map.Entry<BytesRef, List<Tuple<Integer, Integer>>> entry : duplicates.entrySet()) {
            List<InternalBucket> buckets = new ArrayList<>(entry.getValue().size());
            for (Tuple<Integer, Integer> tuple : entry.getValue()) {
                InternalTimeSeries timeSeries = (InternalTimeSeries) aggregations.get(tuple.v1());
                buckets.add(timeSeries.getBuckets().get(tuple.v2()));
            }
            InternalBucket reduced = reduceBucket(buckets, reduceContext);
        }

        final List<IteratorAndCurrent<InternalBucket>> iterators = new ArrayList<>(aggregations.size());
        for (InternalAggregation aggregation : aggregations) {
            InternalTimeSeries timeSeries = (InternalTimeSeries) aggregation;
            if (timeSeries.buckets.isEmpty() == false) {
                IteratorAndCurrent<InternalBucket> iterator = new IteratorAndCurrent<>(timeSeries.buckets.iterator());
                iterators.add(iterator);
            }
        }
        final Comparator<MultiBucketsAggregation.Bucket> comparator = order != null
            ? order.comparator()
            : Comparator.comparing(MultiBucketsAggregation.Bucket::getKeyAsString);
        final List<InternalBucket> merged = new ArrayList<>(size);
        while (iterators.isEmpty() == false && merged.size() < size) {
            int competitiveSlot = 0;
            InternalBucket competitive = iterators.get(0).current();
            for (int i = 1; i < iterators.size(); i++) {
                InternalBucket contender = iterators.get(i).current();
                int cmp = comparator.compare(competitive, contender);
                if (cmp < 0) {
                    competitive = contender;
                    competitiveSlot = i;
                }
            }

            merged.add(competitive);
            if (iterators.get(competitiveSlot).hasNext()) {
                iterators.get(competitiveSlot).next();
            } else {
                iterators.remove(competitiveSlot);
            }
        }
        return new InternalTimeSeries(name, merged, keyed, size, order, getMetadata());
    }

    @Override
    public InternalTimeSeries create(List<InternalBucket> buckets) {
        return new InternalTimeSeries(name, buckets, keyed, size, order, metadata);
    }

    @Override
    public InternalBucket createBucket(InternalAggregations aggregations, InternalBucket prototype) {
        return new InternalBucket(prototype.key, prototype.docCount, aggregations, prototype.keyed);
    }

    @Override
    protected InternalBucket reduceBucket(List<InternalBucket> buckets, AggregationReduceContext context) {
        InternalTimeSeries.InternalBucket reduced = null;
        List<InternalAggregations> aggregationsList = new ArrayList<>(buckets.size());
        for (InternalTimeSeries.InternalBucket bucket : buckets) {
            if (reduced == null) {
                reduced = new InternalTimeSeries.InternalBucket(bucket.key, bucket.docCount, bucket.aggregations, bucket.keyed);
            } else {
                reduced.docCount += bucket.docCount;
            }
            aggregationsList.add(bucket.aggregations);
        }
        reduced.aggregations = InternalAggregations.reduce(aggregationsList, context);
        return reduced;
    }

    @Override
    public List<InternalBucket> getBuckets() {
        return buckets;
    }

    @Override
    public InternalBucket getBucketByKey(String key) {
        if (bucketMap == null) {
            bucketMap = new HashMap<>(buckets.size());
            for (InternalBucket bucket : buckets) {
                bucketMap.put(bucket.getKeyAsString(), bucket);
            }
        }
        return bucketMap.get(key);
    }
}
