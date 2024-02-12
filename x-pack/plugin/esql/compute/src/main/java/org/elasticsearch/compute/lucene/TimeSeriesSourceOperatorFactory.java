/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.PriorityQueue;
import org.elasticsearch.common.Rounding;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.DocVector;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.function.Function;

public record TimeSeriesSourceOperatorFactory(
    int limit,
    int maxPageSize,
    int taskConcurrency,
    TimeValue timeSeriesPeriod,
    LuceneSliceQueue sliceQueue
) implements LuceneOperator.Factory {

    @Override
    public SourceOperator get(DriverContext driverContext) {
        var rounding = timeSeriesPeriod.equals(TimeValue.ZERO) == false ? Rounding.builder(timeSeriesPeriod).build() : null;
        return new Impl(driverContext.blockFactory(), sliceQueue, maxPageSize, limit, rounding);
    }

    @Override
    public int taskConcurrency() {
        return taskConcurrency;
    }

    @Override
    public String describe() {
        return "TimeSeriesSourceOperator[maxPageSize = " + maxPageSize + ", limit = " + limit + "]";
    }

    public static TimeSeriesSourceOperatorFactory create(
        int limit,
        int maxPageSize,
        int taskConcurrency,
        TimeValue timeSeriesPeriod,
        List<? extends ShardContext> searchContexts,
        Function<ShardContext, Query> queryFunction
    ) {
        var weightFunction = LuceneOperator.weightFunction(queryFunction, ScoreMode.COMPLETE_NO_SCORES);
        var sliceQueue = LuceneSliceQueue.create(searchContexts, weightFunction, DataPartitioning.SHARD, taskConcurrency);
        taskConcurrency = Math.min(sliceQueue.totalSlices(), taskConcurrency);
        return new TimeSeriesSourceOperatorFactory(limit, maxPageSize, taskConcurrency, timeSeriesPeriod, sliceQueue);
    }

    static final class Impl extends SourceOperator {

        private final int minPageSize;
        private final int maxPageSize;
        private final BlockFactory blockFactory;
        private final LuceneSliceQueue sliceQueue;
        private final Rounding.Prepared prepared;
        private int currentPagePos = 0;
        private int remainingDocs;
        private boolean doneCollecting;
        private IntVector.Builder docsBuilder;
        private IntVector.Builder segmentsBuilder;
        private LongVector.Builder timestampIntervalBuilder;
        // TODO: handle tsid spanning multiple backing indices
        // This works well when either the query limits the operation to a single backing index or
        // timestamp interval buckets don't cross backing indices.
        // When a timestamp interval bucket overlaps with two backing indices we need to pull the tsid as bytes for that interval bucket.
        // So that we can merge it later.
        private IntVector.Builder tsOrdBuilder;
        private TimeSeriesIterator iterator;

        Impl(BlockFactory blockFactory, LuceneSliceQueue sliceQueue, int maxPageSize, int limit, Rounding rounding) {
            this.maxPageSize = maxPageSize;
            this.minPageSize = Math.max(1, maxPageSize / 2);
            this.blockFactory = blockFactory;
            this.remainingDocs = limit;
            this.docsBuilder = blockFactory.newIntVectorBuilder(Math.min(limit, maxPageSize));
            this.segmentsBuilder = null;
            this.timestampIntervalBuilder = blockFactory.newLongVectorBuilder(Math.min(limit, maxPageSize));
            this.tsOrdBuilder = blockFactory.newIntVectorBuilder(Math.min(limit, maxPageSize));
            this.sliceQueue = sliceQueue;
            try {
                this.prepared = rounding != null ? sliceQueue.prepareRounding(rounding) : null;
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        @Override
        public void finish() {
            this.doneCollecting = true;
        }

        @Override
        public boolean isFinished() {
            return doneCollecting;
        }

        @Override
        public Page getOutput() {
            if (isFinished()) {
                return null;
            }

            if (remainingDocs <= 0) {
                doneCollecting = true;
                return null;
            }

            Page page = null;
            IntBlock shard = null;
            IntVector leaf = null;
            IntVector docs = null;
            LongVector timestampIntervals = null;
            IntVector tsids = null;
            try {
                if (iterator == null) {
                    var slice = sliceQueue.nextSlice();
                    if (slice == null) {
                        doneCollecting = true;
                        return null;
                    }
                    if (segmentsBuilder == null && slice.numLeaves() > 1) {
                        segmentsBuilder = blockFactory.newIntVectorBuilder(Math.min(remainingDocs, maxPageSize));
                    }
                    iterator = new TimeSeriesIterator(slice);
                }
                iterator.consume();
                shard = blockFactory.newConstantIntBlockWith(iterator.slice.shardContext().index(), currentPagePos);
                boolean singleSegmentNonDecreasing = false;
                if (iterator.slice.numLeaves() == 1) {
                    singleSegmentNonDecreasing = true;
                    int segmentOrd = iterator.slice.getLeaf(0).leafReaderContext().ord;
                    leaf = blockFactory.newConstantIntBlockWith(segmentOrd, currentPagePos).asVector();
                } else {
                    leaf = segmentsBuilder.build();
                    segmentsBuilder = blockFactory.newIntVectorBuilder(Math.min(remainingDocs, maxPageSize));
                }
                docs = docsBuilder.build();
                docsBuilder = blockFactory.newIntVectorBuilder(Math.min(remainingDocs, maxPageSize));

                timestampIntervals = timestampIntervalBuilder.build();
                timestampIntervalBuilder = blockFactory.newLongVectorBuilder(Math.min(remainingDocs, maxPageSize));
                tsids = tsOrdBuilder.build();
                tsOrdBuilder = blockFactory.newIntVectorBuilder(Math.min(remainingDocs, maxPageSize));

                // Due to the multi segment nature of time series source operator singleSegmentNonDecreasing must be false
                page = new Page(
                    currentPagePos,
                    new DocVector(shard.asVector(), leaf, docs, singleSegmentNonDecreasing).asBlock(),
                    tsids.asBlock(),
                    timestampIntervals.asBlock()
                );

                currentPagePos = 0;
                if (iterator.completed()) {
                    iterator = null;
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            } finally {
                if (page == null) {
                    Releasables.closeExpectNoException(shard, leaf, docs, timestampIntervals, tsids);
                }
            }
            return page;
        }

        @Override
        public void close() {
            Releasables.closeExpectNoException(docsBuilder, segmentsBuilder, timestampIntervalBuilder, tsOrdBuilder);
        }

        class TimeSeriesIterator {

            final LuceneSlice slice;
            final Leaf leaf;
            final PriorityQueue<Leaf> queue;
            int globalTsidOrd;
            BytesRef currentTsid = new BytesRef();

            TimeSeriesIterator(LuceneSlice slice) throws IOException {
                this.slice = slice;
                Weight weight = slice.weight().get();
                if (slice.numLeaves() == 1) {
                    queue = null;
                    leaf = new Leaf(weight, slice.getLeaf(0).leafReaderContext());
                } else {
                    queue = new PriorityQueue<>(slice.numLeaves()) {
                        @Override
                        protected boolean lessThan(Leaf a, Leaf b) {
                            // tsid hash in ascending order:
                            int cmp = a.timeSeriesHash.compareTo(b.timeSeriesHash);
                            if (cmp == 0) {
                                // timestamp in descending order:
                                cmp = -Long.compare(a.timestamp, b.timestamp);
                            }
                            return cmp < 0;
                        }
                    };
                    leaf = null;
                    for (var leafReaderContext : slice.leaves()) {
                        Leaf leaf = new Leaf(weight, leafReaderContext.leafReaderContext());
                        if (leaf.nextDoc()) {
                            queue.add(leaf);
                        }
                    }
                }
            }

            void consume() throws IOException {
                if (queue != null) {
                    currentTsid = BytesRef.deepCopyOf(queue.top().timeSeriesHash);
                    boolean needConsumeRemainingDocs = false;
                    while (queue.size() > 0) {
                        if (remainingDocs <= 0 || currentPagePos > maxPageSize) {
                            needConsumeRemainingDocs = true;
                        }

                        currentPagePos++;
                        remainingDocs--;
                        Leaf leaf = queue.top();
                        segmentsBuilder.appendInt(leaf.segmentOrd);
                        docsBuilder.appendInt(leaf.iterator.docID());
                        if (prepared != null) {
                            timestampIntervalBuilder.appendLong(prepared.round(leaf.timestamp));
                        } else {
                            timestampIntervalBuilder.appendLong(leaf.timestamp);
                        }
                        tsOrdBuilder.appendInt(globalTsidOrd);
                        if (leaf.nextDoc()) {
                            Leaf newTop = queue.updateTop();
                            if (newTop.timeSeriesHash.equals(currentTsid) == false) {
                                globalTsidOrd++;
                                currentTsid = BytesRef.deepCopyOf(newTop.timeSeriesHash);
                                if (needConsumeRemainingDocs) {
                                    break;
                                }
                            }
                        } else {
                            queue.pop();
                        }
                    }
                } else {
                    int previousTsidOrd = leaf.timeSeriesHashOrd;
                    boolean needConsumeRemainingDocs = false;
                    // Only one segment, so no need to use priority queue and use segment ordinals as tsid ord.
                    while (leaf.nextDoc()) {
                        if (currentPagePos > maxPageSize || remainingDocs <= 0) {
                            needConsumeRemainingDocs = true;
                        }
                        if (needConsumeRemainingDocs) {
                            if (previousTsidOrd != leaf.timeSeriesHashOrd) {
                                break;
                            }
                        }

                        currentPagePos++;
                        remainingDocs--;

                        tsOrdBuilder.appendInt(leaf.timeSeriesHashOrd);
                        if (prepared != null) {
                            timestampIntervalBuilder.appendLong(prepared.round(leaf.timestamp));
                        } else {
                            timestampIntervalBuilder.appendLong(leaf.timestamp);
                        }
                        // Don't append segment ord, because there is only one segment.
                        docsBuilder.appendInt(leaf.iterator.docID());
                        previousTsidOrd = leaf.timeSeriesHashOrd;
                    }
                }
            }

            boolean completed() {
                if (queue != null) {
                    return iterator.queue.size() == 0;
                } else {
                    return leaf.iterator.docID() == DocIdSetIterator.NO_MORE_DOCS;
                }
            }

            static class Leaf {

                private final int segmentOrd;
                private final SortedDocValues tsids;
                private final SortedNumericDocValues timestamps;
                private final DocIdSetIterator iterator;

                private long timestamp;
                private int timeSeriesHashOrd;
                private BytesRef timeSeriesHash;

                Leaf(Weight weight, LeafReaderContext leaf) throws IOException {
                    this.segmentOrd = leaf.ord;
                    tsids = leaf.reader().getSortedDocValues("_tsid");
                    timestamps = leaf.reader().getSortedNumericDocValues("@timestamp");
                    iterator = weight.scorer(leaf).iterator();
                }

                boolean nextDoc() throws IOException {
                    int docID = iterator.nextDoc();
                    if (docID == DocIdSetIterator.NO_MORE_DOCS) {
                        return false;
                    }

                    boolean advanced = tsids.advanceExact(iterator.docID());
                    assert advanced;
                    timeSeriesHashOrd = tsids.ordValue();
                    timeSeriesHash = tsids.lookupOrd(timeSeriesHashOrd);
                    advanced = timestamps.advanceExact(iterator.docID());
                    assert advanced;
                    timestamp = timestamps.nextValue();
                    return true;
                }

            }

        }

        @Override
        public String toString() {
            return this.getClass().getSimpleName() + "[" + "maxPageSize=" + maxPageSize + ", remainingDocs=" + remainingDocs + "]";
        }

    }
}
