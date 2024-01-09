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
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.DocVector;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.function.Function;

public class TimeSeriesSourceOperator extends SourceOperator {

    public static class Factory implements LuceneOperator.Factory {

        private final int limit;
        private final int maxPageSize;
        private final int taskConcurrency;
        private final LuceneSliceQueue sliceQueue;

        public Factory(
            List<SearchContext> searchContexts,
            Function<SearchContext, Query> queryFunction,
            int taskConcurrency,
            int maxPageSize,
            int limit
        ) {
            this.limit = limit;
            this.maxPageSize = maxPageSize;
            var weightFunction = LuceneOperator.weightFunction(queryFunction, ScoreMode.COMPLETE_NO_SCORES);
            this.sliceQueue = LuceneSliceQueue.create(searchContexts, weightFunction, DataPartitioning.SHARD, taskConcurrency);
            this.taskConcurrency = Math.min(sliceQueue.totalSlices(), taskConcurrency);
        }

        @Override
        public SourceOperator get(DriverContext driverContext) {
            return new TimeSeriesSourceOperator(driverContext.blockFactory(), sliceQueue, maxPageSize, limit);
        }

        @Override
        public int taskConcurrency() {
            return taskConcurrency;
        }

        @Override
        public String describe() {
            return "TimeSeriesSourceOperator[maxPageSize = " + maxPageSize + ", limit = " + limit + "]";
        }
    }

    private final int minPageSize;
    private final int maxPageSize;
    private final BlockFactory blockFactory;
    private final LuceneSliceQueue sliceQueue;

    private int currentPagePos = 0;
    private int remainingDocs;
    private boolean doneCollecting;
    private IntVector.Builder docsBuilder;
    private IntVector.Builder segmentsBuilder;
    private LongVector.Builder timestampBuilder;
    private BytesRefVector.Builder tsidBuilder;
    private TimeSeriesIterator iterator;

    public TimeSeriesSourceOperator(BlockFactory blockFactory, LuceneSliceQueue sliceQueue, int maxPageSize, int limit) {
        this.maxPageSize = maxPageSize;
        this.minPageSize = Math.max(1, maxPageSize / 2);
        this.blockFactory = blockFactory;
        this.remainingDocs = limit;
        this.docsBuilder = blockFactory.newIntVectorBuilder(Math.min(limit, maxPageSize));
        this.segmentsBuilder = blockFactory.newIntVectorBuilder(Math.min(limit, maxPageSize));
        this.timestampBuilder = blockFactory.newLongVectorBuilder(Math.min(limit, maxPageSize));
        this.tsidBuilder = blockFactory.newBytesRefVectorBuilder(Math.min(limit, maxPageSize));
        this.sliceQueue = sliceQueue;
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

        Page page = null;
        IntBlock shard = null;
        IntVector leaf = null;
        IntVector docs = null;
        LongVector timestamps = null;
        BytesRefVector tsids = null;
        try {
            if (iterator == null) {
                var slice = sliceQueue.nextSlice();
                if (slice == null) {
                    doneCollecting = true;
                    return null;
                }
                iterator = new TimeSeriesIterator(slice.weight().get(), slice);
            }
            iterator.consume();
            shard = blockFactory.newConstantIntBlockWith(iterator.slice.shardIndex(), currentPagePos);
            leaf = segmentsBuilder.build();
            segmentsBuilder = blockFactory.newIntVectorBuilder(Math.min(remainingDocs, maxPageSize));
            docs = docsBuilder.build();
            docsBuilder = blockFactory.newIntVectorBuilder(Math.min(remainingDocs, maxPageSize));

            timestamps = timestampBuilder.build();
            timestampBuilder = blockFactory.newLongVectorBuilder(Math.min(remainingDocs, maxPageSize));
            tsids = tsidBuilder.build();
            tsidBuilder = blockFactory.newBytesRefVectorBuilder(Math.min(remainingDocs, maxPageSize));

            // Due to the multi segment nature of time series source operator singleSegmentNonDecreasing must be false
            page = new Page(
                currentPagePos,
                new DocVector(shard.asVector(), leaf, docs, false).asBlock(),
                tsids.asBlock(),
                timestamps.asBlock()
            );

            currentPagePos = 0;
            if (iterator.queue.size() == 0) {
                iterator = null;
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            if (page == null) {
                Releasables.closeExpectNoException(shard, leaf, docs, timestamps, tsids);
            }
        }
        return page;
    }

    @Override
    public void close() {
        Releasables.closeExpectNoException(docsBuilder, segmentsBuilder, timestampBuilder, tsidBuilder);
    }

    class TimeSeriesIterator {

        final LuceneSlice slice;
        final PriorityQueue<Leaf> queue;

        TimeSeriesIterator(Weight weight, LuceneSlice slice) throws IOException {
            this.slice = slice;
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
            for (var leafReaderContext : slice.leaves()) {
                Leaf leaf = new Leaf(weight, leafReaderContext.leafReaderContext());
                if (leaf.nextDoc()) {
                    queue.add(leaf);
                }
            }
        }

        void consume() throws IOException {
            while (currentPagePos < maxPageSize && remainingDocs > 0 && queue.size() > 0) {
                currentPagePos++;
                remainingDocs--;
                Leaf leaf = queue.top();
                segmentsBuilder.appendInt(leaf.segmentOrd);
                docsBuilder.appendInt(leaf.iterator.docID());

                timestampBuilder.appendLong(leaf.timestamp);
                tsidBuilder.appendBytesRef(leaf.timeSeriesHash);

                if (leaf.nextDoc()) {
                    queue.updateTop();
                } else {
                    queue.pop();
                }
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
        StringBuilder sb = new StringBuilder();
        sb.append(this.getClass().getSimpleName()).append("[");
        sb.append("maxPageSize=").append(maxPageSize);
        sb.append(", remainingDocs=").append(remainingDocs);
        sb.append("]");
        return sb.toString();
    }
}
