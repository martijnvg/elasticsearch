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
import org.elasticsearch.compute.data.DocVector;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.function.Function;

public record TimeSeriesLastSampleOperatorFactory(int limit, int maxPageSize, int taskConcurrency, LuceneSliceQueue sliceQueue)
    implements
        LuceneOperator.Factory {


    @Override
    public SourceOperator get(DriverContext driverContext) {
        return new Impl(driverContext.blockFactory(), sliceQueue, maxPageSize, limit);
    }

    @Override
    public String describe() {
        return "TimeSeriesLastSampleOperatorFactory[maxPageSize = " + maxPageSize + ", limit = " + limit + "]";
    }

    public static TimeSeriesLastSampleOperatorFactory create(
        int limit,
        int maxPageSize,
        int taskConcurrency,
        List<? extends ShardContext> searchContexts,
        Function<ShardContext, Query> queryFunction
    ) {
        var weightFunction = LuceneOperator.weightFunction(queryFunction, ScoreMode.COMPLETE_NO_SCORES);
        var sliceQueue = LuceneSliceQueue.create(searchContexts, weightFunction, DataPartitioning.SHARD, taskConcurrency);
        taskConcurrency = Math.min(sliceQueue.totalSlices(), taskConcurrency);
        return new TimeSeriesLastSampleOperatorFactory(limit, maxPageSize, taskConcurrency, sliceQueue);
    }

    static final class Impl extends SourceOperator {

        private final int minPageSize;
        private final int maxPageSize;
        private final BlockFactory blockFactory;
        private final LuceneSliceQueue sliceQueue;
        private int currentPagePos = 0;
        private int remainingDocs;
        private boolean doneCollecting;
        private IntVector.Builder docsBuilder;
        private IntVector.Builder segmentsBuilder;
        private TimeSeriesIterator iterator;

        Impl(BlockFactory blockFactory, LuceneSliceQueue sliceQueue, int maxPageSize, int limit) {
            this.maxPageSize = maxPageSize;
            this.minPageSize = Math.max(1, maxPageSize / 2);
            this.blockFactory = blockFactory;
            this.remainingDocs = limit;
            this.docsBuilder = blockFactory.newIntVectorBuilder(Math.min(limit, maxPageSize));
            this.segmentsBuilder = null;
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

            if (remainingDocs <= 0) {
                doneCollecting = true;
                return null;
            }

            Page page = null;
            IntBlock shard = null;
            IntVector leaf = null;
            IntVector docs = null;
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

                // Due to the multi segment nature of time series source operator singleSegmentNonDecreasing must be false
                page = new Page(
                    currentPagePos,
                    new DocVector(shard.asVector(), leaf, docs, singleSegmentNonDecreasing).asBlock()
                );

                currentPagePos = 0;
                if (iterator.completed()) {
                    iterator = null;
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            } finally {
                if (page == null) {
                    Releasables.closeExpectNoException(shard, leaf, docs);
                }
            }
            return page;
        }

        @Override
        public void close() {
            Releasables.closeExpectNoException(docsBuilder, segmentsBuilder);
        }

        class TimeSeriesIterator {

            final LuceneSlice slice;
            final Leaf leaf;
            final PriorityQueue<Leaf> queue;
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
                        if (leaf.nextOrd()) {
                            queue.add(leaf);
                        }
                    }

                    currentTsid = BytesRef.deepCopyOf(queue.top().timeSeriesHash);
                    docsBuilder.appendInt(queue.top().iterator.docID());
                    segmentsBuilder.appendInt(queue.top().segmentOrd);
                    currentPagePos++;
                    remainingDocs--;
                }
            }

            void consume() throws IOException {
                if (queue != null) {
                    while (queue.size() > 0) {
                        if (remainingDocs <= 0 || currentPagePos > maxPageSize) {
                            break;
                        }

                        Leaf leaf = queue.top();
                        int prevTopDocId = leaf.iterator.docID();
                        int prevSegmentOrd = leaf.segmentOrd;
                        if (leaf.nextOrd()) {
                            Leaf newTop = queue.updateTop();
                            if (newTop.timeSeriesHash.equals(currentTsid) == false) {
                                currentTsid = BytesRef.deepCopyOf(newTop.timeSeriesHash);

                                // Append first doc, since that is the last sample of this tsid (most recent timestamp)
                                docsBuilder.appendInt(prevTopDocId);
                                segmentsBuilder.appendInt(prevSegmentOrd);

                                currentPagePos++;
                                remainingDocs--;

                                // TODO: need skip to next tsid
                            }
                        } else {
                            queue.pop();
                        }
                    }
                } else {
                    int previousTsidOrd = leaf.timeSeriesHashOrd;
                    // Only one segment, so no need to use priority queue and use segment ordinals as tsid ord.
                    while (leaf.nextDoc()) {
                        if (currentPagePos > maxPageSize || remainingDocs <= 0) {
                            break;
                        }
                        if (previousTsidOrd != leaf.timeSeriesHashOrd) {
                            // Append first doc, since that is the last sample of this tsid (most recent timestamp)
                            docsBuilder.appendInt(leaf.iterator.docID());
                            // Don't append segment ord, because there is only one segment.
                            previousTsidOrd = leaf.timeSeriesHashOrd;

                            currentPagePos++;
                            remainingDocs--;

                            // TODO: need skip to next tsid.
                            // (maybe index the _tsid field? Or special docvalues format that knows how to skip forward)
                        }
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
                private int timeSeriesHashOrd = -1;
                private BytesRef timeSeriesHash;

                Leaf(Weight weight, LeafReaderContext leaf) throws IOException {
                    this.segmentOrd = leaf.ord;
                    this.tsids = leaf.reader().getSortedDocValues("_tsid");
                    this.timestamps = leaf.reader().getSortedNumericDocValues("@timestamp");
                    this.iterator = weight.scorer(leaf).iterator();
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

                boolean nextOrd() throws IOException {
                    do {
                        int docID = iterator.nextDoc();
                        if (docID == DocIdSetIterator.NO_MORE_DOCS) {
                            return false;
                        }
                        boolean advanced = tsids.advanceExact(docID);
                        assert advanced;
                    } while (timeSeriesHashOrd != tsids.ordValue());
                    timeSeriesHashOrd = tsids.ordValue();

                    timeSeriesHash = tsids.lookupOrd(timeSeriesHashOrd);
                    boolean advanced = timestamps.advanceExact(iterator.docID());
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
