/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.common.util.LongLongHash;
import org.elasticsearch.compute.aggregation.GroupingAggregator;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.aggregation.SeenGroupIds;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.IntSupplier;

import static java.util.stream.Collectors.joining;

/**
 * Copy of {@link org.elasticsearch.compute.operator.AggregationOperator.AggregationOperatorFactory} but does time series grouping.
 */
public record TimeSeriesAggregationOperatorFactory(
    TimeValue timeSeriesPeriod,
    IntSupplier tsidChannel,
    IntSupplier timestampChannel,
    List<GroupingAggregator.Factory> aggregators
) implements Operator.OperatorFactory {

    @Override
    public Operator get(DriverContext driverContext) {
        return new Impl1(
            timeSeriesPeriod,
            tsidChannel.getAsInt(),
            timestampChannel.getAsInt(),
            aggregators.stream().map(x -> x.apply(driverContext)).toList(),
            driverContext
        );
    }

    @Override
    public String describe() {
        return "AggregationOperator[aggs = " + aggregators.stream().map(GroupingAggregator.Factory::describe).collect(joining(", ")) + "]";
    }

    static final class Impl1 extends AbstractPageMappingOperator {
        private final int tsidChannel;
        private final int timestampIntervalsChannel;
        private final DriverContext driverContext;
        private final List<GroupingAggregator> aggregators;
        private final LongLongHash longLongHash;

        private int currentOrd;

        Impl1(
            TimeValue timeSeriesPeriod,
            int tsidChannel,
            int timestampIntervalsChannel,
            List<GroupingAggregator> aggregators,
            DriverContext driverContext
        ) {
            this.tsidChannel = tsidChannel;
            this.timestampIntervalsChannel = timestampIntervalsChannel;
            this.aggregators = Objects.requireNonNull(aggregators);
            if (aggregators.isEmpty()) {
                throw new IllegalArgumentException("no aggregators");
            }
            this.driverContext = driverContext;
            this.longLongHash = timeSeriesPeriod.equals(TimeValue.ZERO) == false ? new LongLongHash(1, driverContext.bigArrays()) : null;
        }

        @Override
        protected Page process(Page page) {
            IntBlock tsidBlock = page.getBlock(tsidChannel);
            LongBlock timestampIntervalBlock = page.getBlock(timestampIntervalsChannel);

            SeenGroupIds seenGroupIds = new SeenGroupIds.Range(0, currentOrd);
            GroupingAggregatorFunction.AddInput[] prepared = new GroupingAggregatorFunction.AddInput[aggregators.size()];
            for (int i = 0; i < prepared.length; i++) {
                prepared[i] = aggregators.get(i).prepareProcessPage(seenGroupIds, page);
            }

            IntVector tsidVector = tsidBlock.asVector();
            if (tsidVector != null) {
                vectorAddInput(prepared, tsidVector, timestampIntervalBlock.asVector());
            } else {
                blockAddInput(prepared, tsidBlock, timestampIntervalBlock);
            }

            int[] aggBlockCounts = aggregators.stream().mapToInt(GroupingAggregator::evaluateBlockCount).toArray();
            Block[] blocks = new Block[Arrays.stream(aggBlockCounts).sum()];
            boolean success = false;
            IntVector selected = null;
            try {
                selected = IntVector.range(0, currentOrd, driverContext.blockFactory());
                int offset = 0;
                for (int i = 0; i < aggregators.size(); i++) {
                    var aggregator = aggregators.get(i);
                    aggregator.evaluate(blocks, offset, selected, driverContext);
                    offset += aggBlockCounts[i];
                }
                success = true;
                return new Page(blocks.length, blocks);
            } finally {
                // selected should always be closed
                if (selected != null) {
                    selected.close();
                }
                page.releaseBlocks();
                if (success == false) {
                    Releasables.closeExpectNoException(blocks);
                }
            }
        }

        private void blockAddInput(
            GroupingAggregatorFunction.AddInput[] prepared,
            IntBlock tsidOrdBlock,
            LongBlock timestampIntervalBlock
        ) {
            if (longLongHash == null) {
                currentOrd = tsidOrdBlock.getPositionCount() - 1;
                for (var addInput : prepared) {
                    addInput.add(0, tsidOrdBlock);
                }
            } else {
                try (var builder = driverContext.blockFactory().newIntVectorBuilder(tsidOrdBlock.getPositionCount() / 10)) {
                    for (int i = 0; i < tsidOrdBlock.getPositionCount(); i++) {
                        int tsidOrd = tsidOrdBlock.getInt(i);
                        long timestampInterval = timestampIntervalBlock.getLong(i);
                        long groupId = longLongHash.add(tsidOrd, timestampInterval);
                        if (groupId < 0) {
                            groupId = -1 - groupId;
                        }
                        builder.appendInt(Math.toIntExact(groupId));
                    }
                    currentOrd = tsidOrdBlock.getPositionCount() - 1;
                    try (var vector = builder.build()) {
                        for (var addInput : prepared) {
                            addInput.add(0, vector);
                        }
                    }
                }
            }
        }

        private void vectorAddInput(
            GroupingAggregatorFunction.AddInput[] prepared,
            IntVector tsidOrdVector,
            LongVector timestampIntervalVector
        ) {
            if (longLongHash == null) {
                currentOrd = tsidOrdVector.getPositionCount() - 1;
                for (var addInput : prepared) {
                    addInput.add(0, tsidOrdVector);
                }
            } else {
                try (var builder = driverContext.blockFactory().newIntVectorBuilder(tsidOrdVector.getPositionCount() / 10)) {
                    for (int i = 0; i < tsidOrdVector.getPositionCount(); i++) {
                        int tsidOrd = tsidOrdVector.getInt(i);
                        long timestampInterval = timestampIntervalVector.getLong(i);
                        long groupId = longLongHash.add(tsidOrd, timestampInterval);
                        if (groupId < 0) {
                            groupId = -1 - groupId;
                        }
                        builder.appendInt(Math.toIntExact(groupId));
                    }
                    currentOrd = tsidOrdVector.getPositionCount() - 1;
                    try (var vector = builder.build()) {
                        for (var addInput : prepared) {
                            addInput.add(0, vector);
                        }
                    }
                }
            }
        }

        @Override
        public void close() {
            Releasables.close(aggregators);
        }

        @Override
        public String toString() {
            return this.getClass().getSimpleName() + "[" + "aggregators=" + aggregators + "]";
        }
    }

}
