/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BytesRefHash;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.compute.aggregation.AggregatorFunction;
import org.elasticsearch.compute.aggregation.AggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.AggregatorState;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.aggregation.IntermediateStateDesc;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;

import java.util.List;

public class LastValue extends NumericAggregate {

    public LastValue(Source source, Expression field) {
        super(source, field);
    }

    @Override
    protected AggregatorFunctionSupplier longSupplier(List<Integer> inputChannels) {
        return new AggregatorFunctionSupplier() {
            @Override
            public AggregatorFunction aggregator(DriverContext driverContext) {
                return new TestAggFunction(driverContext, inputChannels);
            }

            @Override
            public GroupingAggregatorFunction groupingAggregator(DriverContext driverContext) {
                throw new UnsupportedOperationException();
            }

            @Override
            public String describe() {
                return "";
            }
        };
    }

    @Override
    protected AggregatorFunctionSupplier intSupplier(List<Integer> inputChannels) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected AggregatorFunctionSupplier doubleSupplier(List<Integer> inputChannels) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new LastValue(source(), newChildren.get(0));
    }

    public DataType dataType() {
        return field().dataType();
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, LastValue::new, field());
    }

    static final class TestAggFunction implements AggregatorFunction {

        private static final List<IntermediateStateDesc> INTERMEDIATE_STATE_DESC = List.of(
            new IntermediateStateDesc("tsid", ElementType.BYTES_REF),
            new IntermediateStateDesc("timestamp", ElementType.LONG),
            new IntermediateStateDesc("last_value", ElementType.LONG),
            new IntermediateStateDesc("seen", ElementType.BOOLEAN)
        );

        private final DriverContext driverContext;

        private final LastValueState state;

        private final List<Integer> channels;

        TestAggFunction(DriverContext driverContext, List<Integer> channels) {
            this.driverContext = driverContext;
            this.channels = channels;
            this.state = new LastValueState(driverContext);
        }

        public static List<IntermediateStateDesc> intermediateStateDesc() {
            return INTERMEDIATE_STATE_DESC;
        }

        @Override
        public int intermediateBlockCount() {
            return INTERMEDIATE_STATE_DESC.size();
        }

        @Override
        public void addRawInput(Page page) {
            BytesRefBlock tsidBlock = page.getBlock(1);
            LongBlock timestampBlock = page.getBlock(2);
            LongBlock valuesBlock = page.getBlock(channels.get(0));
            LongVector vector = valuesBlock.asVector();
            if (vector != null) {
                addRawVector(tsidBlock.asVector(), timestampBlock.asVector(), vector);
            } else {
                addRawBlock(tsidBlock, timestampBlock, valuesBlock);
            }
        }

        private void addRawVector(BytesRefVector tsidBlockVector, LongVector timestampBlockVector, LongVector vector) {
            for (int i = 0; i < vector.getPositionCount(); i++) {
                // LastValueLongAggregator.combine(state.longValue(), vector.getLong(i))
                BytesRef tsid = tsidBlockVector.getBytesRef(i, new BytesRef());
                long ord = state.valueToOrd(tsid);
                long newTimestamp = timestampBlockVector.getLong(i);
                if (newTimestamp > state.getTimestamp(ord)) {
                    state.setTimestamp(ord, newTimestamp);
                    state.setValue(ord, vector.getLong(i));
                }
            }
        }

        private void addRawBlock(BytesRefBlock tsidBlock, LongBlock timestampBlock, LongBlock block) {
            for (int p = 0; p < block.getPositionCount(); p++) {
                if (block.isNull(p)) {
                    continue;
                }
                int start = block.getFirstValueIndex(p);
                int end = start + block.getValueCount(p);
                for (int i = start; i < end; i++) {
                    // LastValueLongAggregator.combine(state.longValue(), block.getLong(i))
                    BytesRef tsid = tsidBlock.getBytesRef(i, new BytesRef());
                    long ord = state.valueToOrd(tsid);
                    long newTimestamp = timestampBlock.getLong(i);
                    if (newTimestamp > state.getTimestamp(ord)) {
                        state.setTimestamp(ord, newTimestamp);
                        state.setValue(ord, block.getLong(i));
                    }
                }
            }
        }

        @Override
        public void addIntermediateInput(Page page) {
            assert channels.size() == intermediateBlockCount();
            assert page.getBlockCount() >= channels.get(0) + intermediateStateDesc().size();
            // TODO: hard coded channels:
            // Block last_valueUncast = page.getBlock(channels.get(0));
            Block last_valueUncast = page.getBlock(2);
            if (last_valueUncast.areAllValuesNull()) {
                return;
            }
            LongVector last_value = ((LongBlock) last_valueUncast).asVector();
            // TODO: hard coded channels:
            // Block seenUncast = page.getBlock(channels.get(1));
            Block seenUncast = page.getBlock(3);
            if (seenUncast.areAllValuesNull()) {
                return;
            }
            BooleanVector seen = ((BooleanBlock) seenUncast).asVector();
            if (seen.getBoolean(0)) {
                // LastValueLongAggregator.combine(state.longValue(), last_value.getLong(0))
                Block tsidUncast = page.getBlock(0);
                BytesRefVector tsids = (BytesRefVector) tsidUncast.asVector();
                Block timestampUncast = page.getBlock(1);
                LongVector timestamp = (LongVector) timestampUncast.asVector();
                for (int ord1 = 0; ord1 < tsids.getPositionCount(); ord1++) {
                    BytesRef tsid = tsids.getBytesRef(ord1, new BytesRef());
                    long ord2 = state.tsids.find(tsid);
                    if (ord2 == -1) {
                        ord2 = state.valueToOrd(tsid);
                        state.setTimestamp(ord2, timestamp.getLong(ord1));
                        state.setValue(ord2, last_value.getLong(ord1));
                    } else {
                        if (timestamp.getLong(ord1) > state.timestamps.get(ord2)) {
                            state.setTimestamp(ord2, timestamp.getLong(ord1));
                            state.setValue(ord2, last_value.getLong(ord1));
                        }
                    }
                }
            }
        }

        @Override
        public void evaluateIntermediate(Block[] blocks, int offset, DriverContext driverContext) {
            state.toIntermediate(blocks, offset, driverContext);
        }

        @Override
        public void evaluateFinal(Block[] blocks, int offset, DriverContext driverContext) {
            int size = (int) state.tsids.size();
            try (
                var valuesBuilder = driverContext.blockFactory().newLongBlockBuilder(size)
            ) {
                for (long ord = 0; ord < size; ord++) {
                    valuesBuilder.appendLong(state.values.get(ord));
                }
                blocks[offset] = valuesBuilder.build();
            }
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append(getClass().getSimpleName()).append("[");
            sb.append("channels=").append(channels);
            sb.append("]");
            return sb.toString();
        }

        @Override
        public void close() {
            state.close();
        }

    }

    // TODO: the grouping should really be done in TimeSeriesGroupingOperator
    static final class LastValueState implements AggregatorState {

        private final BigArrays bigArrays;
        private BytesRefHash tsids;
        private LongArray timestamps;
        private LongArray values;

        LastValueState(DriverContext driverContext) {
            this.bigArrays = driverContext.bigArrays();
            this.tsids = new BytesRefHash(1, driverContext.bigArrays());
            this.timestamps = driverContext.bigArrays().newLongArray(1);
            this.values = driverContext.bigArrays().newLongArray(1);
        }

        long valueToOrd(BytesRef tsid) {
            long ord = tsids.add(tsid);
            if (ord < 0) {
                ord = -1 - ord;
            } else {
                if (ord >= values.size()) {
                    long prevSize = values.size();
                    values = bigArrays.grow(values, ord + 1);
                    values.fill(prevSize, values.size(), 0);
                    timestamps = bigArrays.grow(timestamps, ord + 1);
                    timestamps.fill(prevSize, timestamps.size(), 0);
                }
            }
            return ord;
        }

        long getTimestamp(long ord) {
            return timestamps.get(ord);
        }

        void setTimestamp(long ord, long timestamp) {
            timestamps.set(ord, timestamp);
        }

        void setValue(long ord, long value) {
            values.set(ord, value);
        }

        /** Extracts an intermediate view of the contents of this state.  */
        @Override
        public void toIntermediate(Block[] blocks, int offset, DriverContext driverContext) {
            assert blocks.length >= offset + 4;
            int size = (int) tsids.size();
            try (
                var tsidsBuilder = driverContext.blockFactory().newBytesRefBlockBuilder(size);
                var timestampBuilder = driverContext.blockFactory().newLongBlockBuilder(size);
                var valuesBuilder = driverContext.blockFactory().newLongBlockBuilder(size)
            ) {
                for (long ord = 0; ord < size; ord++) {
                    tsidsBuilder.appendBytesRef(tsids.get(ord, new BytesRef()));
                    timestampBuilder.appendLong(timestamps.get(ord));
                    if (ord < values.size()) {
                        valuesBuilder.appendLong(values.get(ord));
                    } else {
                        valuesBuilder.appendLong(0); // TODO can we just use null?
                    }
                }
                blocks[offset + 0] = tsidsBuilder.build();
                blocks[offset + 1] = timestampBuilder.build();
                blocks[offset + 2] = valuesBuilder.build();
            }
            blocks[offset + 3] = driverContext.blockFactory().newConstantBooleanBlockWith(true, size);
        }

        @Override
        public void close() {}
    }

}
