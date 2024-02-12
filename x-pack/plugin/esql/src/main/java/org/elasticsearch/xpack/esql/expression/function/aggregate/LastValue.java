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
import org.elasticsearch.compute.aggregation.AbstractArrayState;
import org.elasticsearch.compute.aggregation.AggregatorFunction;
import org.elasticsearch.compute.aggregation.AggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.AggregatorState;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.aggregation.GroupingAggregatorState;
import org.elasticsearch.compute.aggregation.IntermediateStateDesc;
import org.elasticsearch.compute.aggregation.SeenGroupIds;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasables;
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
                return new TestGroupingAggregatorFunction(driverContext, inputChannels);
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
//            int size = (int) state.tsids.size();
//            try (
//                var tsidsBuilder = driverContext.blockFactory().newBytesRefBlockBuilder(size);
//                var timestampBuilder = driverContext.blockFactory().newLongBlockBuilder(size);
//                var valuesBuilder = driverContext.blockFactory().newLongBlockBuilder(size)
//            ) {
//                for (long ord = 0; ord < size; ord++) {
//                    tsidsBuilder.appendBytesRef(state.tsids.get(ord, new BytesRef()));
//                    timestampBuilder.appendLong(state.timestamps.get(ord));
//                    if (ord < state.values.size()) {
//                        valuesBuilder.appendLong(state.values.get(ord));
//                    } else {
//                        valuesBuilder.appendLong(0); // TODO can we just use null?
//                    }
//                }
//                blocks[offset + 0] = tsidsBuilder.build();
//                blocks[offset + 1] = timestampBuilder.build();
//                blocks[offset + 2] = valuesBuilder.build();
//            }
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

    static final class TestGroupingAggregatorFunction implements GroupingAggregatorFunction {

        private static final List<IntermediateStateDesc> INTERMEDIATE_STATE_DESC = List.of(
//            new IntermediateStateDesc("tsid", ElementType.BYTES_REF),
            new IntermediateStateDesc("timestamp", ElementType.LONG),
            new IntermediateStateDesc("last_value", ElementType.LONG),
            new IntermediateStateDesc("seen", ElementType.BOOLEAN)
        );

        private final DriverContext driverContext;
        private final List<Integer> channels;
        private final LongArrayState state;

        TestGroupingAggregatorFunction(DriverContext driverContext, List<Integer> channels) {
            this.driverContext = driverContext;
            this.channels = channels;
            this.state = new LongArrayState(driverContext.bigArrays(), 0);
        }

        public static List<IntermediateStateDesc> intermediateStateDesc() {
            return INTERMEDIATE_STATE_DESC;
        }

        @Override
        public int intermediateBlockCount() {
            return INTERMEDIATE_STATE_DESC.size();
        }

        @Override
        public GroupingAggregatorFunction.AddInput prepareProcessPage(SeenGroupIds seenGroupIds,
                                                                      Page page) {
            LongBlock valuesBlock = page.getBlock(channels.get(0));
            LongBlock timestampBlock = page.getBlock(2);
            LongVector valuesVector = valuesBlock.asVector();
            if (valuesVector == null) {
                if (valuesBlock.mayHaveNulls()) {
                    state.enableGroupIdTracking(seenGroupIds);
                }
                return new GroupingAggregatorFunction.AddInput() {
                    @Override
                    public void add(int positionOffset, IntBlock groupIds) {
                        addRawInput(positionOffset, groupIds, valuesBlock, timestampBlock);
                    }

                    @Override
                    public void add(int positionOffset, IntVector groupIds) {
                        addRawInput(positionOffset, groupIds, valuesBlock, timestampBlock);
                    }
                };
            }
            LongVector timestampVector = timestampBlock.asVector();
            return new GroupingAggregatorFunction.AddInput() {
                @Override
                public void add(int positionOffset, IntBlock groupIds) {
                    addRawInput(positionOffset, groupIds, valuesVector, timestampVector);
                }

                @Override
                public void add(int positionOffset, IntVector groupIds) {
                    addRawInput(positionOffset, groupIds, valuesVector, timestampVector);
                }
            };
        }

        private void addRawInput(int positionOffset, IntVector groups, LongBlock values, LongBlock timestampBlock) {
            for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
                int groupId = Math.toIntExact(groups.getInt(groupPosition));
                if (values.isNull(groupPosition + positionOffset)) {
                    continue;
                }
                int valuesStart = values.getFirstValueIndex(groupPosition + positionOffset);
                int valuesEnd = valuesStart + values.getValueCount(groupPosition + positionOffset);
                for (int v = valuesStart; v < valuesEnd; v++) {
                    long newTimestamp = timestampBlock.getLong(v);
                    if (newTimestamp > state.getTimestamp(groupId)) {
                        state.setTimestamp(groupId, newTimestamp);
                        state.setValue(groupId, values.getLong(v));
                    }
                }
            }
        }

        private void addRawInput(int positionOffset, IntVector groups, LongVector values, LongVector timestampVector) {
            for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
                int groupId = Math.toIntExact(groups.getInt(groupPosition));
                long newTimestamp = timestampVector.getLong(groupPosition + positionOffset);
                if (newTimestamp > state.getTimestampOrDefault(groupId)) {
                    state.setTimestamp(groupId, newTimestamp);
                    state.setValue(groupId, values.getLong(groupPosition + positionOffset));
                }
            }
        }

        private void addRawInput(int positionOffset, IntBlock groups, LongBlock values, LongBlock timestampBlock) {
            for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
                if (groups.isNull(groupPosition)) {
                    continue;
                }
                int groupStart = groups.getFirstValueIndex(groupPosition);
                int groupEnd = groupStart + groups.getValueCount(groupPosition);
                for (int g = groupStart; g < groupEnd; g++) {
                    int groupId = Math.toIntExact(groups.getInt(g));
                    if (values.isNull(groupPosition + positionOffset)) {
                        continue;
                    }
                    int valuesStart = values.getFirstValueIndex(groupPosition + positionOffset);
                    int valuesEnd = valuesStart + values.getValueCount(groupPosition + positionOffset);
                    for (int v = valuesStart; v < valuesEnd; v++) {
                        long newTimestamp = timestampBlock.getLong(v);
                        if (newTimestamp > state.getTimestampOrDefault(groupId)) {
                            state.setTimestamp(groupId, newTimestamp);
                            state.setValue(groupId, values.getLong(v));
                        }
                    }
                }
            }
        }

        private void addRawInput(int positionOffset, IntBlock groups, LongVector values, LongVector timestampVector) {
            for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
                if (groups.isNull(groupPosition)) {
                    continue;
                }
                int groupStart = groups.getFirstValueIndex(groupPosition);
                int groupEnd = groupStart + groups.getValueCount(groupPosition);
                for (int g = groupStart; g < groupEnd; g++) {
                    int groupId = Math.toIntExact(groups.getInt(g));
                    long newTimestamp = timestampVector.getLong(groupPosition + positionOffset);
                    if (newTimestamp > state.getTimestampOrDefault(groupId)) {
                        state.setTimestamp(groupId, newTimestamp);
                        state.setValue(groupId, values.getLong(groupPosition + positionOffset));
                    }
                }
            }
        }

        @Override
        public void addIntermediateInput(int positionOffset, IntVector groups, Page page) {
            state.enableGroupIdTracking(new SeenGroupIds.Empty());
            assert channels.size() == intermediateBlockCount();
            LongVector timestamp = page.<LongBlock>getBlock(1).asVector();
            LongVector max = page.<LongBlock>getBlock(channels.get(0)).asVector();
            BooleanVector seen = page.<BooleanBlock>getBlock(channels.get(1)).asVector();
            assert max.getPositionCount() == seen.getPositionCount();
            for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
                int groupId = Math.toIntExact(groups.getInt(groupPosition));
                if (seen.getBoolean(groupPosition + positionOffset)) {
                    long newTimestamp = timestamp.getLong(groupPosition + positionOffset);
                    if (newTimestamp > state.getTimestamp(groupId)) {
                        state.setTimestamp(groupId, newTimestamp);
                        state.setValue(groupId, max.getLong(groupPosition + positionOffset));
                    }
                }
            }
        }

        @Override
        public void addIntermediateRowInput(int groupId, GroupingAggregatorFunction input, int position) {
            if (input.getClass() != getClass()) {
                throw new IllegalArgumentException("expected " + getClass() + "; got " + input.getClass());
            }
            LongArrayState inState = ((TestGroupingAggregatorFunction) input).state;
            state.enableGroupIdTracking(new SeenGroupIds.Empty());
            if (inState.hasValue(position)) {
                long newTimestamp = inState.getTimestamp(position);
                if (newTimestamp > state.getTimestamp(groupId)) {
                    state.setTimestamp(groupId, newTimestamp);
                    state.setValue(groupId, inState.get(position));
                }
            }
        }

        @Override
        public void evaluateIntermediate(Block[] blocks, int offset, IntVector selected) {
            state.toIntermediate(blocks, offset, selected, driverContext);

            try (
                var valuesBuilder = driverContext.blockFactory().newLongBlockBuilder(selected.getPositionCount());
                var hasValueBuilder = driverContext.blockFactory().newBooleanVectorFixedBuilder(selected.getPositionCount())
            ) {
                for (int i = 0; i < selected.getPositionCount(); i++) {
                    int group = selected.getInt(i);
                    if (group < state.values.size()) {
                        valuesBuilder.appendLong(state.get(group));
                    } else {
                        valuesBuilder.appendLong(0); // TODO can we just use null?
                    }
                    hasValueBuilder.appendBoolean(state.hasValue(group));
                }
                blocks[offset + 0] = valuesBuilder.build();
                blocks[offset + 1] = hasValueBuilder.build().asBlock();
            }
        }

        @Override
        public void evaluateFinal(Block[] blocks, int offset, IntVector selected,
                                  DriverContext driverContext) {
            blocks[offset] = state.toValuesBlock(selected, driverContext);
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

   static class LongArrayState extends AbstractArrayState implements GroupingAggregatorState {
        private final long init;
        private LongArray values;
        private LongArray timestamps;

        LongArrayState(BigArrays bigArrays, long init) {
            super(bigArrays);
            this.timestamps = bigArrays.newLongArray(1, false);
            this.values = bigArrays.newLongArray(1, false);
            this.values.set(0, init);
            this.init = init;
        }

        long get(int groupId) {
            return values.get(groupId);
        }

        long getTimestamp(int groupId) {
            return timestamps.get(groupId);
        }

        long getTimestampOrDefault(int groupId) {
            return groupId < timestamps.size() ? timestamps.get(groupId) : 0;
        }

        void setValue(int groupId, long value) {
            values.set(groupId, value);
        }

        void setTimestamp(int groupId, long timestamp) {
            ensureCapacity(groupId);
            timestamps.set(groupId, timestamp);
            trackGroupId(groupId);
        }

        Block toValuesBlock(org.elasticsearch.compute.data.IntVector selected, DriverContext driverContext) {
            if (false == trackingGroupIds()) {
                try (LongVector.Builder builder = driverContext.blockFactory().newLongVectorFixedBuilder(selected.getPositionCount())) {
                    for (int i = 0; i < selected.getPositionCount(); i++) {
                        builder.appendLong(values.get(selected.getInt(i)));
                    }
                    return builder.build().asBlock();
                }
            }
            try (LongBlock.Builder builder = driverContext.blockFactory().newLongBlockBuilder(selected.getPositionCount())) {
                for (int i = 0; i < selected.getPositionCount(); i++) {
                    int group = selected.getInt(i);
                    if (hasValue(group)) {
                        builder.appendLong(values.get(group));
                    } else {
                        builder.appendNull();
                    }
                }
                return builder.build();
            }
        }

        private void ensureCapacity(int groupId) {
            if (groupId >= values.size()) {
                long prevSize = values.size();
                values = bigArrays.grow(values, groupId + 1);
                values.fill(prevSize, values.size(), init);
                timestamps = bigArrays.grow(timestamps, groupId + 1);
            }
        }

        /** Extracts an intermediate view of the contents of this state.  */
        @Override
        public void toIntermediate(
            Block[] blocks,
            int offset,
            IntVector selected,
            org.elasticsearch.compute.operator.DriverContext driverContext
        ) {
            assert blocks.length >= offset + 3;
            try (
                var timestampBuilder = driverContext.blockFactory().newLongBlockBuilder(selected.getPositionCount());
                var valuesBuilder = driverContext.blockFactory().newLongBlockBuilder(selected.getPositionCount());
                var hasValueBuilder = driverContext.blockFactory().newBooleanVectorFixedBuilder(selected.getPositionCount())
            ) {
                for (int i = 0; i < selected.getPositionCount(); i++) {
                    int group = selected.getInt(i);
                    if (group < values.size()) {
                        timestampBuilder.appendLong(timestamps.get(group));
                        valuesBuilder.appendLong(values.get(group));
                    } else {
                        valuesBuilder.appendLong(0); // TODO can we just use null?
                    }
                    hasValueBuilder.appendBoolean(hasValue(group));
                }
                blocks[offset + 0] = timestampBuilder.build();
                blocks[offset + 1] = valuesBuilder.build();
                blocks[offset + 2] = hasValueBuilder.build().asBlock();
            }
        }

        @Override
        public void close() {
            Releasables.close(values, super::close);
        }
    }

}
