/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ccr.action;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.single.shard.SingleShardRequest;
import org.elasticsearch.action.support.single.shard.TransportSingleShardAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.ShardsIterator;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardNotStartedException;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class ShardChangesAction extends Action<ShardChangesAction.Response> {

    public static final ShardChangesAction INSTANCE = new ShardChangesAction();
    public static final String NAME = "indices:data/read/xpack/ccr/shard_changes";

    private ShardChangesAction() {
        super(NAME);
    }

    @Override
    public Response newResponse() {
        return new Response();
    }

    public static class Request extends SingleShardRequest<Request> {

        private ShardId shardId;
        private long size;
        private long fromSeqNo;
        private long maxTranslogsBytes = ShardFollowTasksExecutor.DEFAULT_MAX_TRANSLOG_BYTES;

        public Request(ShardId shardId) {
            super(shardId.getIndexName());
            this.shardId = shardId;
        }

        Request() {
        }

        public ShardId getShard() {
            return shardId;
        }

        public void setSize(long size) {
            this.size = size;
        }

        public long getSize() {
            return size;
        }

        public void setFromSeqNo(long fromSeqNo) {
            this.fromSeqNo = fromSeqNo;
        }

        public long getFromSeqNo() {
            return fromSeqNo;
        }

        public void setMaxTranslogsBytes(long maxTranslogsBytes) {
            this.maxTranslogsBytes = maxTranslogsBytes;
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;
            if (fromSeqNo < 0) {
                validationException = addValidationError("fromSeqNo [" + fromSeqNo + "] cannot be lower than 0", validationException);
            }
            if (size < 0) {
                validationException = addValidationError("size [" + size + "] cannot be lower than 0", validationException);
            }
            if (maxTranslogsBytes <= 0) {
                validationException = addValidationError("maxTranslogsBytes [" + maxTranslogsBytes + "] cannot be lower than 0",
                    validationException);
            }
            return validationException;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            fromSeqNo = in.readVLong();
            size = in.readVLong();
            shardId = ShardId.readShardId(in);
            maxTranslogsBytes = in.readVLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeVLong(fromSeqNo);
            out.writeVLong(size);
            shardId.writeTo(out);
            out.writeVLong(maxTranslogsBytes);
        }


        @Override
        public boolean equals(final Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            final Request request = (Request) o;
            return fromSeqNo == request.fromSeqNo &&
                    size == request.size &&
                    Objects.equals(shardId, request.shardId) &&
                    maxTranslogsBytes == request.maxTranslogsBytes;
        }

        @Override
        public int hashCode() {
            return Objects.hash(fromSeqNo, size, shardId, maxTranslogsBytes);
        }
    }

    public static final class Response extends ActionResponse {

        private long indexMetadataVersion;
        private long leaderGlobalCheckpoint;
        private Translog.Operation[] operations;

        Response() {
        }

        Response(long indexMetadataVersion, long leaderGlobalCheckpoint, final Translog.Operation[] operations) {
            this.indexMetadataVersion = indexMetadataVersion;
            this.leaderGlobalCheckpoint = leaderGlobalCheckpoint;
            this.operations = operations;
        }

        public long getIndexMetadataVersion() {
            return indexMetadataVersion;
        }

        public long getLeaderGlobalCheckpoint() {
            return leaderGlobalCheckpoint;
        }

        public Translog.Operation[] getOperations() {
            return operations;
        }

        @Override
        public void readFrom(final StreamInput in) throws IOException {
            super.readFrom(in);
            indexMetadataVersion = in.readVLong();
            leaderGlobalCheckpoint = in.readZLong();
            operations = in.readArray(Translog.Operation::readOperation, Translog.Operation[]::new);
        }

        @Override
        public void writeTo(final StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeVLong(indexMetadataVersion);
            out.writeZLong(leaderGlobalCheckpoint);
            out.writeArray(Translog.Operation::writeOperation, operations);
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            final Response response = (Response) o;
            return indexMetadataVersion == response.indexMetadataVersion &&
                    leaderGlobalCheckpoint == response.leaderGlobalCheckpoint &&
                    Arrays.equals(operations, response.operations);
        }

        @Override
        public int hashCode() {
            int result = 1;
            result += Objects.hashCode(indexMetadataVersion);
            result += Objects.hashCode(leaderGlobalCheckpoint);
            result += Arrays.hashCode(operations);
            return result;
        }
    }

    public static class TransportAction extends TransportSingleShardAction<Request, Response> {

        private final IndicesService indicesService;

        @Inject
        public TransportAction(Settings settings,
                               ThreadPool threadPool,
                               ClusterService clusterService,
                               TransportService transportService,
                               ActionFilters actionFilters,
                               IndexNameExpressionResolver indexNameExpressionResolver,
                               IndicesService indicesService) {
            super(settings, NAME, threadPool, clusterService, transportService, actionFilters,
                    indexNameExpressionResolver, Request::new, ThreadPool.Names.GET);
            this.indicesService = indicesService;
        }

        @Override
        protected Response shardOperation(Request request, ShardId shardId) throws IOException {
            IndexService indexService = indicesService.indexServiceSafe(request.getShard().getIndex());
            IndexShard indexShard = indexService.getShard(request.getShard().id());

            final long indexMetaDataVersion = clusterService.state().metaData().index(shardId.getIndex()).getVersion();
            final long globalCheckpoint = indexShard.getGlobalCheckpoint();
            if (globalCheckpoint == SequenceNumbers.NO_OPS_PERFORMED || request.fromSeqNo > globalCheckpoint) {
                return new Response(indexMetaDataVersion, globalCheckpoint, EMPTY_OPERATIONS_ARRAY);
            }

            // The following shard generates this request based on the global checkpoint on the primary copy on the leader.
            // Although this value might not have been synced to all replica copies on the leader, the requesting range
            // is guaranteed to be at most the local-checkpoint of any shard copies on the leader.
            assert request.fromSeqNo <= globalCheckpoint : "invalid request from_seqno=[" + request.fromSeqNo + "]," +
                " size=[" + request.size + "], global_checkpoint=[" + globalCheckpoint + "]";
            final Translog.Operation[] operations = getOperationsBetween(indexShard, request.fromSeqNo, request.size,
                request.maxTranslogsBytes);
            return new Response(indexMetaDataVersion, globalCheckpoint, operations);
        }

        @Override
        protected boolean resolveIndex(Request request) {
            return false;
        }

        @Override
        protected ShardsIterator shards(ClusterState state, InternalRequest request) {
            return state.routingTable()
                    .index(request.concreteIndex())
                    .shard(request.request().getShard().id())
                    .activeInitializingShardsRandomIt();
        }

        @Override
        protected Response newResponse() {
            return new Response();
        }

    }

    private static final Translog.Operation[] EMPTY_OPERATIONS_ARRAY = new Translog.Operation[0];

    static Translog.Operation[] getOperationsBetween(IndexShard indexShard, long fromSeqNo, long size, long byteLimit) throws IOException {
        if (indexShard.state() != IndexShardState.STARTED) {
            throw new IndexShardNotStartedException(indexShard.shardId(), indexShard.state());
        }
        int seenBytes = 0;
        long maxSeqNo = fromSeqNo + size;
        maxSeqNo = Math.min(maxSeqNo, indexShard.getGlobalCheckpoint());
        final List<Translog.Operation> operations = new ArrayList<>();
        try (Translog.Snapshot snapshot = indexShard.newLuceneChangesSnapshot("ccr", fromSeqNo, maxSeqNo, true)) {
            Translog.Operation op;
            while ((op = snapshot.next()) != null) {
                if (op.getSource() == null) {
                    throw new IllegalStateException("source not found for operation: " + op + " fromSeqNo: " + fromSeqNo + " size: " +
                        size);
                }
                operations.add(op);
                seenBytes += op.estimateSize();
                if (seenBytes > byteLimit) {
                    break;
                }
            }
        }
        return operations.toArray(EMPTY_OPERATIONS_ARRAY);
    }
}
