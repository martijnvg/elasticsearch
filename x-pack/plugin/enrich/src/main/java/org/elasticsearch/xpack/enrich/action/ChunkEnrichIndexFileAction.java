/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.enrich.action;

import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefIterator;
import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.single.shard.SingleShardRequest;
import org.elasticsearch.action.support.single.shard.TransportSingleShardAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.ShardsIterator;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.ReleasablePagedBytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.ByteArray;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardNotFoundException;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;

public class ChunkEnrichIndexFileAction extends Action<ChunkEnrichIndexFileAction.Response> {

    public static final ChunkEnrichIndexFileAction INSTANCE = new ChunkEnrichIndexFileAction();
    public static final String NAME = "internal:admin/enrich/chunk_enrich_index_file";

    private ChunkEnrichIndexFileAction() {
        super(NAME);
    }

    @Override
    public Response newResponse() {
        throw new UnsupportedOperationException("usage of Streamable is to be replaced by Writeable");
    }

    @Override
    public Writeable.Reader<Response> getResponseReader() {
        return Response::new;
    }

    public static class Request extends SingleShardRequest<Request> {

        private String fileName;
        private long offset;
        private int size;

        public Request(String index, String fileName, long offset, int size) {
            super(index);
            this.fileName = fileName;
            this.offset = offset;
            this.size = size;
        }

        Request() {}

        @Override
        public ActionRequestValidationException validate() {
            return validateNonNullIndex();
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            fileName = in.readString();
            offset = in.readVLong();
            size = in.readVInt();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(fileName);
            out.writeVLong(offset);
            out.writeVInt(size);
        }
    }

    public static class Response extends ActionResponse {

        private final long offset;
        private final BytesReference chunk;

        Response(StreamInput streamInput) throws IOException {
            super(streamInput);
            offset = streamInput.readVLong();
            chunk = streamInput.readBytesReference();
        }

        public Response(long offset, BytesReference chunk) {
            this.offset = offset;
            this.chunk = chunk;
        }

        public long getOffset() {
            return offset;
        }

        public BytesReference getChunk() {
            return chunk;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeVLong(offset);
            out.writeBytesReference(chunk);
        }

    }

    public static class TransportAction extends TransportSingleShardAction<Request, Response> {

        private final BigArrays bigArrays;
        private final IndicesService indicesService;

        @Inject
        public TransportAction(ThreadPool threadPool, ClusterService clusterService, TransportService transportService,
                               ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                               BigArrays bigArrays, IndicesService indicesService) {
            super(NAME, threadPool, clusterService, transportService, actionFilters, indexNameExpressionResolver,
                Request::new, ThreadPool.Names.GENERIC);
            this.bigArrays = bigArrays;
            this.indicesService = indicesService;
        }

        @Override
        protected Response shardOperation(Request request, ShardId shardId) throws IOException {
            throw new AssertionError("should not be invoked");
        }

        @Override
        protected void asyncShardOperation(Request request, ShardId shardId, ActionListener<Response> listener) throws IOException {
            final IndexShard indexShard = indicesService.getShardOrNull(shardId);
            if (indexShard == null) {
                throw new ShardNotFoundException(shardId);
            }

            final ByteArray array = bigArrays.newByteArray(request.size, false);
            // This is currently safe to do because calling `onResponse` will serialize the bytes to the network layer data
            // structure on the same thread. So the bytes will be copied before the reference is released.
            try (ReleasablePagedBytesReference reference = new ReleasablePagedBytesReference(array, request.size, array)) {
                try (IndexInput indexInput = indexShard.store().directory().openInput(request.fileName, IOContext.READ)) {
                    indexInput.seek(request.offset);

                    BytesRefIterator refIterator = reference.iterator();
                    BytesRef ref;
                    while ((ref = refIterator.next()) != null) {
                        indexInput.readBytes(ref.bytes, ref.offset, ref.length);
                    }

                    long offsetAfterRead = indexInput.getFilePointer();
                    long offsetBeforeRead = offsetAfterRead - reference.length();
                    listener.onResponse(new Response(offsetBeforeRead, reference));
                }
            } catch (IOException e) {
                listener.onFailure(e);
            }
        }

        @Override
        protected Writeable.Reader<Response> getResponseReader() {
            return Response::new;
        }

        @Override
        protected boolean resolveIndex(Request request) {
            return false;
        }

        @Override
        protected ShardsIterator shards(ClusterState state, InternalRequest request) {
            return state.routingTable().index(request.concreteIndex()).randomAllActiveShardsIt();
        }
    }

}
