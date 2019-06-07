/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.enrich.action;

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
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardNotFoundException;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;

public class ListEnrichIndexFilesAction extends Action<ListEnrichIndexFilesAction.Response> {

    public static final ListEnrichIndexFilesAction INSTANCE = new ListEnrichIndexFilesAction();
    public static final String NAME = "internal:admin/enrich/list_enrich_index_files";

    private ListEnrichIndexFilesAction() {
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

        public Request(String enrichIndex) {
            super(enrichIndex);
        }

        Request() {
        }

        @Override
        public ActionRequestValidationException validate() {
            return validateNonNullIndex();
        }
    }

    public static class Response extends ActionResponse {

        private final Store.MetadataSnapshot snapshot;

        Response(StreamInput in) throws IOException {
            this(new Store.MetadataSnapshot(in));
        }

        Response(Store.MetadataSnapshot snapshot) {
            this.snapshot = snapshot;
        }

        public Store.MetadataSnapshot getSnapshot() {
            return snapshot;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            snapshot.writeTo(out);
        }
    }

    public static class TransportAction extends TransportSingleShardAction<Request, Response> {

        private final IndicesService indicesService;

        @Inject
        public TransportAction(ThreadPool threadPool, ClusterService clusterService,
                               TransportService transportService, ActionFilters actionFilters,
                               IndexNameExpressionResolver indexNameExpressionResolver, IndicesService indicesService) {
            super(NAME, threadPool, clusterService, transportService, actionFilters,
                indexNameExpressionResolver, Request::new, ThreadPool.Names.GENERIC);
            this.indicesService = indicesService;
        }

        @Override
        protected Response shardOperation(Request request, ShardId shardId) throws IOException {
            IndexShard indexShard = indicesService.getShardOrNull(shardId);
            if (indexShard == null) {
                throw new ShardNotFoundException(shardId);
            }
            Store.MetadataSnapshot snapshot =
                indexShard.store().getMetadata(indexShard.acquireSafeIndexCommit().getIndexCommit());
            return new Response(snapshot);
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
