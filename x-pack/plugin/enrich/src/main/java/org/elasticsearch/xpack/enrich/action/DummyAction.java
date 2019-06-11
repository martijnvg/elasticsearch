/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.enrich.action;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
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
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.enrich.EnrichSourceFieldMapper;

import java.io.IOException;
import java.util.Map;

public class DummyAction extends Action<DummyAction.Response> {

    public static final DummyAction INSTANCE = new DummyAction();
    public static final String NAME = "indices:data/read/dummy";

    private DummyAction() {
        super(NAME);
    }

    @Override
    public Writeable.Reader<Response> getResponseReader() {
        return Response::new;
    }

    @Override
    public Response newResponse() {
        throw new UnsupportedOperationException("usage of Streamable is to be replaced by Writeable");
    }

    public static final class Request extends SingleShardRequest<Request> {

        private String lookupField;
        private BytesRef lookupValue;

        Request() {}

        public Request(String index, String lookupField, BytesRef lookupValue) {
            super(index);
            this.lookupField = lookupField;
            this.lookupValue = lookupValue;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }
    }

    public static final class Response extends ActionResponse {

        private Map<String, Object> response;

        public Response(Map<String, Object> response) {
            this.response = response;
        }

        public Response(StreamInput in) throws IOException {
            this(in.readMap());
        }

        public Map<String, Object> getResponse() {
            return response;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeMap(response);
        }
    }

    public static final class TransportAction extends TransportSingleShardAction<Request, Response> {

        private final IndicesService indicesService;

        @Inject
        public TransportAction(ThreadPool threadPool, ClusterService clusterService, TransportService transportService,
                               ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                               IndicesService indicesService) {
            super(NAME, threadPool, clusterService, transportService, actionFilters, indexNameExpressionResolver,
                Request::new, ThreadPool.Names.SAME);
            this.indicesService = indicesService;
        }

        @Override
        protected Response shardOperation(Request request, ShardId shardId) throws IOException {
            IndexService indexService = indicesService.indexServiceSafe(shardId.getIndex());
            IndexShard indexShard = indexService.getShard(0);
            try (Engine.Searcher engineSearcher = indexShard.acquireSearcher("bla")) {
                if (engineSearcher.getDirectoryReader().leaves().size() == 0) {
                    return new Response(Map.of());
                } else if (engineSearcher.getDirectoryReader().leaves().size() != 1) {
                    throw new IllegalStateException("enrich index must have exactly a single segment");
                }

                final LeafReader leafReader = engineSearcher.getDirectoryReader().leaves().get(0).reader();
                final Terms terms = leafReader.terms(request.lookupField);
                if (terms == null) {
                    throw new IllegalStateException("enrich key field does not exist");
                }

                final TermsEnum tenum = terms.iterator();
                if (tenum.seekExact(request.lookupValue)) {
                    PostingsEnum penum = tenum.postings(null, PostingsEnum.NONE);
                    final int docId = penum.nextDoc();
                    assert docId != PostingsEnum.NO_MORE_DOCS : "no matching doc id for [" + request.lookupField + "]";
                    assert penum.nextDoc() == PostingsEnum.NO_MORE_DOCS : "more than one doc id matching for [" + request.lookupField + "]";

                    BinaryDocValues enrichSourceField = leafReader.getBinaryDocValues(EnrichSourceFieldMapper.NAME);
                    assert enrichSourceField != null : "enrich source field is missing";

                    boolean exact = enrichSourceField.advanceExact(docId);
                    assert exact : "doc id [" + docId + "] doesn't have binary doc values";

                    final BytesReference encoded = new BytesArray(enrichSourceField.binaryValue());
                    final Map<String, Object> decoded =
                        XContentHelper.convertToMap(encoded, false, XContentType.SMILE).v2();
                    return new Response(decoded);
                } else {
                    return new Response(Map.of());
                }
            }
        }

        @Override
        protected Writeable.Reader<Response> getResponseReader() {
            return Response::new;
        }

        @Override
        protected boolean resolveIndex(Request request) {
            return true;
        }

        @Override
        protected ShardsIterator shards(ClusterState state, InternalRequest request) {
            return state
                .routingTable()
                .shardRoutingTable(request.concreteIndex(), 0)
                .shardsRandomIt();
        }
    }

}
