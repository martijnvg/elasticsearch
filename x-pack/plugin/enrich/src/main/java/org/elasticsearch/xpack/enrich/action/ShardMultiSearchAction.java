/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.enrich.action;

import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopDocs;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.ValidateActions;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.single.shard.SingleShardRequest;
import org.elasticsearch.action.support.single.shard.TransportSingleShardAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.ShardsIterator;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.fieldvisitor.FieldsVisitor;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.enrich.EnrichPolicy;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

public class ShardMultiSearchAction extends ActionType<MultiSearchResponse> {

    public static final ShardMultiSearchAction INSTANCE = new ShardMultiSearchAction();
    private static final String NAME = "indices:data/read/shard_msearch";

    private ShardMultiSearchAction() {
        super(NAME, MultiSearchResponse::new);
    }

    public static class Request extends SingleShardRequest<Request> {

        private final MultiSearchRequest multiSearchRequest;

        public Request(MultiSearchRequest multiSearchRequest) {
            super(multiSearchRequest.requests().get(0).indices()[0]);
            this.multiSearchRequest = multiSearchRequest;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            multiSearchRequest = new MultiSearchRequest();
            multiSearchRequest.readFrom(in);
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = validateNonNullIndex();
            if (index.startsWith(EnrichPolicy.ENRICH_INDEX_NAME_BASE) == false) {
                validationException = ValidateActions.addValidationError("index [" + index + "] is not an enrich index",
                    validationException);
            }
            return validationException;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            multiSearchRequest.writeTo(out);
        }
    }

    public static class TransportAction extends TransportSingleShardAction<Request, MultiSearchResponse> {

        private final IndicesService indicesService;

        @Inject
        public TransportAction(ThreadPool threadPool, ClusterService clusterService, TransportService transportService,
                               ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                               IndicesService indicesService) {
            super(NAME, threadPool, clusterService, transportService, actionFilters, indexNameExpressionResolver,
                Request::new, ThreadPool.Names.SEARCH);
            this.indicesService = indicesService;
        }

        @Override
        protected Writeable.Reader<MultiSearchResponse> getResponseReader() {
            return MultiSearchResponse::new;
        }

        @Override
        protected boolean resolveIndex(Request request) {
            return true;
        }

        @Override
        protected ShardsIterator shards(ClusterState state, InternalRequest request) {
            String index = request.concreteIndex();
            IndexRoutingTable indexRouting = state.routingTable().index(index);
            int numShards = indexRouting.shards().size();
            if (numShards != 1) {
                throw new IllegalStateException("index [" + index + "] should have 1 shard, but has " + numShards + " shards");
            }
            return indexRouting.shard(0).shardsRandomIt();
        }

        @Override
        protected MultiSearchResponse shardOperation(Request request, ShardId shardId) throws IOException {
            final long nowInMillis = System.currentTimeMillis();
            final IndexService indexService = indicesService.indexService(shardId.getIndex());
            final IndexShard indexShard = indicesService.getShardOrNull(shardId);
            try (Engine.Searcher searcher = indexShard.acquireSearcher("enrich_msearch")) {
                final FieldsVisitor visitor = new FieldsVisitor(true);
                final QueryShardContext context =
                    indexService.newQueryShardContext(shardId.id(), searcher.reader(), () -> nowInMillis, null);
                final MapperService mapperService = context.getMapperService();
                final Text typeText = mapperService.documentMapper().typeText();

                final MultiSearchResponse.Item[] items = new MultiSearchResponse.Item[request.multiSearchRequest.requests().size()];
                for (int i = 0; i < request.multiSearchRequest.requests().size(); i++) {
                    final SearchRequest searchRequest = request.multiSearchRequest.requests().get(i);

                    final Query luceneQuery = searchRequest.source().query().rewrite(context).toQuery(context);
                    final int from = searchRequest.source().from() == -1 ? 0 : searchRequest.source().from();
                    final int size = searchRequest.source().size() == -1 ? 10 : searchRequest.source().size();
                    final int n = from + size;
                    final TopDocs topDocs = searcher.searcher().search(luceneQuery, n, new Sort(SortField.FIELD_DOC));

                    final SearchHit[] hits = new SearchHit[topDocs.scoreDocs.length];
                    for (int j = 0; j < topDocs.scoreDocs.length; j++) {
                        final ScoreDoc scoreDoc = topDocs.scoreDocs[j];

                        visitor.reset();
                        searcher.searcher().doc(scoreDoc.doc, visitor);
                        visitor.postProcess(mapperService);
                        final SearchHit hit = new SearchHit(scoreDoc.doc, visitor.uid().id(), typeText, Map.of());
                        hit.sourceRef(filterSource(searchRequest, visitor.source()));
                        hits[j] = hit;
                    }
                    items[i] = new MultiSearchResponse.Item(createSearchResponse(topDocs, hits), null);
                }
                return new MultiSearchResponse(items, 1L);
            }
        }

    }

    private static BytesReference filterSource(SearchRequest searchRequest, BytesReference source) throws IOException {
        Set<String> includes = Set.of(searchRequest.source().fetchSource().includes());
        Set<String> excludes = Set.of(searchRequest.source().fetchSource().excludes());

        XContentBuilder builder =
            new XContentBuilder(XContentType.SMILE.xContent(), new BytesStreamOutput(source.length()), includes, excludes);
        XContentParser sourceParser = XContentHelper.createParser(NamedXContentRegistry.EMPTY,
            DeprecationHandler.THROW_UNSUPPORTED_OPERATION, source, XContentType.SMILE);
        builder.copyCurrentStructure(sourceParser);
        return BytesReference.bytes(builder);
    }

    private static SearchResponse createSearchResponse(TopDocs topDocs, SearchHit[] hits) {
        SearchHits searchHits = new SearchHits(hits, topDocs.totalHits, 0);
        return new SearchResponse(
            new InternalSearchResponse(searchHits, null, null, null, false, null, 0),
            null, 1, 1, 0, 1L, ShardSearchFailure.EMPTY_ARRAY, SearchResponse.Clusters.EMPTY
        );
    }
}
