/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.action.admin.indices.rollover;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.time.Instant;
import java.util.EnumSet;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_CREATION_DATE;

public class TransportRolloverIndexAction extends TransportMasterNodeAction<RolloverIndexAction.Request, AcknowledgedResponse> {

    private final AllocationService allocationService;

    @Inject
    public TransportRolloverIndexAction(TransportService transportService, ClusterService clusterService, ThreadPool threadPool,
                                        ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                                        AllocationService allocationService) {
        super(RolloverIndexAction.NAME, transportService, clusterService, threadPool, actionFilters, RolloverIndexAction.Request::new,
            indexNameExpressionResolver);
        this.allocationService = allocationService;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected AcknowledgedResponse read(StreamInput in) throws IOException {
        return new AcknowledgedResponse(in);
    }

    final AtomicInteger counter = new AtomicInteger();

    @Override
    protected void masterOperation(Task task, RolloverIndexAction.Request request, ClusterState state,
                                   ActionListener<AcknowledgedResponse> listener) throws Exception {
        clusterService.submitStateUpdateTask("rollover_index", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                String oldIndexName = request.getIndex();
                String newIndexName = String.format(Locale.ROOT, "%s-%d", request.getIndex(), counter.getAndIncrement());

                IndexMetaData current = currentState.metaData().index(oldIndexName);

                // UPDATING METADATA:
                MetaData.Builder mdBuilder = MetaData.builder(state.metaData());
                {
                    // Create new IndexMetaData for old index name:
                    IndexMetaData.Builder newIndex = new IndexMetaData.Builder(oldIndexName);
                    newIndex.putMapping(current.mapping());
                    newIndex.settings(Settings.builder()
                        .put(current.getSettings())
                        .put(IndexMetaData.SETTING_INDEX_PROVIDED_NAME, oldIndexName)
                        .put(SETTING_CREATION_DATE, Instant.now().toEpochMilli())
                        .put(IndexMetaData.SETTING_INDEX_UUID, UUIDs.randomBase64UUID()).build());
                    for (ObjectCursor<AliasMetaData> cursor : current.getAliases().values()) {
                        newIndex.putAlias(cursor.value);
                    }
                    mdBuilder.put(newIndex);
                }
                {
                    // Reuse existing IndexMetaData for new index name:
                    IndexMetaData.Builder rolledOverIndex = new IndexMetaData.Builder(current);
                    rolledOverIndex.index(newIndexName);
                    rolledOverIndex.settings(Settings.builder()
                        .put(current.getSettings())
                        .put(IndexMetaData.SETTING_INDEX_UUID, current.getIndexUUID()).build());
                    mdBuilder.put(rolledOverIndex);
                }
                ClusterState updatedState = ClusterState.builder(state).metaData(mdBuilder).build();

                // UPDATING ROUTING TABLE:
                RoutingTable.Builder routingTableBuilder = RoutingTable.builder(updatedState.routingTable());
                {
                    // Changing old IndexRoutingTable and ShardRouting to use new index name
                    IndexRoutingTable rolledOverIndexRoutingTable = updatedState.routingTable().index(oldIndexName);
                    IndexRoutingTable.Builder builder =
                        IndexRoutingTable.builder(updatedState.metaData().index(newIndexName).getIndex());
                    for (IndexShardRoutingTable shardRoutings : rolledOverIndexRoutingTable) {
                        ShardId newShardId =
                            new ShardId(newIndexName, shardRoutings.shardId().getIndex().getUUID(), shardRoutings.shardId().getId());
                        IndexShardRoutingTable.Builder indexShardBuilder = new IndexShardRoutingTable.Builder(newShardId);
                        for (ShardRouting shardRouting : shardRoutings) {
                            indexShardBuilder.addShard(shardRouting.renameIndex(newIndexName));
                        }
                        builder.addIndexShard(indexShardBuilder.build());
                    }
                    routingTableBuilder.add(builder);
                }
                {
                    // Changing old IndexRoutingTable and ShardRouting for the old name:
                    routingTableBuilder.addAsNew(updatedState.metaData().index(oldIndexName));
                }
                updatedState = allocationService.reroute(
                    ClusterState.builder(updatedState).routingTable(routingTableBuilder.build()).build(),
                    "new index [" + oldIndexName + "] created");

                return updatedState;
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                listener.onResponse(new AcknowledgedResponse(true));
            }

            @Override
            public void onFailure(String source, Exception e) {
                listener.onFailure(e);
            }
        });
    }

    @Override
    protected ClusterBlockException checkBlock(RolloverIndexAction.Request request, ClusterState state) {
        IndicesOptions indicesOptions = new IndicesOptions(EnumSet.of(
            IndicesOptions.Option.IGNORE_UNAVAILABLE,
            IndicesOptions.Option.ALLOW_NO_INDICES,
            IndicesOptions.Option.FORBID_ALIASES_TO_MULTIPLE_INDICES,
            IndicesOptions.Option.FORBID_CLOSED_INDICES),
            EnumSet.noneOf(IndicesOptions.WildcardStates.class));
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_WRITE,
            indexNameExpressionResolver.concreteIndexNames(state, indicesOptions, request.getIndex()));
    }
}
