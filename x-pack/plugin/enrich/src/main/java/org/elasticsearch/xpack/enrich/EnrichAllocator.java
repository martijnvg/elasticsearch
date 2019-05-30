package org.elasticsearch.xpack.enrich;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.xpack.core.enrich.EnrichPolicy;

import java.util.List;

public class EnrichAllocator implements ClusterStateListener {

    private static final Logger LOGGER = LogManager.getLogger(EnrichAllocator.class);

    private final ClusterService clusterService;

    EnrichAllocator(ClusterService clusterService) {
        this.clusterService = clusterService;
        clusterService.addListener(this);
    }

    ClusterState allocateEnrichIndexShards(ClusterState currentState) {
        boolean changed = false;
        RoutingTable.Builder routingTableBuilder = RoutingTable.builder(currentState.routingTable());

        DiscoveryNode ingestNode = currentState.nodes().getIngestNodes().values().iterator().next().value;

        for (IndexMetaData indexMetaData : currentState.metaData()) {
            if (indexMetaData.getIndex().getName().startsWith(EnrichPolicy.ENRICH_INDEX_NAME_BASE)) {
                IndexRoutingTable.Builder b = IndexRoutingTable.builder(indexMetaData.getIndex());
                List<ShardRouting> shardRoutings = currentState.routingTable().allShards(indexMetaData.getIndex().getName());
                for (ShardRouting shardRouting : shardRoutings) {
                    if (shardRouting.unassigned()) {
                        b.addShard(shardRouting.initialize(ingestNode.getId(), null, -1L));
                        changed = true;
                    }
                }
                if (changed) {
                    routingTableBuilder.add(b);
                }
            }
        }

        if (changed) {
            return ClusterState.builder(currentState)
                .routingTable(routingTableBuilder.build())
                .build();
        } else {
            return currentState;
        }
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        ClusterState state = event.state();
        if (state.nodes().isLocalNodeElectedMaster() == false) {
            return;
        }

        boolean shouldAllocate = false;
        int numIngestNodes = state.nodes().getIngestNodes().size();
        for (IndexMetaData indexMetaData : state.metaData()) {
            if (indexMetaData.getIndex().getName().startsWith(EnrichPolicy.ENRICH_INDEX_NAME_BASE)) {
                long numEnrichIndexShards = state.routingTable().allShards(indexMetaData.getIndex().getName()).stream()
                    .filter(ShardRouting::active)
                    .count();
                if (numIngestNodes != numEnrichIndexShards) {
                    shouldAllocate = true;
                    break;
                }
            }
        }

        if (shouldAllocate) {
            clusterService.submitStateUpdateTask("enrich-allocator", new ClusterStateUpdateTask(Priority.IMMEDIATE) {
                @Override
                public ClusterState execute(ClusterState currentState) throws Exception {
                    return allocateEnrichIndexShards(currentState);
                }

                @Override
                public void onFailure(String source, Exception e) {
                    LOGGER.error("Failed to allocate enrich index shards", e);
                }
            });
        }
    }
}
