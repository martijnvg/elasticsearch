/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.datastreams;

import org.apache.lucene.index.FilterMergePolicy;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.plugins.EnginePlugin;
import org.elasticsearch.xpack.core.action.CreateDataStreamAction;
import org.elasticsearch.xpack.core.action.DataStreamsStatsAction;
import org.elasticsearch.xpack.core.action.DeleteDataStreamAction;
import org.elasticsearch.xpack.core.action.GetDataStreamAction;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.index.mapper.MetadataFieldMapper;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.xpack.datastreams.action.DataStreamsStatsTransportAction;
import org.elasticsearch.xpack.datastreams.rest.RestCreateDataStreamAction;
import org.elasticsearch.xpack.datastreams.rest.RestDataStreamsStatsAction;
import org.elasticsearch.xpack.datastreams.rest.RestDeleteDataStreamAction;
import org.elasticsearch.xpack.datastreams.rest.RestGetDataStreamsAction;
import org.elasticsearch.xpack.core.action.XPackInfoFeatureAction;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureAction;
import org.elasticsearch.xpack.datastreams.action.CreateDataStreamTransportAction;
import org.elasticsearch.xpack.datastreams.action.DataStreamInfoTransportAction;
import org.elasticsearch.xpack.datastreams.action.DataStreamUsageTransportAction;
import org.elasticsearch.xpack.datastreams.action.DeleteDataStreamTransportAction;
import org.elasticsearch.xpack.datastreams.action.GetDataStreamsTransportAction;
import org.elasticsearch.xpack.datastreams.mapper.DataStreamTimestampFieldMapper;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Supplier;

public class DataStreamsPlugin extends Plugin implements ActionPlugin, MapperPlugin, EnginePlugin {

    @Override
    public Map<String, MetadataFieldMapper.TypeParser> getMetadataMappers() {
        return Map.of(DataStreamTimestampFieldMapper.NAME, DataStreamTimestampFieldMapper.PARSER);
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        var createDsAction = new ActionHandler<>(CreateDataStreamAction.INSTANCE, CreateDataStreamTransportAction.class);
        var deleteDsInfoAction = new ActionHandler<>(DeleteDataStreamAction.INSTANCE, DeleteDataStreamTransportAction.class);
        var getDsAction = new ActionHandler<>(GetDataStreamAction.INSTANCE, GetDataStreamsTransportAction.class);
        var dsStatsAction = new ActionHandler<>(DataStreamsStatsAction.INSTANCE, DataStreamsStatsTransportAction.class);
        var dsUsageAction = new ActionHandler<>(XPackUsageFeatureAction.DATA_STREAMS, DataStreamUsageTransportAction.class);
        var dsInfoAction = new ActionHandler<>(XPackInfoFeatureAction.DATA_STREAMS, DataStreamInfoTransportAction.class);
        return List.of(createDsAction, deleteDsInfoAction, getDsAction, dsStatsAction, dsUsageAction, dsInfoAction);
    }

    @Override
    public List<RestHandler> getRestHandlers(
        Settings settings,
        RestController restController,
        ClusterSettings clusterSettings,
        IndexScopedSettings indexScopedSettings,
        SettingsFilter settingsFilter,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<DiscoveryNodes> nodesInCluster
    ) {
        var createDsAction = new RestCreateDataStreamAction();
        var deleteDsAction = new RestDeleteDataStreamAction();
        var getDsAction = new RestGetDataStreamsAction();
        var dsStatsAction = new RestDataStreamsStatsAction();
        return List.of(createDsAction, deleteDsAction, getDsAction, dsStatsAction);
    }

    @Override
    public Optional<EngineFactory> getEngineFactory(IndexSettings indexSettings) {
        return Optional.empty();
    }

    @Override
    public BiFunction<FilterMergePolicy, Engine, FilterMergePolicy> getMergePolicyDecorator() {
        return (in, engine) -> {
            // Check differently whether an index is a backing index:
            if (engine.config().getShardId().getIndex().getName().startsWith(DataStream.BACKING_INDEX_PREFIX)) {
                return new PruneFieldsMergePolicy(in, engine::getMinRetainedSeqNo);
            } else {
                return in;
            }
        };
    }
}
