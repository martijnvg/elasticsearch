/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexClusterStateUpdateRequest;
import org.elasticsearch.action.datastreams.ModifyDataStreamsAction;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.DataStreamAction.Type;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.indices.IndicesService;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.function.Function;

/**
 * Handles data stream modification requests.
 */
public class MetadataDataStreamsService {

    static final DateFormatter FORMATTER = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER;

    private final ClusterService clusterService;
    private final IndicesService indicesService;
    private final MetadataCreateIndexService metadataCreateIndexService;

    public MetadataDataStreamsService(
        ClusterService clusterService,
        IndicesService indicesService,
        MetadataCreateIndexService metadataCreateIndexService
    ) {
        this.clusterService = clusterService;
        this.indicesService = indicesService;
        this.metadataCreateIndexService = metadataCreateIndexService;
    }

    public void modifyDataStream(final ModifyDataStreamsAction.Request request, final ActionListener<AcknowledgedResponse> listener) {
        if (request.getActions().size() == 0) {
            listener.onResponse(AcknowledgedResponse.TRUE);
        } else {
            submitUnbatchedTask("update-backing-indices", new AckedClusterStateUpdateTask(Priority.URGENT, request, listener) {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    return modifyDataStream(metadataCreateIndexService, currentState, request.getActions(), indexMetadata -> {
                        try {
                            return indicesService.createIndexMapperServiceForValidation(indexMetadata);
                        } catch (IOException e) {
                            throw new IllegalStateException(e);
                        }
                    });
                }
            });
        }
    }

    @SuppressForbidden(reason = "legacy usage of unbatched task") // TODO add support for batching here
    private void submitUnbatchedTask(@SuppressWarnings("SameParameterValue") String source, ClusterStateUpdateTask task) {
        clusterService.submitUnbatchedStateUpdateTask(source, task);
    }

    /**
     * Computes the resulting cluster state after applying all requested data stream modifications in order.
     *
     * @param metadataCreateIndexService The service that creates backing indices for actions of type {@link Type#CREATE_TIME_SERIES_INDEX}
     * @param clusterState               the cluster state
     * @param actions                    ordered list of modifications to perform
     * @return resulting cluster state after all modifications have been performed
     */
    static ClusterState modifyDataStream(
        MetadataCreateIndexService metadataCreateIndexService,
        ClusterState clusterState,
        Iterable<DataStreamAction> actions,
        Function<IndexMetadata, MapperService> mapperSupplier
    ) {
        Metadata updatedMetadata = clusterState.metadata();
        for (var action : actions) {
            Metadata.Builder builder = Metadata.builder(updatedMetadata);
            if (action.getType() == Type.ADD_BACKING_INDEX) {
                addBackingIndex(updatedMetadata, builder, mapperSupplier, action.getDataStream(), action.getIndex());
            } else if (action.getType() == Type.REMOVE_BACKING_INDEX) {
                removeBackingIndex(updatedMetadata, builder, action.getDataStream(), action.getIndex());
            } else if (action.getType() == Type.CREATE_TIME_SERIES_INDEX) {
                // Skip do in the second loop, because this action updates more than just Metadata
            } else {
                throw new IllegalStateException("unsupported data stream action type [" + action.getClass().getName() + "]");
            }
            updatedMetadata = builder.build();
        }
        clusterState = ClusterState.builder(clusterState).metadata(updatedMetadata).build();
        for (var action : actions) {
            if (action.getType() == Type.CREATE_TIME_SERIES_INDEX) {
                clusterState = createTimeSeriesIndex(metadataCreateIndexService, clusterState, action);
            }
        }
        return clusterState;
    }

    private static void addBackingIndex(
        Metadata metadata,
        Metadata.Builder builder,
        Function<IndexMetadata, MapperService> mapperSupplier,
        String dataStreamName,
        String indexName
    ) {
        var dataStream = validateDataStream(metadata, dataStreamName);
        var index = validateIndex(metadata, indexName);

        try {
            MetadataMigrateToDataStreamService.prepareBackingIndex(
                builder,
                metadata.index(index.getWriteIndex()),
                dataStreamName,
                mapperSupplier,
                false
            );
        } catch (IOException e) {
            throw new IllegalArgumentException("unable to prepare backing index", e);
        }

        // add index to data stream
        builder.put(dataStream.getDataStream().addBackingIndex(metadata, index.getWriteIndex()));
    }

    private static void removeBackingIndex(Metadata metadata, Metadata.Builder builder, String dataStreamName, String indexName) {
        boolean indexNotRemoved = true;
        var dataStream = validateDataStream(metadata, dataStreamName).getDataStream();
        for (Index backingIndex : dataStream.getIndices()) {
            if (backingIndex.getName().equals(indexName)) {
                builder.put(dataStream.removeBackingIndex(backingIndex));
                indexNotRemoved = false;
                break;
            }
        }

        if (indexNotRemoved) {
            throw new IllegalArgumentException("index [" + indexName + "] not found");
        }

        // un-hide index
        var indexMetadata = builder.get(indexName);
        if (indexMetadata != null) {
            builder.put(
                IndexMetadata.builder(indexMetadata)
                    .settings(Settings.builder().put(indexMetadata.getSettings()).put("index.hidden", "false").build())
                    .settingsVersion(indexMetadata.getSettingsVersion() + 1)
            );
        }
    }

    private static ClusterState createTimeSeriesIndex(
        MetadataCreateIndexService metadataCreateIndexService,
        ClusterState current,
        DataStreamAction action
    ) {
        var dataStream = validateDataStream(current.metadata(), action.getDataStream()).getDataStream();
        var templateName = MetadataIndexTemplateService.findV2Template(current.metadata(), action.getDataStream(), false);

        final List<CompressedXContent> mappings;
        try {
            mappings = MetadataCreateIndexService.collectV2Mappings(
                null,
                current,
                templateName,
                metadataCreateIndexService.getxContentRegistry(),
                action.getDataStream()
            );
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        final var templateSettings = MetadataIndexTemplateService.resolveSettings(current.metadata(), templateName);

        final var now = Instant.now();
        Settings.Builder additionalSettings = Settings.builder();
        for (var indexSettingProvider : metadataCreateIndexService.getIndexSettingProviders()) {
            additionalSettings.put(
                indexSettingProvider.getAdditionalIndexSettings(
                    action.getIndex(),
                    dataStream.getName(),
                    true,
                    current.metadata(),
                    now,
                    templateSettings,
                    mappings
                )
            );
        }

        additionalSettings.put(templateSettings);
        var start = FORMATTER.format(action.getTimeSeriesStart());
        var end = FORMATTER.format(action.getTimeSeriesEnd());
        additionalSettings.put(IndexSettings.TIME_SERIES_START_TIME.getKey(), start);
        additionalSettings.put(IndexSettings.TIME_SERIES_END_TIME.getKey(), end);
        additionalSettings.put(IndexMetadata.SETTING_INDEX_HIDDEN, true);

        var request = new CreateIndexClusterStateUpdateRequest("create_tsdb_index", action.getIndex(), action.getIndex());
        request.settings(additionalSettings.build());
        request.dataStreamName(action.getDataStream());
        try {
            current = metadataCreateIndexService.applyCreateIndexRequest(current, request, true, ActionListener.noop());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        var updateDataStream = dataStream.addBackingIndex(current.metadata(), current.metadata().index(action.getIndex()).getIndex());
        var mdBuilder = Metadata.builder(current.metadata());
        mdBuilder.put(updateDataStream);
        return ClusterState.builder(current).metadata(mdBuilder).build();
    }

    private static IndexAbstraction.DataStream validateDataStream(Metadata metadata, String dataStreamName) {
        IndexAbstraction dataStream = metadata.getIndicesLookup().get(dataStreamName);
        if (dataStream == null || dataStream.getType() != IndexAbstraction.Type.DATA_STREAM) {
            throw new IllegalArgumentException("data stream [" + dataStreamName + "] not found");
        }
        return (IndexAbstraction.DataStream) dataStream;
    }

    private static IndexAbstraction validateIndex(Metadata metadata, String indexName) {
        IndexAbstraction index = metadata.getIndicesLookup().get(indexName);
        if (index == null || index.getType() != IndexAbstraction.Type.CONCRETE_INDEX) {
            throw new IllegalArgumentException("index [" + indexName + "] not found");
        }
        return index;
    }

}
