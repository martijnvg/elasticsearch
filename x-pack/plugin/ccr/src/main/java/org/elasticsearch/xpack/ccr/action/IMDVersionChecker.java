/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ccr.action;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.Index;

import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

final class IMDVersionChecker implements BiConsumer<Long, Consumer<Exception>> {

    private static final Logger LOGGER = Loggers.getLogger(IMDVersionChecker.class);

    private final Client followClient;
    private final Client leaderClient;
    private final Index leaderIndex;
    private final Index followIndex;
    private final AtomicLong currentIndexMetadataVersion;
    private final Semaphore updateMappingSemaphore = new Semaphore(1);

    IMDVersionChecker(Index leaderIndex, Index followIndex, Client followClient, Client leaderClient) {
        this.followClient = followClient;
        this.leaderIndex = leaderIndex;
        this.followIndex = followIndex;
        this.leaderClient = leaderClient;
        this.currentIndexMetadataVersion = new AtomicLong();
    }

    public void accept(Long minimumRequiredIndexMetadataVersion, Consumer<Exception> handler) {
        if (currentIndexMetadataVersion.get() >= minimumRequiredIndexMetadataVersion) {
            LOGGER.debug("Current index metadata version [{}] is higher or equal than minimum required index metadata version [{}]",
                currentIndexMetadataVersion.get(), minimumRequiredIndexMetadataVersion);
            handler.accept(null);
        } else {
            updateMapping(minimumRequiredIndexMetadataVersion, handler);
        }
    }

    void updateMapping(long minimumRequiredIndexMetadataVersion, Consumer<Exception> handler) {
        try {
            updateMappingSemaphore.acquire();
        } catch (InterruptedException e) {
            handler.accept(e);
            return;
        }
        if (currentIndexMetadataVersion.get() >= minimumRequiredIndexMetadataVersion) {
            updateMappingSemaphore.release();
            LOGGER.debug("Current index metadata version [{}] is higher or equal than minimum required index metadata version [{}]",
                currentIndexMetadataVersion.get(), minimumRequiredIndexMetadataVersion);
            handler.accept(null);
            return;
        }

        ClusterStateRequest clusterStateRequest = new ClusterStateRequest();
        clusterStateRequest.clear();
        clusterStateRequest.metaData(true);
        clusterStateRequest.indices(leaderIndex.getName());

        leaderClient.admin().cluster().state(clusterStateRequest, ActionListener.wrap(clusterStateResponse -> {
            IndexMetaData indexMetaData = clusterStateResponse.getState().metaData().getIndexSafe(leaderIndex);
            assert indexMetaData.getMappings().size() == 1;
            MappingMetaData mappingMetaData = indexMetaData.getMappings().iterator().next().value;

            PutMappingRequest putMappingRequest = new PutMappingRequest(followIndex.getName());
            putMappingRequest.type(mappingMetaData.type());
            putMappingRequest.source(mappingMetaData.source().string(), XContentType.JSON);
            followClient.admin().indices().putMapping(putMappingRequest, ActionListener.wrap(putMappingResponse -> {
                currentIndexMetadataVersion.set(indexMetaData.getVersion());
                updateMappingSemaphore.release();
                handler.accept(null);
            }, e -> {
                updateMappingSemaphore.release();
                handler.accept(e);
            }));
        }, e -> {
            updateMappingSemaphore.release();
            handler.accept(e);
        }));
    }
}
