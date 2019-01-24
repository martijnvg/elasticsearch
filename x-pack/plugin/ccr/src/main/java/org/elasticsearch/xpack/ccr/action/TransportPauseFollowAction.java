/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ccr.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ccr.action.PauseFollowAction;

import java.util.List;
import java.util.stream.Collectors;

public class TransportPauseFollowAction extends TransportMasterNodeAction<PauseFollowAction.Request, AcknowledgedResponse> {

    private final Client client;

    @Inject
    public TransportPauseFollowAction(
            final TransportService transportService,
            final ActionFilters actionFilters,
            final ClusterService clusterService,
            final ThreadPool threadPool,
            final IndexNameExpressionResolver indexNameExpressionResolver,
            final Client client) {
        super(PauseFollowAction.NAME, transportService, clusterService, threadPool, actionFilters,
            PauseFollowAction.Request::new, indexNameExpressionResolver);
        this.client = client;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected AcknowledgedResponse newResponse() {
        return new AcknowledgedResponse();
    }

    @Override
    protected void masterOperation(PauseFollowAction.Request request,
                                   ClusterState state,
                                   ActionListener<AcknowledgedResponse> listener) throws Exception {
        client.execute(PauseShardFollowTasksAction.INSTANCE, new PauseShardFollowTasksAction.Request(request.getFollowIndex()),
            ActionListener.wrap(response -> listener.onResponse(new AcknowledgedResponse(true)), listener::onFailure));
    }

    @Override
    protected ClusterBlockException checkBlock(PauseFollowAction.Request request, ClusterState state) {
        return state.blocks().indexBlockedException(ClusterBlockLevel.METADATA_WRITE, request.getFollowIndex());
    }

}
