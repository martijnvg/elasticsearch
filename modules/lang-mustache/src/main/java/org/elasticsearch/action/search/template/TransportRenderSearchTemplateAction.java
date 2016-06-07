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

package org.elasticsearch.action.search.template;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

public class TransportRenderSearchTemplateAction
        extends AbstractSearchTemplateAction<RenderSearchTemplateRequest, RenderSearchTemplateResponse> {

    @Inject
    public TransportRenderSearchTemplateAction(Settings settings, ThreadPool threadPool, TransportService transportService,
                                               ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                                               ClusterService clusterService, ScriptService scriptService) {
        super(settings, RenderSearchTemplateAction.NAME, threadPool, transportService, actionFilters, indexNameExpressionResolver,
                RenderSearchTemplateRequest::new, clusterService, scriptService);
    }

    @Override
    protected void doExecute(RenderSearchTemplateRequest r, ActionListener<RenderSearchTemplateResponse> listener, BytesReference source) {
        listener.onResponse(new RenderSearchTemplateResponse(source));
    }
}
