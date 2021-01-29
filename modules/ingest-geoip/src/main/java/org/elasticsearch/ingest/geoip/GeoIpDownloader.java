/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.ingest.geoip;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.OriginSettingClient;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractAsyncTask;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.persistent.AllocatedPersistentTask;
import org.elasticsearch.persistent.PersistentTaskParams;
import org.elasticsearch.persistent.PersistentTaskState;
import org.elasticsearch.persistent.PersistentTasksExecutor;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

class GeoIpDownloader extends PersistentTasksExecutor<PersistentTaskParams> implements ClusterStateListener {

    public static final Setting<TimeValue> POLL_INTERVAL_SETTING = Setting.timeSetting("geoip.downloader.poll.interval",
        TimeValue.timeValueDays(3), TimeValue.timeValueDays(1), Setting.Property.Dynamic, Setting.Property.NodeScope);
    public static final Setting<String> ENDPOINT_SETTING = Setting.simpleString("geoip.downloader.endpoint",
        "https://paisano.elastic.dev/v1/geoip/database", Setting.Property.Dynamic, Setting.Property.NodeScope);
    public static final String GEOIP_DOWNLOADER = "geoip-downloader";

    static final String DATABASES_INDEX = ".geoip_databases";

    private final Client client;
    private final HttpClient httpClient;
    private final ClusterService clusterService;
    private final ThreadPool threadPool;
    private final Logger logger = LogManager.getLogger(GeoIpDownloader.class);
    private final PersistentTasksService tasksService;
    private volatile AbstractAsyncTask task;
    private volatile TimeValue pollInterval;
    private volatile String endpoint;

    GeoIpDownloader(Client client, HttpClient httpClient, ClusterService clusterService, ThreadPool threadPool, Settings settings) {
        super(GEOIP_DOWNLOADER, ThreadPool.Names.GENERIC);
        this.client = new OriginSettingClient(client, "geoip");
        this.httpClient = httpClient;
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        tasksService = new PersistentTasksService(clusterService, threadPool, client);
        endpoint = ENDPOINT_SETTING.get(settings);
        pollInterval = POLL_INTERVAL_SETTING.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(POLL_INTERVAL_SETTING, this::setPollInterval);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(ENDPOINT_SETTING, this::setEndpoint);
        clusterService.addListener(this);
    }

    public void setPollInterval(TimeValue pollInterval) {
        this.pollInterval = pollInterval;
        if (task != null) {
            task.setInterval(new TimeValue(1));
        }
    }

    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }

    void updateDatabases(Runnable onSuccess) throws IOException {
        logger.info("updating geoip databases");
        String data = httpClient.getString(endpoint + "?key=11111111-1111-1111-1111-111111111111");
        List<Map<String, Object>> response = XContentHelper.convertToList(XContentFactory.xContent(XContentType.JSON), data, false);
        client.prepareSearch(DATABASES_INDEX)
            .setIndicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN_HIDDEN)
            .execute(ActionListener.wrap(searchResponse -> {
                    Map<String, Object> current = Arrays.stream(searchResponse.getHits().getHits()).collect(
                        Collectors.toMap(SearchHit::getId, h -> h.getSourceAsMap().get("md5_hash")));
                    BulkRequestBuilder bulkRequestBuilder =
                        client.prepareBulk(DATABASES_INDEX).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
                    for (Map<String, Object> map : response) {
                        String url = (String) map.remove("url");
                        String name = map.get("name").toString().replace(".gz", "");
                        if (Objects.equals(map.get("md5_hash"), current.get(name))) {
                            continue;
                        }
                        map.put("data", httpClient.getBytes(url));
                        map.put("name", name);
                        bulkRequestBuilder.add(new IndexRequest().id((String) map.get("name")).source(map));
                    }
                    if (bulkRequestBuilder.numberOfActions() > 0) {
                        bulkRequestBuilder.execute(ActionListener.wrap(bulkItemResponses -> {
                            if (bulkItemResponses.hasFailures()) {
                                logger.error("could not update geoip databases [" + bulkItemResponses.buildFailureMessage() + "]");
                                return;
                            }
                            logger.info("successfully updated geoip databases");
                            onSuccess.run();
                        }, e -> logger.error("could not update geoip databases", e)));
                    } else {
                        onSuccess.run();
                    }
                },
                e -> logger.error("could not update geoip databases", e)));
    }

    @Override
    protected void nodeOperation(AllocatedPersistentTask allocatedTask, PersistentTaskParams params, PersistentTaskState state) {
        long lastExecution = state == null ? 0 : ((GeoIpDownloaderTaskState) state).getLastExecutionTime();
        long now = System.currentTimeMillis();
        long nextRun = lastExecution + pollInterval.millis();
        long firstInterval = nextRun <= now ? 1 : nextRun - now;
        task = new AbstractAsyncTask(logger, threadPool, new TimeValue(firstInterval), false) {
            @Override
            protected boolean mustReschedule() {
                return true;
            }

            @Override
            protected void runInternal() {
                task.setInterval(pollInterval);
                try {
                    updateDatabases(() -> allocatedTask.updatePersistentTaskState(new GeoIpDownloaderTaskState(System.currentTimeMillis()),
                        ActionListener.wrap(r -> logger.info("updated geoip task state"),
                            e -> logger.error("failed to update geoip downloader task state", e))));
                } catch (Exception e) {
                    logger.error("exception during geoip databases update", e);
                }
            }
        };
        task.rescheduleIfNecessary();
    }


    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.localNodeMaster()) {
            clusterService.removeListener(this);
            tasksService.sendStartRequest(GEOIP_DOWNLOADER, GEOIP_DOWNLOADER, new GeoIpTaskParams(), ActionListener.wrap(r -> {
            }, e -> {
                if (e instanceof ResourceAlreadyExistsException == false) {
                    logger.error("failed to create geoip downloader task", e);
                    clusterService.addListener(this);
                }
            }));
        }
    }
}
