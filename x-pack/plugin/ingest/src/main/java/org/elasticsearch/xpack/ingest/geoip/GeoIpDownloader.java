/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ingest.geoip;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.OriginSettingClient;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.license.LicenseService;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.scheduler.SchedulerEngine;

import java.io.IOException;
import java.time.Clock;
import java.util.List;
import java.util.Map;

public class GeoIpDownloader implements ClusterStateListener, SchedulerEngine.Schedule, SchedulerEngine.Listener {

    public static final Setting<TimeValue> POLL_INTERVAL_SETTING = Setting.timeSetting("geoip.downloader.poll.interval",
        TimeValue.timeValueDays(3), TimeValue.timeValueDays(1), Setting.Property.Dynamic, Setting.Property.NodeScope);
    public static final Setting<String> ENDPOINT_SETTING = Setting.simpleString("geoip.downloader.endpoint",
        "https://paisano.elastic.dev/v2/geoip/database", Setting.Property.Dynamic, Setting.Property.NodeScope);

    static final String JOB_NAME = "geoip-download";
    private static final Logger logger = LogManager.getLogger(GeoIpDownloader.class);

    private volatile SchedulerEngine schedulerEngine;
    private volatile boolean scheduleImmediately;

    private final Settings settings;
    private final ClusterService clusterService;
    private final HttpClient httpClient;
    private final Client client;
    private final Clock clock;
    private TimeValue pollInterval;
    private String endpoint;

    public GeoIpDownloader(Settings settings, Client client, HttpClient httpClient, ClusterService clusterService, Clock clock) {
        this.clusterService = clusterService;
        this.httpClient = httpClient;
        this.client = new OriginSettingClient(client, ClientHelper.GEOIP_ORIGIN);
        this.clock = clock;
        this.settings = settings;

        pollInterval = POLL_INTERVAL_SETTING.get(settings);
        endpoint = ENDPOINT_SETTING.get(settings);

        clusterService.getClusterSettings().addSettingsUpdateConsumer(POLL_INTERVAL_SETTING, this::setPollInterval);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(ENDPOINT_SETTING, this::setEndpoint);

        clusterService.addListener(this);
    }

    void updateDatabases() throws IOException {
        logger.info("updating geoip databases");
        String data = httpClient.getString(endpoint + "?key=" + getLicenseUid());
        List<Map<String, Object>> response = XContentHelper.convertToList(XContentFactory.xContent(XContentType.JSON), data, false);
        for (Map<String, Object> map : response) {
            String url = (String) map.remove("url");
            map.put("data", httpClient.getBytes(url));
        }
        BulkRequestBuilder bulkRequestBuilder = client.prepareBulk(".geoip_databases");
        for (Map<String, Object> stringObjectMap : response) {
            bulkRequestBuilder.add(new IndexRequest().id((String) stringObjectMap.get("name")).source(stringObjectMap));
        }
        bulkRequestBuilder.execute(new ActionListener<>() {
            @Override
            public void onResponse(BulkResponse bulkItemResponses) {
                if (bulkItemResponses.hasFailures()) {
                    logger.error("could not update geoip databases [" + bulkItemResponses.buildFailureMessage() + "]");
                    return;
                }
                clusterService.submitStateUpdateTask(JOB_NAME, new ClusterStateUpdateTask(Priority.LOW) {
                    @Override
                    public ClusterState execute(ClusterState currentState) {
                        return ClusterState.builder(currentState)
                            .metadata(Metadata.builder(currentState.metadata())
                                .putCustom(GeoIpMetadata.TYPE, new GeoIpMetadata(clock.millis())))
                            .build();
                    }

                    @Override
                    public void onFailure(String source, Exception e) {
                        logger.error("could not update geoip update timestamp", e);
                    }
                });
            }

            @Override
            public void onFailure(Exception e) {
                logger.error("could not update GeoIp databases", e);
            }
        });


    }

    protected String getLicenseUid() {
        return LicenseService.getLicense(clusterService.state().metadata()).uid();
    }

    void setPollInterval(TimeValue pollInterval) {
        this.pollInterval = pollInterval;
        scheduleJob(true);
    }

    void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.localNodeMaster() && LicenseService.getLicense(event.state().metadata()) != null) {
            if (schedulerEngine == null) {
                schedulerEngine = new SchedulerEngine(settings, clock);
                schedulerEngine.register(this);
                scheduleJob(false);
            }
        } else if (schedulerEngine != null) {
            SchedulerEngine se = schedulerEngine;
            schedulerEngine = null;
            se.stop();
        }
    }

    private void scheduleJob(boolean forceStart) {
        SchedulerEngine se = schedulerEngine;
        if (se != null && (forceStart || !se.scheduledJobIds().contains(JOB_NAME))) {
            scheduleImmediately = true;
            se.add(new SchedulerEngine.Job(JOB_NAME, this));
        }
    }

    @Override
    public long nextScheduledTimeAfter(long startTime, long now) {
        if (scheduleImmediately) {
            scheduleImmediately = false;
            return now;
        }
        long interval = pollInterval.millis();
        long delta = now - startTime;
        return startTime + (delta / interval + 1) * interval;
    }

    @Override
    public void triggered(SchedulerEngine.Event event) {
        if (JOB_NAME.equals(event.getJobName())) {
            try {
                updateDatabases();
            } catch (Exception e) {
                logger.error("error during geoip database update", e);
            }
        }
    }
}
