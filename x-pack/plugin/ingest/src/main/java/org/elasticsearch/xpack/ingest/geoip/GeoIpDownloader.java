/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ingest.geoip;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.license.LicenseService;
import org.elasticsearch.xpack.core.scheduler.SchedulerEngine;

import java.io.IOException;
import java.time.Clock;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class GeoIpDownloader implements ClusterStateListener {

    public static final Setting<TimeValue> POLL_INTERVAL_SETTING = Setting.timeSetting("geoip.downloader.poll.interval",
        TimeValue.timeValueDays(3), Setting.Property.Dynamic);
    public static final Setting<String> ENDPOINT_SETTING = Setting.simpleString("geoip.downloader.endpoint",
        "https://paisano.elastic.dev/v1/geoip/database", Setting.Property.Dynamic);

    private static final Logger logger = LogManager.getLogger(GeoIpDownloader.class);

    private final SetOnce<SchedulerEngine> schedulerEngine = new SetOnce<>();
    private final LicenseService licenseService;
    private final ClusterService clusterService;
    private final HttpClient httpClient;
    private final Client client;
    private final Clock clock;
    private TimeValue pollInterval;
    private String endpoint;

    public GeoIpDownloader(LicenseService licenseService, Client client, HttpClient httpClient, ClusterService clusterService,
                           Clock clock) {
        this.licenseService = licenseService;
        this.clusterService = clusterService;
        this.httpClient = httpClient;
        this.client = client;
        this.clock = clock;

        pollInterval = POLL_INTERVAL_SETTING.get(clusterService.state().metadata().settings());
        endpoint = ENDPOINT_SETTING.get(clusterService.state().metadata().settings());

        clusterService.addListener(this);
    }

    void updateDatabases() throws IOException {
        String data = httpClient.getString(endpoint + "?key=" + licenseService.getLicense().uid());
        List<Map<String, Object>> response = XContentHelper.convertToList(XContentFactory.xContent(XContentType.JSON), data, false);
        for (Map<String, Object> map : response) {
            String url = (String) map.remove("url");
            map.put("data", httpClient.getBytes(url));
        }
        BulkRequestBuilder bulkRequestBuilder = client.prepareBulk(".geoip_processors");
        for (Map<String, Object> stringObjectMap : response) {
            bulkRequestBuilder.add(new IndexRequest().id((String) stringObjectMap.get("name")).source(stringObjectMap));
        }
        BulkResponse bulkItemResponses = bulkRequestBuilder.execute().actionGet();
        if (bulkItemResponses.hasFailures()) {
            logger.error("could not update GeoIp databases [" + bulkItemResponses.buildFailureMessage() + "]");
            return;
        }

        AcknowledgedResponse updateSettingsResponse =
            client.admin().cluster().prepareUpdateSettings().setPersistentSettings(Settings.builder().put("geoip.updater.last.update",
                System.currentTimeMillis())).get();
        if (updateSettingsResponse.isAcknowledged() == false) {
            logger.error("could not update GeoIP update timestamp");
        }
    }

    void setPollInterval(TimeValue pollInterval) {
        this.pollInterval = pollInterval;
    }

    void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.localNodeMaster()) {

            schedulerEngine.set(new SchedulerEngine(event.state().getMetadata().settings(), clock));
            schedulerEngine.get().start(Collections.singleton(new SchedulerEngine.Job("sf", new SchedulerEngine.Schedule() {
                @Override
                public long nextScheduledTimeAfter(long startTime, long now) {
                    return 0;
                }
            })));
        }
    }
}
