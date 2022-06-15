/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.Strings;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.operator.service.OperatorClusterStateController;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class ResourceBundleManager implements ClusterStateListener {

    private static final String NAMESPACE = "builtin_resources";
    private static final Logger LOGGER = LogManager.getLogger(ResourceBundleManager.class);

    private final ClusterService clusterService;
    private final ResourceBundles resourceBundles;
    private final OperatorClusterStateController controller;
    private final AtomicReference<InstallationState> installing = new AtomicReference<>(InstallationState.NOT_INSTALLED);

    public ResourceBundleManager(
        ClusterService clusterService,
        ResourceBundles resourceBundles,
        OperatorClusterStateController controller
    ) {
        this.clusterService = clusterService;
        this.resourceBundles = resourceBundles;
        this.controller = controller;
        clusterService.addListener(this);
        resourceBundles.resourceBundles()
            .stream().filter(resourceBundle -> resourceBundle.getEnabledSetting() != null)
            .forEach(
                templateBundle -> clusterService.getClusterSettings()
                    .addSettingsUpdateConsumer(templateBundle.getEnabledSetting(), aBoolean -> {
                        // TODO: if from true to false then delete namespace
                        // if from false to true then add namespace
                    })
            );
    }

    void checkAndInstallResources(ClusterState state) {
        if (installing.compareAndSet(InstallationState.NOT_INSTALLED, InstallationState.INSTALLING)) {
            try {
                installResource();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    void installResource() throws IOException {
        final var counter = new AtomicInteger(resourceBundles.resourceBundles().size());
        for (var templateBundle : resourceBundles.resourceBundles()) {

            if (templateBundle.getEnabledSetting() != null) {
                boolean enabled = clusterService.getClusterSettings().get(templateBundle.getEnabledSetting());
                if (enabled == false) {
                    continue;
                }
            }

            try (XContentBuilder resourceBundle = XContentBuilder.builder(XContentType.JSON.xContent())) {
                resourceBundle.startObject();

                resourceBundle.startObject("metadata");
                resourceBundle.field("version", "" + templateBundle.version()); // TODO: change parser to parse as number instead of string
                resourceBundle.field("compatibility", templateBundle.minimalSupportedVersion().toString());
                resourceBundle.endObject();

                resourceBundle.startObject("state");
                if (templateBundle.getLifecycleTemplates().isEmpty() == false) {
                    resourceBundle.startObject("ilm");
                    for (var entry : templateBundle.getLifecycleTemplates().entrySet()) {
                        // policy top level field, because of PutLifecycleAction.Request
                        // (maybe not rely on request classes and use LifecyclePolicy directly?)
                        Map<String, Object> policy = Map.of(
                            "policy",
                            XContentHelper.convertToMap(
                                XContentType.JSON.xContent(),
                                entry.getValue().array(),
                                entry.getValue().arrayOffset(),
                                entry.getValue().length(),
                                false
                            )
                        );
                        resourceBundle.field(entry.getKey(), policy);
                    }
                    resourceBundle.endObject();
                }
                resourceBundle.endObject();
                resourceBundle.endObject();
                controller.process(
                    NAMESPACE + "/" + templateBundle.getName(),
                    XContentHelper.createParser(XContentParserConfiguration.EMPTY, BytesReference.bytes(resourceBundle), XContentType.JSON),
                    e -> {
                        if (counter.decrementAndGet() == 0) {
                            if (e == null) {
                                LOGGER.info(
                                    "successfully installed resources for [{}], ilm templates [{}]",
                                    templateBundle.getName(),
                                    Strings.join(templateBundle.getLifecycleTemplates().keySet(), ',')
                                );
                                installing.set(InstallationState.INSTALLED);
                            } else {
                                LOGGER.error(() -> "failed to resources for [" + templateBundle.getName() + "]", e);
                                installing.set(InstallationState.NOT_INSTALLED);
                            }
                        }
                    }
                );
            }
        }
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        ClusterState state = event.state();
        if (state.blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
            // wait until the gateway has recovered from disk, otherwise we think may not have the templates,
            // while they actually do exist
            return;
        }

        // no master node, exit immediately
        DiscoveryNode masterNode = event.state().getNodes().getMasterNode();
        if (masterNode == null) {
            return;
        }

        // This requires to run on a master node.
        // If not a master node, exit.
        if (event.localNodeMaster() == false) {
            return;
        }

        checkAndInstallResources(state);
    }

    private enum InstallationState {

        NOT_INSTALLED,
        INSTALLING,
        INSTALLED;

    }
}
