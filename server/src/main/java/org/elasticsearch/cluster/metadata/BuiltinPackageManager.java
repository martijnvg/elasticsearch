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
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.immutablestate.service.ImmutableClusterStateController;
import org.elasticsearch.immutablestate.service.PackageVersion;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class BuiltinPackageManager implements ClusterStateListener {

    private static final String NAMESPACE = "builtin_resources";
    private static final Logger LOGGER = LogManager.getLogger(BuiltinPackageManager.class);

    private final ClusterService clusterService;
    private final BuiltinPackages builtinPackages;
    private final ImmutableClusterStateController controller;
    private final AtomicReference<InstallationState> installing = new AtomicReference<>(InstallationState.NOT_INSTALLED);

    public BuiltinPackageManager(
        ClusterService clusterService,
        BuiltinPackages builtinPackages,
        ImmutableClusterStateController controller
    ) {
        this.clusterService = clusterService;
        this.builtinPackages = builtinPackages;
        this.controller = controller;
        clusterService.addListener(this);
        builtinPackages.packages()
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
        final var counter = new AtomicInteger(builtinPackages.packages().size());
        for (var builtinPackage : builtinPackages.packages()) {

            if (builtinPackage.getEnabledSetting() != null) {
                boolean enabled = clusterService.getClusterSettings().get(builtinPackage.getEnabledSetting());
                if (enabled == false) {
                    continue;
                }
            }

            var packageVersion = new PackageVersion(builtinPackage.version(), Version.CURRENT);
            Map<String, Object> state = new HashMap<>();
            state.put("ilm", List.copyOf(builtinPackage.getLifecycleTemplates().values()));
            var p = new ImmutableClusterStateController.Package(state, packageVersion);
            controller.process(NAMESPACE + "/" + builtinPackage.getName(), p, e -> {
                if (counter.decrementAndGet() == 0) {
                    if (e == null) {
                        LOGGER.info(
                            "successfully installed resources for [{}], ilm templates [{}]",
                            builtinPackage.getName(),
                            Strings.join(builtinPackage.getLifecycleTemplates().keySet(), ',')
                        );
                        installing.set(InstallationState.INSTALLED);
                    } else {
                        LOGGER.error(() -> "failed to resources for [" + builtinPackage.getName() + "]", e);
                        installing.set(InstallationState.NOT_INSTALLED);
                    }
                }
            });
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
