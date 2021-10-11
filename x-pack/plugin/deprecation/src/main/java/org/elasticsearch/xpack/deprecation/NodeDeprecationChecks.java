/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.deprecation;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.node.info.PluginsAndModules;
import org.elasticsearch.bootstrap.BootstrapSettings;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.coordination.JoinHelper;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.routing.allocation.DataTier;
import org.elasticsearch.cluster.routing.allocation.DiskThresholdSettings;
import org.elasticsearch.cluster.routing.allocation.decider.DiskThresholdDecider;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.ssl.SslConfigurationKeys;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.xpack.core.deprecation.DeprecationIssue;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.jdk.JavaVersion;
import org.elasticsearch.license.License;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeRoleSettings;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.transport.RemoteClusterService;
import org.elasticsearch.transport.SniffConnectionStrategy;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.SecurityField;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;
import org.elasticsearch.xpack.core.security.authc.esnative.NativeRealmSettings;
import org.elasticsearch.xpack.core.security.authc.file.FileRealmSettings;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.routing.allocation.DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_INCLUDE_RELOCATIONS_SETTING;
import static org.elasticsearch.xpack.cluster.routing.allocation.DataTierAllocationDecider.CLUSTER_ROUTING_EXCLUDE_SETTING;
import static org.elasticsearch.xpack.cluster.routing.allocation.DataTierAllocationDecider.CLUSTER_ROUTING_INCLUDE_SETTING;
import static org.elasticsearch.xpack.cluster.routing.allocation.DataTierAllocationDecider.CLUSTER_ROUTING_REQUIRE_SETTING;
import static org.elasticsearch.xpack.core.security.authc.RealmSettings.RESERVED_REALM_NAME_PREFIX;
import static org.elasticsearch.xpack.core.security.authc.saml.SamlRealmSettings.PRINCIPAL_ATTRIBUTE;

class NodeDeprecationChecks {

    static DeprecationIssue checkPidfile(final Settings settings, final PluginsAndModules pluginsAndModules,
                                         final ClusterState clusterState, final XPackLicenseState licenseState) {
        return checkDeprecatedSetting(
            settings,
            pluginsAndModules,
            Environment.PIDFILE_SETTING,
            Environment.NODE_PIDFILE_SETTING,
            "https://ela.st/es-deprecation-7-pidfile-setting");
    }

    static DeprecationIssue checkProcessors(final Settings settings , final PluginsAndModules pluginsAndModules,
                                            final ClusterState clusterState, final XPackLicenseState licenseState) {
        return checkDeprecatedSetting(
            settings,
            pluginsAndModules,
            EsExecutors.PROCESSORS_SETTING,
            EsExecutors.NODE_PROCESSORS_SETTING,
            "https://ela.st/es-deprecation-7-processors-setting");
    }

    static DeprecationIssue checkMissingRealmOrders(final Settings settings, final PluginsAndModules pluginsAndModules,
                                                    final ClusterState clusterState, final XPackLicenseState licenseState) {
        final Set<String> orderNotConfiguredRealms = RealmSettings.getRealmSettings(settings).entrySet()
                .stream()
                .filter(e -> false == e.getValue().hasValue(RealmSettings.ORDER_SETTING_KEY))
                .filter(e -> e.getValue().getAsBoolean(RealmSettings.ENABLED_SETTING_KEY, true))
                .map(e -> RealmSettings.realmSettingPrefix(e.getKey()) + RealmSettings.ORDER_SETTING_KEY)
                .collect(Collectors.toSet());

        if (orderNotConfiguredRealms.isEmpty()) {
            return null;
        }

        final String details = String.format(
            Locale.ROOT,
            "Found realms without order config: [%s]. In next major release, node will fail to start with missing realm order.",
            String.join("; ", orderNotConfiguredRealms));
        return new DeprecationIssue(
            DeprecationIssue.Level.CRITICAL,
            "Realm order will be required in next major release.",
            "https://ela.st/es-deprecation-7-realm-orders-required",
            details,
            false,
            null
        );
    }

    static DeprecationIssue checkUniqueRealmOrders(final Settings settings, final PluginsAndModules pluginsAndModules,
                                                   final ClusterState clusterState, final XPackLicenseState licenseState) {
        final Map<String, List<String>> orderToRealmSettings =
            RealmSettings.getRealmSettings(settings).entrySet()
                .stream()
                .filter(e -> e.getValue().hasValue(RealmSettings.ORDER_SETTING_KEY))
                .collect(Collectors.groupingBy(
                    e -> e.getValue().get(RealmSettings.ORDER_SETTING_KEY),
                    Collectors.mapping(e -> RealmSettings.realmSettingPrefix(e.getKey()) + RealmSettings.ORDER_SETTING_KEY,
                        Collectors.toList())));

        Set<String> duplicateOrders = orderToRealmSettings.entrySet().stream()
            .filter(entry -> entry.getValue().size() > 1)
            .map(entry -> entry.getKey() + ": " + entry.getValue())
            .collect(Collectors.toSet());

        if (duplicateOrders.isEmpty()) {
            return null;
        }

        final String details = String.format(
            Locale.ROOT,
            "Found multiple realms configured with the same order: [%s]. " +
                "In next major release, node will fail to start with duplicated realm order.",
            String.join("; ", duplicateOrders));

        return new DeprecationIssue(
            DeprecationIssue.Level.CRITICAL,
            "Realm orders must be unique in next major release.",
            "https://ela.st/es-deprecation-7-realm-orders-unique",
            details,
           false, null
        );
    }

    static DeprecationIssue checkImplicitlyDisabledSecurityOnBasicAndTrial(final Settings settings,
                                                                           final PluginsAndModules pluginsAndModules,
                                                                           final ClusterState clusterState,
                                                                           final XPackLicenseState licenseState) {
        if ( XPackSettings.SECURITY_ENABLED.exists(settings) == false
            && (licenseState.getOperationMode().equals(License.OperationMode.BASIC)
            || licenseState.getOperationMode().equals(License.OperationMode.TRIAL))) {
          String details = "The default behavior of disabling security on " + licenseState.getOperationMode().description()
              + " licenses is deprecated. In a later version of Elasticsearch, the value of [xpack.security.enabled] will "
              + "default to \"true\" , regardless of the license level. "
              + "See https://www.elastic.co/guide/en/elasticsearch/reference/" + Version.CURRENT.major + "."
              + Version.CURRENT.minor + "/security-minimal-setup.html to enable security, or explicitly disable security by "
              + "setting [xpack.security.enabled] to \"false\" in elasticsearch.yml";
            return new DeprecationIssue(
                DeprecationIssue.Level.CRITICAL,
                "Security is enabled by default for all licenses in the next major version.",
                "https://ela.st/es-deprecation-7-implicitly-disabled-security",
                details,
               false, null);
        }
        return null;
    }

    static DeprecationIssue checkImplicitlyDisabledBasicRealms(final Settings settings, final PluginsAndModules pluginsAndModules,
                                                               final ClusterState clusterState, final XPackLicenseState licenseState) {
        final Map<RealmConfig.RealmIdentifier, Settings> realmSettings = RealmSettings.getRealmSettings(settings);
        if (realmSettings.isEmpty()) {
            return null;
        }

        boolean anyRealmEnabled = false;
        final Set<String> unconfiguredBasicRealms =
            new HashSet<>(org.elasticsearch.core.Set.of(FileRealmSettings.TYPE, NativeRealmSettings.TYPE));
        for (Map.Entry<RealmConfig.RealmIdentifier, Settings> realmSetting: realmSettings.entrySet()) {
            anyRealmEnabled = anyRealmEnabled || realmSetting.getValue().getAsBoolean(RealmSettings.ENABLED_SETTING_KEY, true);
            unconfiguredBasicRealms.remove(realmSetting.getKey().getType());
        }

        final String details;
        if (false == anyRealmEnabled) {
            final List<String> explicitlyDisabledBasicRealms =
                Sets.difference(org.elasticsearch.core.Set.of(FileRealmSettings.TYPE, NativeRealmSettings.TYPE),
                    unconfiguredBasicRealms).stream().sorted().collect(Collectors.toList());
            if (explicitlyDisabledBasicRealms.isEmpty()) {
                return null;
            }
            details = String.format(
                Locale.ROOT,
                "Found explicitly disabled basic %s: [%s]. But %s will be enabled because no other realms are configured or enabled. " +
                    "In next major release, explicitly disabled basic realms will remain disabled.",
                explicitlyDisabledBasicRealms.size() == 1 ? "realm" : "realms",
                Strings.collectionToDelimitedString(explicitlyDisabledBasicRealms, ","),
                explicitlyDisabledBasicRealms.size() == 1 ? "it" : "they"
                );
        } else {
            if (unconfiguredBasicRealms.isEmpty()) {
                return null;
            }
            details = String.format(
                Locale.ROOT,
                "Found implicitly disabled basic %s: [%s]. %s disabled because there are other explicitly configured realms." +
                    "In next major release, basic realms will always be enabled unless explicitly disabled.",
                unconfiguredBasicRealms.size() == 1 ? "realm" : "realms",
                Strings.collectionToDelimitedString(unconfiguredBasicRealms, ","),
                unconfiguredBasicRealms.size() == 1 ? "It is" : "They are");
        }
        return new DeprecationIssue(
            DeprecationIssue.Level.WARNING,
            "File and/or native realms are enabled by default in next major release.",
            "https://ela.st/es-deprecation-7-implicitly-disabled-basic-realms",
            details,
            false,
            null
        );

    }

    static DeprecationIssue checkReservedPrefixedRealmNames(final Settings settings, final PluginsAndModules pluginsAndModules,
                                                            final ClusterState clusterState, final XPackLicenseState licenseState) {
        final Map<RealmConfig.RealmIdentifier, Settings> realmSettings = RealmSettings.getRealmSettings(settings);
        if (realmSettings.isEmpty()) {
            return null;
        }
        List<RealmConfig.RealmIdentifier> reservedPrefixedRealmIdentifiers = new ArrayList<>();
        for (RealmConfig.RealmIdentifier realmIdentifier: realmSettings.keySet()) {
            if (realmIdentifier.getName().startsWith(RESERVED_REALM_NAME_PREFIX)) {
                reservedPrefixedRealmIdentifiers.add(realmIdentifier);
            }
        }
        if (reservedPrefixedRealmIdentifiers.isEmpty()) {
            return null;
        } else {
            return new DeprecationIssue(
                DeprecationIssue.Level.WARNING,
                "Realm names cannot start with [" + RESERVED_REALM_NAME_PREFIX + "] in a future major release.",
                "https://ela.st/es-deprecation-7-realm-names",
                String.format(Locale.ROOT, "Found realm " + (reservedPrefixedRealmIdentifiers.size() == 1 ? "name" : "names")
                        + " with reserved prefix [%s]: [%s]. "
                        + "In a future major release, node will fail to start if any realm names start with reserved prefix.",
                    RESERVED_REALM_NAME_PREFIX,
                    reservedPrefixedRealmIdentifiers.stream()
                        .map(rid -> RealmSettings.PREFIX + rid.getType() + "." + rid.getName())
                        .sorted()
                        .collect(Collectors.joining("; "))),
               false, null
            );
        }
    }

    static DeprecationIssue checkThreadPoolListenerQueueSize(final Settings settings) {
        return checkThreadPoolListenerSetting("thread_pool.listener.queue_size", settings);
    }

    static DeprecationIssue checkThreadPoolListenerSize(final Settings settings) {
        return checkThreadPoolListenerSetting("thread_pool.listener.size", settings);
    }

    private static DeprecationIssue checkThreadPoolListenerSetting(final String name, final Settings settings) {
        final FixedExecutorBuilder builder = new FixedExecutorBuilder(settings, "listener", 1, -1, "thread_pool.listener", true);
        final List<Setting<?>> listenerSettings = builder.getRegisteredSettings();
        final Optional<Setting<?>> setting = listenerSettings.stream().filter(s -> s.getKey().equals(name)).findFirst();
        assert setting.isPresent();
        return checkRemovedSetting(
            settings,
            setting.get(),
            "https://ela.st/es-deprecation-7-thread-pool-listener-settings");
    }

    public static DeprecationIssue checkClusterRemoteConnectSetting(final Settings settings, final PluginsAndModules pluginsAndModules,
                                                                    final ClusterState clusterState, final XPackLicenseState licenseState) {
        return checkDeprecatedSetting(
            settings,
            pluginsAndModules,
            RemoteClusterService.ENABLE_REMOTE_CLUSTERS,
            Setting.boolSetting(
                "node.remote_cluster_client",
                RemoteClusterService.ENABLE_REMOTE_CLUSTERS,
                Property.Deprecated,
                Property.NodeScope),
            "https://ela.st/es-deprecation-7-cluster-remote-connect-setting"
        );
    }

    public static DeprecationIssue checkNodeLocalStorageSetting(final Settings settings, final PluginsAndModules pluginsAndModules,
                                                                final ClusterState clusterState, final XPackLicenseState licenseState) {
        return checkRemovedSetting(
            settings,
            Node.NODE_LOCAL_STORAGE_SETTING,
            "https://ela.st/es-deprecation-7-node-local-storage-setting"
        );
    }

    public static DeprecationIssue checkNodeBasicLicenseFeatureEnabledSetting(final Settings settings, Setting<?> setting) {
        return checkRemovedSetting(
            settings,
            setting,
            "https://ela.st/es-deprecation-7-xpack-basic-feature-settings"
        );
    }

    public static DeprecationIssue checkGeneralScriptSizeSetting(final Settings settings, final PluginsAndModules pluginsAndModules,
                                                                 final ClusterState clusterState, final XPackLicenseState licenseState) {
        return checkDeprecatedSetting(
            settings,
            pluginsAndModules,
            ScriptService.SCRIPT_GENERAL_CACHE_SIZE_SETTING,
            ScriptService.SCRIPT_CACHE_SIZE_SETTING,
            "a script context",
            "https://ela.st/es-deprecation-7-script-cache-size-setting"
        );
    }

    public static DeprecationIssue checkGeneralScriptExpireSetting(final Settings settings, final PluginsAndModules pluginsAndModules,
                                                                   final ClusterState clusterState, final XPackLicenseState licenseState) {
        return checkDeprecatedSetting(
            settings,
            pluginsAndModules,
            ScriptService.SCRIPT_GENERAL_CACHE_EXPIRE_SETTING,
            ScriptService.SCRIPT_CACHE_EXPIRE_SETTING,
            "a script context",
            "https://ela.st/es-deprecation-7-script-cache-expire-setting"
        );
    }

    public static DeprecationIssue checkGeneralScriptCompileSettings(final Settings settings, final PluginsAndModules pluginsAndModules,
                                                                    final ClusterState clusterState, final XPackLicenseState licenseState) {
        return checkDeprecatedSetting(
            settings,
            pluginsAndModules,
            ScriptService.SCRIPT_GENERAL_MAX_COMPILATIONS_RATE_SETTING,
            ScriptService.SCRIPT_MAX_COMPILATIONS_RATE_SETTING,
            "a script context",
            "https://ela.st/es-deprecation-7-script-max-compilations-rate-setting"
        );
    }

    public static DeprecationIssue checkLegacyRoleSettings(
        final Setting<Boolean> legacyRoleSetting,
        final Settings settings,
        final PluginsAndModules pluginsAndModules
    ) {

        return checkDeprecatedSetting(
            settings,
            pluginsAndModules,
            legacyRoleSetting,
            NodeRoleSettings.NODE_ROLES_SETTING,
            (v, s) -> {
                return DiscoveryNode.getRolesFromSettings(s)
                    .stream()
                    .map(DiscoveryNodeRole::roleName)
                    .collect(Collectors.joining(","));
            },
            "https://ela.st/es-deprecation-7-node-roles"
        );
    }

    static DeprecationIssue checkBootstrapSystemCallFilterSetting(final Settings settings, final PluginsAndModules pluginsAndModules,
                                                                  final ClusterState clusterState, final XPackLicenseState licenseState) {
        return checkRemovedSetting(
            settings,
            BootstrapSettings.SYSTEM_CALL_FILTER_SETTING,
            "https://ela.st/es-deprecation-7-system-call-filter-setting"
        );
    }

    private static DeprecationIssue checkDeprecatedSetting(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final Setting<?> deprecatedSetting,
        final Setting<?> replacementSetting,
        final String url
    ) {
        return checkDeprecatedSetting(settings, pluginsAndModules, deprecatedSetting, replacementSetting, (v, s) -> v, url);
    }

    private static DeprecationIssue checkDeprecatedSetting(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final Setting<?> deprecatedSetting,
        final Setting<?> replacementSetting,
        final BiFunction<String, Settings, String> replacementValue,
        final String url
    ) {
        assert deprecatedSetting.isDeprecated() : deprecatedSetting;
        if (deprecatedSetting.exists(settings) == false) {
            return null;
        }
        final String deprecatedSettingKey = deprecatedSetting.getKey();
        final String replacementSettingKey = replacementSetting.getKey();
        final String value = deprecatedSetting.get(settings).toString();
        final String message = String.format(
            Locale.ROOT,
            "setting [%s] is deprecated in favor of setting [%s]",
            deprecatedSettingKey,
            replacementSettingKey);
        final String details = String.format(
            Locale.ROOT,
            "the setting [%s] is currently set to [%s], instead set [%s] to [%s]",
            deprecatedSettingKey,
            value,
            replacementSettingKey,
            replacementValue.apply(value, settings));
        return new DeprecationIssue(DeprecationIssue.Level.CRITICAL, message, url, details, false, null);
    }

    private static DeprecationIssue checkDeprecatedSetting(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final Setting<?> deprecatedSetting,
        final Setting.AffixSetting<?> replacementSetting,
        final String star,
        final String url
    ) {
        return checkDeprecatedSetting(settings, pluginsAndModules, deprecatedSetting, replacementSetting, (v, s) -> v, star, url);
    }

    private static DeprecationIssue checkDeprecatedSetting(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final Setting<?> deprecatedSetting,
        final Setting.AffixSetting<?> replacementSetting,
        final BiFunction<String, Settings, String> replacementValue,
        final String star,
        final String url
    ) {
        assert deprecatedSetting.isDeprecated() : deprecatedSetting;
        if (deprecatedSetting.exists(settings) == false) {
            return null;
        }
        final String deprecatedSettingKey = deprecatedSetting.getKey();
        final String replacementSettingKey = replacementSetting.getKey();
        final String value = deprecatedSetting.get(settings).toString();
        final String message = String.format(
            Locale.ROOT,
            "setting [%s] is deprecated in favor of grouped setting [%s]",
            deprecatedSettingKey,
            replacementSettingKey);
        final String details = String.format(
            Locale.ROOT,
            "the setting [%s] is currently set to [%s], instead set [%s] to [%s] where * is %s",
            deprecatedSettingKey,
            value,
            replacementSettingKey,
            replacementValue.apply(value, settings),
            star);
        return new DeprecationIssue(DeprecationIssue.Level.CRITICAL, message, url, details, false, null);
    }

    static DeprecationIssue checkRemovedSetting(final Settings settings, final Setting<?> removedSetting, final String url) {
        return checkRemovedSetting(settings, removedSetting, url, DeprecationIssue.Level.CRITICAL);
    }

    static DeprecationIssue checkRemovedSetting(final Settings settings,
                                                final Setting<?> removedSetting,
                                                final String url,
                                                DeprecationIssue.Level deprecationLevel) {
        if (removedSetting.exists(settings) == false) {
            return null;
        }
        final String removedSettingKey = removedSetting.getKey();
        Object removedSettingValue = removedSetting.get(settings);
        String value;
        if (removedSettingValue instanceof TimeValue) {
            value = ((TimeValue) removedSettingValue).getStringRep();
        } else {
            value = removedSettingValue.toString();
        }
        final String message =
            String.format(Locale.ROOT, "setting [%s] is deprecated and will be removed in the next major version", removedSettingKey);
        final String details =
            String.format(Locale.ROOT, "the setting [%s] is currently set to [%s], remove this setting", removedSettingKey, value);
        return new DeprecationIssue(deprecationLevel, message, url, details, false, null);
    }

    static DeprecationIssue javaVersionCheck(Settings nodeSettings, PluginsAndModules plugins, final ClusterState clusterState,
                                             final XPackLicenseState licenseState) {
        final JavaVersion javaVersion = JavaVersion.current();

        if (javaVersion.compareTo(JavaVersion.parse("11")) < 0) {
            return new DeprecationIssue(DeprecationIssue.Level.CRITICAL,
                "Java 11 is required",
                "https://ela.st/es-deprecation-7-java-version",
                "Java 11 will be required for future versions of Elasticsearch, this node is running version ["
                    + javaVersion.toString() + "]. Consider switching to a distribution of Elasticsearch with a bundled JDK. "
                    + "If you are already using a distribution with a bundled JDK, ensure the JAVA_HOME environment variable is not set.",
                false, null);
        }
        return null;
    }

    static DeprecationIssue checkMultipleDataPaths(Settings nodeSettings, PluginsAndModules plugins, final ClusterState clusterState,
                                                   final XPackLicenseState licenseState) {
        List<String> dataPaths = Environment.PATH_DATA_SETTING.get(nodeSettings);
        if (dataPaths.size() > 1) {
            return new DeprecationIssue(DeprecationIssue.Level.CRITICAL,
                "multiple [path.data] entries are deprecated, use a single data directory",
                "https://ela.st/es-deprecation-7-multiple-paths",
                "Multiple data paths are deprecated. Instead, use RAID or other system level features to utilize multiple disks.",
            false, null);
        }
        return null;
    }

    static DeprecationIssue checkDataPathsList(Settings nodeSettings, PluginsAndModules plugins, final ClusterState clusterState,
                                               final XPackLicenseState licenseState) {
        if (Environment.dataPathUsesList(nodeSettings)) {
            return new DeprecationIssue(DeprecationIssue.Level.CRITICAL,
                "[path.data] in a list is deprecated, use a string value",
                "https://ela.st/es-deprecation-7-multiple-paths",
                "Configuring [path.data] with a list is deprecated. Instead specify as a string value.", false, null);
        }
        return null;
    }

    static DeprecationIssue checkSharedDataPathSetting(final Settings settings, final PluginsAndModules pluginsAndModules,
                                                       final ClusterState clusterState, final XPackLicenseState licenseState) {
        if (Environment.PATH_SHARED_DATA_SETTING.exists(settings)) {
            final String message = String.format(Locale.ROOT,
                "setting [%s] is deprecated and will be removed in a future version", Environment.PATH_SHARED_DATA_SETTING.getKey());
            final String url = "https://ela.st/es-deprecation-7-shared-path-settings";
            final String details = "Found shared data path configured. Discontinue use of this setting.";
            return new DeprecationIssue(DeprecationIssue.Level.CRITICAL, message, url, details, false, null);
        }
        return null;
    }

    static DeprecationIssue checkSingleDataNodeWatermarkSetting(final Settings settings, final PluginsAndModules pluginsAndModules,
                                                                final ClusterState clusterState, final XPackLicenseState licenseState) {
        if (DiskThresholdDecider.ENABLE_FOR_SINGLE_DATA_NODE.get(settings) == false
            && DiskThresholdDecider.ENABLE_FOR_SINGLE_DATA_NODE.exists(settings)) {
            String key = DiskThresholdDecider.ENABLE_FOR_SINGLE_DATA_NODE.getKey();
            return new DeprecationIssue(DeprecationIssue.Level.CRITICAL,
                String.format(Locale.ROOT, "setting [%s=false] is deprecated and will not be available in a future version", key),
                "https://ela.st/es-deprecation-7-disk-watermark-enable-for-single-node-setting",
                String.format(Locale.ROOT, "found [%s] configured to false. Discontinue use of this setting or set it to true.", key),
                    false, null
            );
        }

        if (DiskThresholdDecider.ENABLE_FOR_SINGLE_DATA_NODE.get(settings) == false
            && clusterState.getNodes().getDataNodes().size() == 1 && clusterState.getNodes().getLocalNode().isMasterNode()) {
            String key = DiskThresholdDecider.ENABLE_FOR_SINGLE_DATA_NODE.getKey();
            String disableDiskDecider = DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING.getKey();
            return new DeprecationIssue(DeprecationIssue.Level.WARNING,
                String.format(Locale.ROOT, "the default value [false] of setting [%s] is deprecated and will be changed to true" +
                    " in a future version. This cluster has only one data node and behavior will therefore change when upgrading", key),
                "https://ela.st/es-deprecation-7-disk-watermark-enable-for-single-node-setting",
                String.format(Locale.ROOT, "found [%s] defaulting to false on a single data node cluster." +
                        " Set it to true to avoid this warning." +
                        " Consider using [%s] to disable disk based allocation", key,
                    disableDiskDecider),
                false,
                null
            );

        }

        return null;
    }

    static DeprecationIssue checkMonitoringExporterPassword(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        ClusterState cs,
        XPackLicenseState licenseState
    ) {
        // Mimic the HttpExporter#AUTH_PASSWORD_SETTING setting here to avoid a depedency on monitoring module:
        // (just having the setting prefix and suffic here is sufficient to check on whether this setting is used)
        final Setting.AffixSetting<String> AUTH_PASSWORD_SETTING =
            Setting.affixKeySetting("xpack.monitoring.exporters.","auth.password", s -> Setting.simpleString(s));
        List<Setting<?>> passwords = AUTH_PASSWORD_SETTING.getAllConcreteSettings(settings)
            .sorted(Comparator.comparing(Setting::getKey)).collect(Collectors.toList());

        if (passwords.isEmpty()) {
            return null;
        }

        final String passwordSettings = passwords.stream().map(Setting::getKey).collect(Collectors.joining(","));
        final String message = String.format(
            Locale.ROOT,
            "non-secure passwords for monitoring exporters [%s] are deprecated and will be removed in the next major version",
            passwordSettings
        );
        final String details = String.format(
            Locale.ROOT,
            "replace the non-secure monitoring exporter password setting(s) [%s] with their secure 'auth.secure_password' replacement",
            passwordSettings
        );
        final String url = "https://ela.st/es-deprecation-7-monitoring-exporter-passwords";
        return new DeprecationIssue(DeprecationIssue.Level.CRITICAL, message, url, details, false, null);
    }

    static DeprecationIssue checkJoinTimeoutSetting(final Settings settings,
                                                    final PluginsAndModules pluginsAndModules,
                                                    final ClusterState clusterState,
                                                    final XPackLicenseState licenseState) {
        return checkRemovedSetting(settings,
            JoinHelper.JOIN_TIMEOUT_SETTING,
            "https://ela.st/es-deprecation-7-cluster-join-timeout-setting",
            DeprecationIssue.Level.CRITICAL
        );
    }

    static DeprecationIssue checkSearchRemoteSettings(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        ClusterState cs,
        XPackLicenseState licenseState
    ) {
        List<Setting<?>> remoteClusterSettings = new ArrayList<>();
        remoteClusterSettings.addAll(SniffConnectionStrategy.SEARCH_REMOTE_CLUSTERS_SEEDS.getAllConcreteSettings(settings)
            .sorted(Comparator.comparing(Setting::getKey)).collect(Collectors.toList()));
        remoteClusterSettings.addAll(SniffConnectionStrategy.SEARCH_REMOTE_CLUSTERS_PROXY.getAllConcreteSettings(settings)
            .sorted(Comparator.comparing(Setting::getKey)).collect(Collectors.toList()));
        remoteClusterSettings.addAll(RemoteClusterService.SEARCH_REMOTE_CLUSTER_SKIP_UNAVAILABLE.getAllConcreteSettings(settings)
            .sorted(Comparator.comparing(Setting::getKey)).collect(Collectors.toList()));
        if (SniffConnectionStrategy.SEARCH_REMOTE_CONNECTIONS_PER_CLUSTER.exists(settings)) {
            remoteClusterSettings.add(SniffConnectionStrategy.SEARCH_REMOTE_CONNECTIONS_PER_CLUSTER);
        }
        if (RemoteClusterService.SEARCH_REMOTE_INITIAL_CONNECTION_TIMEOUT_SETTING.exists(settings)) {
            remoteClusterSettings.add(RemoteClusterService.SEARCH_REMOTE_INITIAL_CONNECTION_TIMEOUT_SETTING);
        }
        if (RemoteClusterService.SEARCH_REMOTE_NODE_ATTRIBUTE.exists(settings)) {
            remoteClusterSettings.add(RemoteClusterService.SEARCH_REMOTE_NODE_ATTRIBUTE);
        }
        if (RemoteClusterService.SEARCH_ENABLE_REMOTE_CLUSTERS.exists(settings)) {
            remoteClusterSettings.add(RemoteClusterService.SEARCH_ENABLE_REMOTE_CLUSTERS);
        }
        if (remoteClusterSettings.isEmpty()) {
            return null;
        }
        final String remoteClusterSeedSettings = remoteClusterSettings.stream().map(Setting::getKey).collect(Collectors.joining(","));
        final String message = String.format(
            Locale.ROOT,
            "search.remote settings [%s] are deprecated and will be removed in the next major version",
            remoteClusterSeedSettings
        );
        final String details = String.format(
            Locale.ROOT,
            "replace search.remote settings [%s] with their secure 'cluster.remote' replacements",
            remoteClusterSeedSettings
        );
        final String url = "https://ela.st/es-deprecation-7-search-remote-settings";
        return new DeprecationIssue(DeprecationIssue.Level.CRITICAL, message, url, details, false, null);
    }

    static DeprecationIssue checkClusterRoutingAllocationIncludeRelocationsSetting(final Settings settings,
                                                                                   final PluginsAndModules pluginsAndModules,
                                                                                   final ClusterState clusterState,
                                                                                   final XPackLicenseState licenseState) {
        return checkRemovedSetting(settings,
            CLUSTER_ROUTING_ALLOCATION_INCLUDE_RELOCATIONS_SETTING,
            "https://ela.st/es-deprecation-7-cluster-routing-allocation-disk-include-relocations-setting",
            DeprecationIssue.Level.CRITICAL
        );
    }

    static DeprecationIssue checkFractionalByteValueSettings(final Settings settings,
                                                             final PluginsAndModules pluginsAndModules,
                                                             final ClusterState clusterState,
                                                             final XPackLicenseState licenseState) {
        Map<String, String> fractionalByteSettings = new HashMap<>();
        for (String key : settings.keySet()) {
            try {
                settings.getAsBytesSize(key, ByteSizeValue.ZERO);
                String stringValue = settings.get(key);
                if (stringValue.contains(".")) {
                    fractionalByteSettings.put(key, stringValue);
                }
            } catch (Exception ignoreThis) {
                // We expect anything that is not a byte setting to throw an exception, but we don't care about those
            }
        }
        if (fractionalByteSettings.isEmpty()) {
            return null;
        }
        String url = "https://ela.st/es-deprecation-7-fractional-byte-settings";
        String message = "support for fractional byte size values is deprecated and will be removed in a future release";
        String details = "change the following settings to non-fractional values: [" +
            fractionalByteSettings.entrySet().stream().map(fractionalByteSetting -> fractionalByteSetting.getKey() + "->" +
                fractionalByteSetting.getValue()).collect(Collectors.joining(", ")) + "]";
        return new DeprecationIssue(DeprecationIssue.Level.WARNING, message, url, details, false, null);
    }

    static DeprecationIssue checkFrozenCacheLeniency(final Settings settings,
                                                     final PluginsAndModules pluginsAndModules,
                                                     final ClusterState clusterState,
                                                     final XPackLicenseState licenseState) {
        final String cacheSizeSettingKey = "xpack.searchable.snapshot.shared_cache.size";
        Setting<ByteSizeValue> cacheSizeSetting =  Setting.byteSizeSetting(cacheSizeSettingKey,  ByteSizeValue.ZERO);
        if (cacheSizeSetting.exists(settings)) {
            ByteSizeValue cacheSize = cacheSizeSetting.get(settings);
            if (cacheSize.getBytes() > 0) {
                final List<DiscoveryNodeRole> roles = NodeRoleSettings.NODE_ROLES_SETTING.get(settings);
                if (DataTier.isFrozenNode(new HashSet<>(roles)) == false) {
                    String message = String.format(Locale.ROOT, "setting [%s] cannot be greater than zero on non-frozen nodes",
                        cacheSizeSettingKey);
                    String url = "https://ela.st/es-deprecation-7-searchable-snapshot-shared-cache-setting";
                    String details = String.format(Locale.ROOT, "setting [%s] cannot be greater than zero on non-frozen nodes, and is " +
                        "currently set to [%s]", cacheSizeSettingKey, settings.get(cacheSizeSettingKey));
                    return new DeprecationIssue(DeprecationIssue.Level.CRITICAL, message, url, details, false, null);
                }
            }
        }
        return null;
    }

    static DeprecationIssue checkSslServerEnabled(final Settings settings,
                                                   final PluginsAndModules pluginsAndModules,
                                                   final ClusterState clusterState,
                                                   final XPackLicenseState licenseState) {
        List<String> details = new ArrayList<>();
        for (String prefix : new String[] {"xpack.security.transport.ssl", "xpack.security.http.ssl"}) {
            final String enabledSettingKey = prefix + ".enabled";
            String enabledSettingValue = settings.get(enabledSettingKey);
            Settings sslSettings = settings.filter(setting -> setting.startsWith(prefix));
            if (enabledSettingValue == null && sslSettings.size() > 0) {
                String keys = sslSettings.keySet().stream().collect(Collectors.joining(","));
                String detail = String.format(Locale.ROOT, "setting [%s] is unset but the following settings exist: [%s]",
                    enabledSettingKey, keys);
                details.add(detail);
            }
        }
        if (details.isEmpty()) {
            return null;
        } else {
            String url = "https://ela.st/es-deprecation-7-explicit-ssl-required";
            String message = "cannot set ssl properties without explicitly enabling or disabling ssl";
            String detailsString = details.stream().collect(Collectors.joining("; "));
            return new DeprecationIssue(DeprecationIssue.Level.CRITICAL, message, url, detailsString, false, null);
        }
    }

    static DeprecationIssue checkSslCertConfiguration(final Settings settings,
                                                      final PluginsAndModules pluginsAndModules,
                                                      final ClusterState clusterState,
                                                      final XPackLicenseState licenseState) {
        List<String> details = new ArrayList<>();
        for (String prefix : new String[]{"xpack.security.transport.ssl", "xpack.security.http.ssl"}) {
            final String enabledSettingKey = prefix + ".enabled";
            boolean sslEnabled = settings.getAsBoolean(enabledSettingKey, false);
            if (sslEnabled) {
                String keystorePathSettingKey = prefix + "." + SslConfigurationKeys.KEYSTORE_PATH;
                String keyPathSettingKey = prefix + "." + SslConfigurationKeys.KEY;
                String certificatePathSettingKey = prefix + "." + SslConfigurationKeys.CERTIFICATE;
                boolean keystorePathSettingExists = settings.get(keystorePathSettingKey) != null;
                boolean keyPathSettingExists = settings.get(keyPathSettingKey) != null;
                boolean certificatePathSettingExists = settings.get(certificatePathSettingKey) != null;
                if (keystorePathSettingExists == false && keyPathSettingExists == false && certificatePathSettingExists == false) {
                    String detail = String.format(Locale.ROOT, "none of [%s], [%s], or [%s] are set. If [%s] is true either [%s] must be " +
                            "set, or [%s] and [%s] must be set", keystorePathSettingKey, keyPathSettingKey,
                        certificatePathSettingKey, enabledSettingKey, keystorePathSettingKey, keyPathSettingKey, certificatePathSettingKey);
                    details.add(detail);
                } else if (keystorePathSettingExists && keyPathSettingExists && certificatePathSettingExists) {
                    String detail = String.format(Locale.ROOT, "all of [%s], [%s], and [%s] are set. Either [%s] must be set, or [%s] and" +
                            " [%s] must be set", keystorePathSettingKey, keyPathSettingKey, certificatePathSettingKey,
                        keystorePathSettingKey, keyPathSettingKey, certificatePathSettingKey);
                    details.add(detail);
                } else if (keystorePathSettingExists && (keyPathSettingExists || certificatePathSettingExists)) {
                    String detail = String.format(Locale.ROOT, "[%s] and [%s] are set. Either [%s] must be set, or [%s] and [%s] must" +
                            " be set",
                        keystorePathSettingKey,
                        keyPathSettingExists ? keyPathSettingKey : certificatePathSettingKey,
                        keystorePathSettingKey, keyPathSettingKey, certificatePathSettingKey);
                    details.add(detail);
                } else if ((keyPathSettingExists && certificatePathSettingExists == false) ||
                    (keyPathSettingExists == false && certificatePathSettingExists)) {
                    String detail = String.format(Locale.ROOT, "[%s] is set but [%s] is not",
                        keyPathSettingExists ? keyPathSettingKey : certificatePathSettingKey,
                        keyPathSettingExists ? certificatePathSettingKey : keyPathSettingKey);
                    details.add(detail);
                }
            }
        }
        if (details.isEmpty()) {
            return null;
        } else {
            String url = "https://ela.st/es-deprecation-7-ssl-settings";
            String message = "if ssl is enabled either keystore must be set, or key path and certificate path must be set";
            String detailsString = details.stream().collect(Collectors.joining("; "));
            return new DeprecationIssue(DeprecationIssue.Level.CRITICAL, message, url, detailsString, false, null);
        }
    }

    static DeprecationIssue checkNoPermitHandshakeFromIncompatibleBuilds(final Settings settings,
                                                                         final PluginsAndModules pluginsAndModules,
                                                                         final ClusterState clusterState,
                                                                         final XPackLicenseState licenseState,
                                                                         Supplier<String> permitsHandshakesFromIncompatibleBuildsSupplier) {
        if (permitsHandshakesFromIncompatibleBuildsSupplier.get() != null) {
            final String message = String.format(
                Locale.ROOT,
                "the [%s] system property is deprecated and will be removed in the next major release",
                TransportService.PERMIT_HANDSHAKES_FROM_INCOMPATIBLE_BUILDS_KEY
            );
            final String details = String.format(
                Locale.ROOT,
                "allowing handshakes from incompatibile builds is deprecated and will be removed in the next major release; the [%s] " +
                    "system property must be removed",
                TransportService.PERMIT_HANDSHAKES_FROM_INCOMPATIBLE_BUILDS_KEY
            );
            String url = "https://ela.st/es-deprecation-7-permit-handshake-from-incompatible-builds-setting";
            return new DeprecationIssue(DeprecationIssue.Level.CRITICAL, message, url, details, false, null);
        }
        return null;
    }

    static DeprecationIssue checkTransportClientProfilesFilterSetting(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        ClusterState cs,
        XPackLicenseState licenseState
    ) {
        final Setting.AffixSetting<String> transportTypeProfileSetting =
            Setting.affixKeySetting("transport.profiles.","xpack.security.type", s -> Setting.simpleString(s));
        List<Setting<?>> transportProfiles = transportTypeProfileSetting.getAllConcreteSettings(settings)
            .sorted(Comparator.comparing(Setting::getKey)).collect(Collectors.toList());

        if (transportProfiles.isEmpty()) {
            return null;
        }

        final String transportProfilesSettings = transportProfiles.stream().map(Setting::getKey).collect(Collectors.joining(","));
        final String message = String.format(
            Locale.ROOT,
            "settings [%s] are deprecated and will be removed in the next major version",
            transportProfilesSettings
        );
        final String details = String.format(
            Locale.ROOT,
            "transport client will be removed in the next major version so transport client related settings [%s] must be removed",
            transportProfilesSettings
        );

        final String url = "https://ela.st/es-deprecation-7-transport-profiles-settings";
        return new DeprecationIssue(DeprecationIssue.Level.CRITICAL, message, url, details, false, null);
    }

    static DeprecationIssue checkDelayClusterStateRecoverySettings(final Settings settings,
                                                                   final PluginsAndModules pluginsAndModules,
                                                                   final ClusterState clusterState,
                                                                   final XPackLicenseState licenseState) {
        List<Setting<Integer>> deprecatedSettings = new ArrayList<>();
        deprecatedSettings.add(GatewayService.EXPECTED_NODES_SETTING);
        deprecatedSettings.add(GatewayService.EXPECTED_MASTER_NODES_SETTING);
        deprecatedSettings.add(GatewayService.RECOVER_AFTER_NODES_SETTING);
        deprecatedSettings.add(GatewayService.RECOVER_AFTER_MASTER_NODES_SETTING);
        List<Setting<Integer>> existingSettings =
            deprecatedSettings.stream().filter(deprecatedSetting -> deprecatedSetting.exists(settings)).collect(Collectors.toList());
        if (existingSettings.isEmpty()) {
            return null;
        }
        final String settingNames = existingSettings.stream().map(Setting::getKey).collect(Collectors.joining(","));
        final String message = String.format(
            Locale.ROOT,
            "cannot use properties related to delaying cluster state recovery after a majority of master nodes have joined because " +
                "they have been deprecated and will be removed in the next major version",
            settingNames
        );
        final String details = String.format(
            Locale.ROOT,
            "cannot use properties [%s] because they have been deprecated and will be removed in the next major version",
            settingNames
        );
        final String url = "https://ela.st/es-deprecation-7-deferred-cluster-state-recovery";
        return new DeprecationIssue(DeprecationIssue.Level.CRITICAL, message, url, details, false, null);
    }

    static DeprecationIssue checkFixedAutoQueueSizeThreadpool(final Settings settings,
                                                              final PluginsAndModules pluginsAndModules,
                                                              final ClusterState clusterState,
                                                              final XPackLicenseState licenseState) {
        List<Setting<Integer>> deprecatedSettings = new ArrayList<>();
        deprecatedSettings.add(Setting.intSetting("thread_pool.search.min_queue_size", 1, Setting.Property.Deprecated));
        deprecatedSettings.add(Setting.intSetting("thread_pool.search.max_queue_size", 1, Setting.Property.Deprecated));
        deprecatedSettings.add(Setting.intSetting("thread_pool.search.auto_queue_frame_size", 1, Setting.Property.Deprecated));
        deprecatedSettings.add(Setting.intSetting("thread_pool.search.target_response_time", 1, Setting.Property.Deprecated));
        deprecatedSettings.add(Setting.intSetting("thread_pool.search_throttled.min_queue_size", 1, Setting.Property.Deprecated));
        deprecatedSettings.add(Setting.intSetting("thread_pool.search_throttled.max_queue_size", 1, Setting.Property.Deprecated));
        deprecatedSettings.add(Setting.intSetting("thread_pool.search_throttled.auto_queue_frame_size", 1, Setting.Property.Deprecated));
        deprecatedSettings.add(Setting.intSetting("thread_pool.search_throttled.target_response_time", 1, Setting.Property.Deprecated));
        List<Setting<Integer>> existingSettings =
            deprecatedSettings.stream().filter(deprecatedSetting -> deprecatedSetting.exists(settings)).collect(Collectors.toList());
        if (existingSettings.isEmpty()) {
            return null;
        }
        final String settingNames = existingSettings.stream().map(Setting::getKey).collect(Collectors.joining(","));
        final String message = String.format(
            Locale.ROOT,
            "cannot use properties [%s] because fixed_auto_queue_size threadpool type has been deprecated and will be removed in the next" +
                " major version",
            settingNames
        );
        final String details = String.format(
            Locale.ROOT,
            "cannot use properties [%s] because fixed_auto_queue_size threadpool type has been deprecated and will be removed in the next" +
                " major version",
            settingNames
        );
        final String url = "https://ela.st/es-deprecation-7-fixed-auto-queue-size-settings";
        return new DeprecationIssue(DeprecationIssue.Level.CRITICAL, message, url, details, false, null);
    }

    static DeprecationIssue checkClusterRoutingRequireSetting(final Settings settings,
                                                              final PluginsAndModules pluginsAndModules,
                                                              final ClusterState clusterState,
                                                              final XPackLicenseState licenseState) {
        return checkRemovedSetting(settings,
            CLUSTER_ROUTING_REQUIRE_SETTING,
            "https://ela.st/es-deprecation-7-tier-filtering-settings",
            DeprecationIssue.Level.CRITICAL
        );
    }

    static DeprecationIssue checkClusterRoutingIncludeSetting(final Settings settings,
                                                              final PluginsAndModules pluginsAndModules,
                                                              final ClusterState clusterState,
                                                              final XPackLicenseState licenseState) {
        return checkRemovedSetting(settings,
            CLUSTER_ROUTING_INCLUDE_SETTING,
            "https://ela.st/es-deprecation-7-tier-filtering-settings",
            DeprecationIssue.Level.CRITICAL
        );
    }

    static DeprecationIssue checkClusterRoutingExcludeSetting(final Settings settings,
                                                              final PluginsAndModules pluginsAndModules,
                                                              final ClusterState clusterState,
                                                              final XPackLicenseState licenseState) {
        return checkRemovedSetting(settings,
            CLUSTER_ROUTING_EXCLUDE_SETTING,
            "https://ela.st/es-deprecation-7-tier-filtering-settings",
            DeprecationIssue.Level.CRITICAL
        );
    }

    static DeprecationIssue checkAcceptDefaultPasswordSetting(final Settings settings,
                                                              final PluginsAndModules pluginsAndModules,
                                                              final ClusterState clusterState,
                                                              final XPackLicenseState licenseState) {
        return checkRemovedSetting(settings,
            Setting.boolSetting(SecurityField.setting("authc.accept_default_password"),true, Setting.Property.Deprecated),
            "https://ela.st/es-deprecation-7-accept-default-password-setting",
            DeprecationIssue.Level.CRITICAL
        );
    }

    static DeprecationIssue checkAcceptRolesCacheMaxSizeSetting(final Settings settings,
                                                                final PluginsAndModules pluginsAndModules,
                                                                final ClusterState clusterState,
                                                                final XPackLicenseState licenseState) {
        return checkRemovedSetting(settings,
            Setting.intSetting(SecurityField.setting("authz.store.roles.index.cache.max_size"), 10000, Setting.Property.Deprecated),
            "https://ela.st/es-deprecation-7-roles-index-cache-settings",
            DeprecationIssue.Level.CRITICAL
        );
    }

    static DeprecationIssue checkRolesCacheTTLSizeSetting(final Settings settings,
                                                          final PluginsAndModules pluginsAndModules,
                                                          final ClusterState clusterState,
                                                          final XPackLicenseState licenseState) {
        return checkRemovedSetting(settings,
            Setting.timeSetting(SecurityField.setting("authz.store.roles.index.cache.ttl"), TimeValue.timeValueMinutes(20),
                Setting.Property.Deprecated),
            "https://ela.st/es-deprecation-7-roles-index-cache-settings",
            DeprecationIssue.Level.CRITICAL
        );
    }

    static DeprecationIssue checkMaxLocalStorageNodesSetting(final Settings settings,
                                                             final PluginsAndModules pluginsAndModules,
                                                             final ClusterState clusterState,
                                                             final XPackLicenseState licenseState) {
        return checkRemovedSetting(settings,
            NodeEnvironment.MAX_LOCAL_STORAGE_NODES_SETTING,
            "https://ela.st/es-deprecation-7-node-local-storage-setting",
            DeprecationIssue.Level.CRITICAL
        );
    }

    static DeprecationIssue checkSamlNameIdFormatSetting(final Settings settings,
                                                         final PluginsAndModules pluginsAndModules,
                                                         final ClusterState clusterState,
                                                         final XPackLicenseState licenseState) {
        final String principalKeySuffix = ".attributes.principal";
        List<String> detailsList =
            PRINCIPAL_ATTRIBUTE.getAttribute().getAllConcreteSettings(settings).sorted(Comparator.comparing(Setting::getKey))
                .map(concreteSamlPrincipalSetting -> {
                    String concreteSamlPrincipalSettingKey = concreteSamlPrincipalSetting.getKey();
                    int principalKeySuffixIndex = concreteSamlPrincipalSettingKey.indexOf(principalKeySuffix);
                    if (principalKeySuffixIndex > 0) {
                        String realm = concreteSamlPrincipalSettingKey.substring(0, principalKeySuffixIndex);
                        String concreteNameIdFormatSettingKey = realm + ".nameid_format";
                        if (settings.get(concreteNameIdFormatSettingKey) == null) {
                            return String.format(Locale.ROOT, "no value for [%s] set in realm [%s]",
                                concreteNameIdFormatSettingKey, realm);
                        }
                    }
                    return null;
                })
                .filter(detail -> detail != null).collect(Collectors.toList());
        if (detailsList.isEmpty()) {
            return null;
        } else {
            String message = "if nameid_format is not explicitly set, the previous default of " +
                "'urn:oasis:names:tc:SAML:2.0:nameid-format:transient' is no longer used";
            String url = "https://ela.st/es-deprecation-7-saml-nameid-format";
            String details = detailsList.stream().collect(Collectors.joining(","));
            return new DeprecationIssue(DeprecationIssue.Level.WARNING, message, url, details, false, null);
        }
    }
}
