/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.TemplateBundle;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.ml.MlStatsIndex;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndexFields;
import org.elasticsearch.xpack.core.ml.notifications.NotificationsIndex;
import org.elasticsearch.xpack.core.template.IndexTemplateConfig;

import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.core.template.IndexTemplateRegistry.parseComposableTemplates;

public class MachineLearningTemplateBundle implements TemplateBundle {

    private static final String ROOT_RESOURCE_PATH = "/org/elasticsearch/xpack/core/ml/";
    private static final String ANOMALY_DETECTION_PATH = ROOT_RESOURCE_PATH + "anomalydetection/";
    private static final String VERSION_PATTERN = "xpack.ml.version";
    private static final String VERSION_ID_PATTERN = "xpack.ml.version.id";
    private static final String INDEX_LIFECYCLE_NAME = "xpack.ml.index.lifecycle.name";
    private static final String INDEX_LIFECYCLE_ROLLOVER_ALIAS = "xpack.ml.index.lifecycle.rollover_alias";

    public static final IndexTemplateConfig NOTIFICATIONS_TEMPLATE = notificationsTemplate();

    private static final String ML_SIZE_BASED_ILM_POLICY_NAME = "ml-size-based-ilm-policy";

    private static IndexTemplateConfig stateTemplate() {
        Map<String, String> variables = new HashMap<>();
        variables.put(VERSION_ID_PATTERN, String.valueOf(Version.CURRENT.id));
        variables.put(INDEX_LIFECYCLE_NAME, ML_SIZE_BASED_ILM_POLICY_NAME);
        variables.put(INDEX_LIFECYCLE_ROLLOVER_ALIAS, AnomalyDetectorsIndex.jobStateIndexWriteAlias());

        return new IndexTemplateConfig(
            AnomalyDetectorsIndexFields.STATE_INDEX_PREFIX,
            ANOMALY_DETECTION_PATH + "state_index_template.json",
            Version.CURRENT.id,
            VERSION_PATTERN,
            variables
        );
    }

    private static IndexTemplateConfig anomalyDetectionResultsTemplate() {
        Map<String, String> variables = new HashMap<>();
        variables.put(VERSION_ID_PATTERN, String.valueOf(Version.CURRENT.id));
        variables.put("xpack.ml.anomalydetection.results.mappings", AnomalyDetectorsIndex.resultsMapping());

        return new IndexTemplateConfig(
            AnomalyDetectorsIndex.jobResultsIndexPrefix(),
            ANOMALY_DETECTION_PATH + "results_index_template.json",
            Version.CURRENT.id,
            VERSION_PATTERN,
            variables
        );
    }

    private static IndexTemplateConfig notificationsTemplate() {
        Map<String, String> variables = new HashMap<>();
        variables.put(VERSION_ID_PATTERN, String.valueOf(Version.CURRENT.id));
        variables.put("xpack.ml.notifications.mappings", NotificationsIndex.mapping());

        return new IndexTemplateConfig(
            NotificationsIndex.NOTIFICATIONS_INDEX,
            ROOT_RESOURCE_PATH + "notifications_index_template.json",
            Version.CURRENT.id,
            VERSION_PATTERN,
            variables
        );
    }

    private static IndexTemplateConfig statsTemplate() {
        Map<String, String> variables = new HashMap<>();
        variables.put(VERSION_ID_PATTERN, String.valueOf(Version.CURRENT.id));
        variables.put("xpack.ml.stats.mappings", MlStatsIndex.mapping());
        variables.put(INDEX_LIFECYCLE_NAME, ML_SIZE_BASED_ILM_POLICY_NAME);
        variables.put(INDEX_LIFECYCLE_ROLLOVER_ALIAS, MlStatsIndex.writeAlias());

        return new IndexTemplateConfig(
            MlStatsIndex.TEMPLATE_NAME,
            ROOT_RESOURCE_PATH + "stats_index_template.json",
            Version.CURRENT.id,
            VERSION_PATTERN,
            variables
        );
    }

    private static final Map<String, ComposableIndexTemplate> COMPOSABLE_INDEX_TEMPLATE_CONFIGS = parseComposableTemplates(
        anomalyDetectionResultsTemplate(),
        stateTemplate(),
        NOTIFICATIONS_TEMPLATE,
        statsTemplate()
    );

    @Override
    public Map<String, ComposableIndexTemplate> getComposableIndexTemplates() {
        return COMPOSABLE_INDEX_TEMPLATE_CONFIGS;
    }

    @Override
    public String getName() {
        return ClientHelper.ML_ORIGIN;
    }

}
