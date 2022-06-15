/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stack;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.ResourceBundle;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.core.Streams;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.template.TemplateUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.Map;

public class StackResourceBundle implements ResourceBundle {

    public static final String LOGS_ILM_POLICY_NAME = "logs";
    public static final String METRICS_ILM_POLICY_NAME = "metrics";
    public static final String SYNTHETICS_ILM_POLICY_NAME = "synthetics";

    //////////////////////////////////////////////////////////
    // Built in ILM policies for users to use
    //////////////////////////////////////////////////////////
    public static final String ILM_7_DAYS_POLICY_NAME = "7-days-default";
    public static final String ILM_30_DAYS_POLICY_NAME = "30-days-default";
    public static final String ILM_90_DAYS_POLICY_NAME = "90-days-default";
    public static final String ILM_180_DAYS_POLICY_NAME = "180-days-default";
    public static final String ILM_365_DAYS_POLICY_NAME = "365-days-default";

    private static final Map<String, BytesReference> LIFECYCLE_POLICIES;

    static {
        try {
            LIFECYCLE_POLICIES = Map.of(
                LOGS_ILM_POLICY_NAME,
                load("/logs-policy.json"),
                METRICS_ILM_POLICY_NAME,
                load("/metrics-policy.json"),
                SYNTHETICS_ILM_POLICY_NAME,
                load("/synthetics-policy.json"),
                ILM_7_DAYS_POLICY_NAME,
                load("/" + ILM_7_DAYS_POLICY_NAME + ".json"),
                ILM_30_DAYS_POLICY_NAME,
                load("/" + ILM_30_DAYS_POLICY_NAME + ".json"),
                ILM_90_DAYS_POLICY_NAME,
                load("/" + ILM_90_DAYS_POLICY_NAME + ".json"),
                ILM_180_DAYS_POLICY_NAME,
                load("/" + ILM_180_DAYS_POLICY_NAME + ".json"),
                ILM_365_DAYS_POLICY_NAME,
                load("/" + ILM_365_DAYS_POLICY_NAME + ".json")
            );
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public String getName() {
        return ClientHelper.STACK_ORIGIN;
    }

    @Override
    public long version() {
        return StackTemplateRegistry.REGISTRY_VERSION;
    }

    @Override
    public Version minimalSupportedVersion() {
        return Version.V_8_4_0;
    }

    @Override
    public Map<String, BytesReference> getLifecycleTemplates() {
        return LIFECYCLE_POLICIES;
    }

    @Override
    public Setting<Boolean> getEnabledSetting() {
        return StackTemplateRegistry.STACK_TEMPLATES_ENABLED;
    }

    private static BytesReference load(String name) throws IOException {
        try (InputStream is = TemplateUtils.class.getResourceAsStream(name)) {
            try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
                Streams.copy(is, out);
                return new BytesArray(out.toByteArray());
            }
        }
    }

}
