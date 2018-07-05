/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ccr;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.unit.TimeValue;

import java.util.Arrays;
import java.util.List;

/**
 * Container class for CCR settings.
 */
public final class CcrSettings {

    // prevent construction
    private CcrSettings() {

    }

    /**
     * Setting for controlling whether or not CCR is enabled.
     */
    static final Setting<Boolean> CCR_ENABLED_SETTING = Setting.boolSetting("xpack.ccr.enabled", true, Property.NodeScope);

    /**
     * Index setting for a following index.
     */
    public static final Setting<Boolean> CCR_FOLLOWING_INDEX_SETTING =
            Setting.boolSetting("index.xpack.ccr.following_index", false, Setting.Property.IndexScope);

    /**
     * Node setting for controlling the timeout between the next shard changes request when a shard follow
     * task is idle.
     */
    public static final Setting<TimeValue> CCR_IDLE_SHARD_RETRY_DELAY = Setting.timeSetting(
        "xpack.ccr.idle_shard_retry_delay", TimeValue.timeValueSeconds(10), Property.NodeScope);

    /**
     * The settings defined by CCR.
     *
     * @return the settings
     */
    static List<Setting<?>> getSettings() {
        return Arrays.asList(
                CCR_ENABLED_SETTING,
                CCR_FOLLOWING_INDEX_SETTING,
            CCR_IDLE_SHARD_RETRY_DELAY);
    }

}
