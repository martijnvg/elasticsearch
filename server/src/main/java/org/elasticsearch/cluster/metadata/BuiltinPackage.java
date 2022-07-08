/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.common.settings.Setting;

import java.util.Map;

public interface BuiltinPackage {

    String getName();

    long version();

    default Map<String, ?> getLifecycleTemplates() {
        return Map.of();
    }

    default Setting<Boolean> getEnabledSetting() {
        return null;
    };

}
