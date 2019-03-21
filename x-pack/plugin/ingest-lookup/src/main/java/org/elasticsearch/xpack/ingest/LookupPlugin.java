/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ingest;

import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.plugins.IngestPlugin;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.ingest.lookup.LookupFieldMapper;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class LookupPlugin extends Plugin implements IngestPlugin, MapperPlugin {

    @Override
    public Map<String, Processor.Factory> getProcessors(Processor.Parameters parameters) {
        Map<String, Processor.Factory> factories = new HashMap<>();
        factories.put(DecorateProcessor.TYPE, new DecorateProcessor.Factory(parameters.localSearcherProvider));
        factories.put(DecorateProcessor2.TYPE, new DecorateProcessor2.Factory(parameters.searcherProvider, parameters.fieldDateProvider));
        factories.put(DecorateProcessor3.TYPE, new DecorateProcessor3.Factory(parameters.searcherProvider, parameters.fieldDateProvider));
        factories.put(DecorateProcessor4.TYPE, new DecorateProcessor4.Factory(parameters.searcherProvider, parameters.fieldDateProvider));
        return factories;
    }

    @Override
    public Map<String, Mapper.TypeParser> getMappers() {
        // Needed for prototype4:
        return Collections.singletonMap(LookupFieldMapper.CONTENT_TYPE, new LookupFieldMapper.TypeParser());
    }
}
