/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.engine;

import org.apache.lucene.index.FilterMergePolicy;

import java.util.Collection;
import java.util.List;
import java.util.function.BiFunction;

public class InternalEngineFactory implements EngineFactory {

    private final Collection<BiFunction<FilterMergePolicy, Engine, FilterMergePolicy>> mergePolicyDecorators;

    public InternalEngineFactory() {
        this(List.of());
    }

    public InternalEngineFactory(Collection<BiFunction<FilterMergePolicy, Engine, FilterMergePolicy>> mergePolicyDecorators) {
        this.mergePolicyDecorators = mergePolicyDecorators;
    }

    @Override
    public Engine newReadWriteEngine(EngineConfig config) {
        return new InternalEngine(config, mergePolicyDecorators);
    }
}
