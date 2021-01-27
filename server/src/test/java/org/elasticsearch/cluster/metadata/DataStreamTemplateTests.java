/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.cluster.metadata;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.cluster.metadata.ComposableIndexTemplate.DataStreamAliasTemplate;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate.DataStreamTemplate;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;

public class DataStreamTemplateTests extends AbstractSerializingTestCase<DataStreamTemplate> {

    @Override
    protected DataStreamTemplate doParseInstance(XContentParser parser) throws IOException {
        return DataStreamTemplate.PARSER.parse(parser, null);
    }

    @Override
    protected Writeable.Reader<DataStreamTemplate> instanceReader() {
        return DataStreamTemplate::new;
    }

    @Override
    protected DataStreamTemplate createTestInstance() {
        return randomInstance();
    }

    public static DataStreamTemplate randomInstance() {
        if (randomBoolean()) {
            return new ComposableIndexTemplate.DataStreamTemplate();
        }

        boolean hidden = randomBoolean();
        Map<String, DataStreamAliasTemplate> aliases = null;
        if (randomBoolean()) {
            aliases = new HashMap<>();
            int numAliases = randomIntBetween(1, 5);
            for (int i = 0; i < numAliases; i++) {
                ComposableIndexTemplate.DataStreamAliasTemplate instance = randomAliasInstance();
                aliases.put(instance.getAlias(), instance);
            }
        }
        return new ComposableIndexTemplate.DataStreamTemplate(hidden, aliases);
    }

    static DataStreamAliasTemplate randomAliasInstance() {
        String alias = randomAlphaOfLength(4);
        Boolean writeAlias = null;
        if (randomBoolean()) {
            writeAlias = randomBoolean();
        }
        return new DataStreamAliasTemplate(alias, writeAlias);
    }
}
