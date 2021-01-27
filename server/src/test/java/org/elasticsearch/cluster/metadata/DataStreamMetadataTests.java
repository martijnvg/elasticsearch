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

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.cluster.DataStreamTestHelper;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.test.AbstractNamedWriteableTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.test.AbstractXContentTestCase.xContentTester;

public class DataStreamMetadataTests extends AbstractNamedWriteableTestCase<DataStreamMetadata> {

    public void testFromXContent() throws IOException {
        xContentTester(this::createParser, this::createTestInstance, ToXContent.EMPTY_PARAMS, DataStreamMetadata::fromXContent)
            .assertEqualsConsumer(this::assertEqualInstances)
            .test();
    }

    @Override
    protected DataStreamMetadata createTestInstance() {
        if (randomBoolean()) {
            return new DataStreamMetadata(Map.of(), Map.of());
        }
        Map<String, DataStream> dataStreams = new HashMap<>();
        for (int i = 0; i < randomIntBetween(1, 5); i++) {
            dataStreams.put(randomAlphaOfLength(5), DataStreamTestHelper.randomInstance());
        }

        Map<String, DataStreamAlias> dataStreamsAliases = new HashMap<>();
        if (randomBoolean()) {
            for (int i = 0; i < randomIntBetween(1, 5); i++) {
                DataStreamAlias alias = DataStreamTestHelper.randomAliasInstance();
                dataStreamsAliases.put(alias.getName(), alias);
            }
        }
        return new DataStreamMetadata(dataStreams, dataStreamsAliases);
    }

    @Override
    protected DataStreamMetadata mutateInstance(DataStreamMetadata instance) throws IOException {
        return randomValueOtherThan(instance, this::createTestInstance);
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(Collections.singletonList(new NamedWriteableRegistry.Entry(DataStreamMetadata.class,
            DataStreamMetadata.TYPE, DataStreamMetadata::new)));
    }

    @Override
    protected Class<DataStreamMetadata> categoryClass() {
        return DataStreamMetadata.class;
    }
}
