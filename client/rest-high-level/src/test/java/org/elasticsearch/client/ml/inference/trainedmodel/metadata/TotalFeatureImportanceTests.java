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
package org.elasticsearch.client.ml.inference.trainedmodel.metadata;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class TotalFeatureImportanceTests extends AbstractXContentTestCase<TotalFeatureImportance> {


    public static TotalFeatureImportance randomInstance() {
        return new TotalFeatureImportance(
            randomAlphaOfLength(10),
            randomBoolean() ? null : randomImportance(),
            randomBoolean() ?
                null :
                Stream.generate(() -> new TotalFeatureImportance.ClassImportance(randomAlphaOfLength(10), randomImportance()))
                    .limit(randomIntBetween(1, 10))
                    .collect(Collectors.toList())
            );
    }

    private static TotalFeatureImportance.Importance randomImportance() {
        return new TotalFeatureImportance.Importance(randomDouble(), randomDouble(), randomDouble());
    }

    @Override
    protected TotalFeatureImportance createTestInstance() {
        return randomInstance();
    }

    @Override
    protected TotalFeatureImportance doParseInstance(XContentParser parser) throws IOException {
        return TotalFeatureImportance.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

}
