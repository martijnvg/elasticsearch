/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.dataframe.transforms;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xpack.core.dataframe.transforms.pivot.PivotConfigTests;

import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;

import static org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformConfigTests.randomDataFrameTransformConfig;
import static org.elasticsearch.xpack.core.dataframe.transforms.DestConfigTests.randomDestConfig;
import static org.elasticsearch.xpack.core.dataframe.transforms.SourceConfigTests.randomSourceConfig;
import static org.hamcrest.Matchers.equalTo;

public class DataFrameTransformConfigUpdateTests extends AbstractSerializingDataFrameTestCase<DataFrameTransformConfigUpdate> {

    public static DataFrameTransformConfigUpdate randomDataFrameTransformConfigUpdate() {
        return new DataFrameTransformConfigUpdate(
            randomBoolean() ? null : randomSourceConfig(),
            randomBoolean() ? null : randomDestConfig(),
            randomBoolean() ? null : TimeValue.timeValueMillis(randomIntBetween(1_000, 3_600_000)),
            randomBoolean() ? null : randomSyncConfig(),
            randomBoolean() ? null : randomAlphaOfLengthBetween(1, 1000));
    }

    public static SyncConfig randomSyncConfig() {
        return TimeSyncConfigTests.randomTimeSyncConfig();
    }

    @Override
    protected DataFrameTransformConfigUpdate doParseInstance(XContentParser parser) throws IOException {
        return DataFrameTransformConfigUpdate.fromXContent(parser);
    }

    @Override
    protected DataFrameTransformConfigUpdate createTestInstance() {
        return randomDataFrameTransformConfigUpdate();
    }

    @Override
    protected Reader<DataFrameTransformConfigUpdate> instanceReader() {
        return DataFrameTransformConfigUpdate::new;
    }

    public void testIsNoop() {
        for (int i = 0; i < NUMBER_OF_TEST_RUNS; i++) {
            DataFrameTransformConfig config = randomDataFrameTransformConfig();
            DataFrameTransformConfigUpdate update = new DataFrameTransformConfigUpdate(null, null, null, null, null);
            assertTrue("null update is not noop", update.isNoop(config));
            update = new DataFrameTransformConfigUpdate(config.getSource(),
                config.getDestination(),
                config.getFrequency(),
                config.getSyncConfig(),
                config.getDescription());
            assertTrue("equal update is not noop", update.isNoop(config));

            update = new DataFrameTransformConfigUpdate(config.getSource(),
                config.getDestination(),
                config.getFrequency(),
                config.getSyncConfig(),
                "this is a new description");
            assertFalse("true update is noop", update.isNoop(config));
        }
    }

    public void testApply() {
        DataFrameTransformConfig config = new DataFrameTransformConfig("time-transform",
            randomSourceConfig(),
            randomDestConfig(),
            TimeValue.timeValueMillis(randomIntBetween(1_000, 3_600_000)),
            TimeSyncConfigTests.randomTimeSyncConfig(),
            Collections.singletonMap("key", "value"),
            PivotConfigTests.randomPivotConfig(),
            randomBoolean() ? null : randomAlphaOfLengthBetween(1, 1000),
            randomBoolean() ? null : Instant.now(),
            randomBoolean() ? null : Version.CURRENT.toString());
        DataFrameTransformConfigUpdate update = new DataFrameTransformConfigUpdate(null, null, null, null, null);

        assertThat(config, equalTo(update.apply(config)));
        SourceConfig sourceConfig = new SourceConfig("the_new_index");
        DestConfig destConfig = new DestConfig("the_new_dest", "my_new_pipeline");
        TimeValue frequency = TimeValue.timeValueSeconds(10);
        SyncConfig syncConfig = new TimeSyncConfig("time_field", TimeValue.timeValueSeconds(30));
        String newDescription = "new description";
        update = new DataFrameTransformConfigUpdate(sourceConfig, destConfig, frequency, syncConfig, newDescription);

        Map<String, String> headers = Collections.singletonMap("foo", "bar");
        update.setHeaders(headers);
        DataFrameTransformConfig updatedConfig = update.apply(config);

        assertThat(updatedConfig.getSource(), equalTo(sourceConfig));
        assertThat(updatedConfig.getDestination(), equalTo(destConfig));
        assertThat(updatedConfig.getFrequency(), equalTo(frequency));
        assertThat(updatedConfig.getSyncConfig(), equalTo(syncConfig));
        assertThat(updatedConfig.getDescription(), equalTo(newDescription));
        assertThat(updatedConfig.getHeaders(), equalTo(headers));
    }

    public void testApplyWithSyncChange() {
        DataFrameTransformConfig batchConfig = new DataFrameTransformConfig("batch-transform",
            randomSourceConfig(),
            randomDestConfig(),
            TimeValue.timeValueMillis(randomIntBetween(1_000, 3_600_000)),
            null,
            null,
            PivotConfigTests.randomPivotConfig(),
            randomBoolean() ? null : randomAlphaOfLengthBetween(1, 1000),
            randomBoolean() ? null : Instant.now(),
            randomBoolean() ? null : Version.CURRENT.toString());

        DataFrameTransformConfigUpdate update = new DataFrameTransformConfigUpdate(null,
            null,
            null,
            TimeSyncConfigTests.randomTimeSyncConfig(),
            null);

        ElasticsearchStatusException ex = expectThrows(ElasticsearchStatusException.class, () -> update.apply(batchConfig));
        assertThat(ex.getMessage(),
            equalTo("Cannot change the current sync configuration of transform [batch-transform] from [null] to [time]"));

        DataFrameTransformConfig timeSyncedConfig = new DataFrameTransformConfig("time-transform",
            randomSourceConfig(),
            randomDestConfig(),
            TimeValue.timeValueMillis(randomIntBetween(1_000, 3_600_000)),
            TimeSyncConfigTests.randomTimeSyncConfig(),
            null,
            PivotConfigTests.randomPivotConfig(),
            randomBoolean() ? null : randomAlphaOfLengthBetween(1, 1000),
            randomBoolean() ? null : Instant.now(),
            randomBoolean() ? null : Version.CURRENT.toString());

        DataFrameTransformConfigUpdate fooSyncUpdate = new DataFrameTransformConfigUpdate(null,
            null,
            null,
            new FooSync(),
            null);
        ex = expectThrows(ElasticsearchStatusException.class, () -> fooSyncUpdate.apply(timeSyncedConfig));
        assertThat(ex.getMessage(),
            equalTo("Cannot change the current sync configuration of transform [time-transform] from [time] to [foo]"));

    }

    static class FooSync implements SyncConfig {

        @Override
        public boolean isValid() {
            return true;
        }

        @Override
        public QueryBuilder getRangeQuery(DataFrameTransformCheckpoint newCheckpoint) {
            return null;
        }

        @Override
        public QueryBuilder getRangeQuery(DataFrameTransformCheckpoint oldCheckpoint, DataFrameTransformCheckpoint newCheckpoint) {
            return null;
        }

        @Override
        public String getWriteableName() {
            return "foo";
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return null;
        }
    }
}
