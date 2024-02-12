/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.DocVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.AnyOperatorTestCase;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.OperatorTestCase;
import org.elasticsearch.compute.operator.TestResultPageSinkOperator;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.index.mapper.DataStreamTimestampFieldMapper;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.mapper.TimeSeriesIdFieldMapper;
import org.junit.After;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import static org.elasticsearch.compute.lucene.TimeSeriesSourceOperatorTests.writeTS;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class TimeSeriesLastSampleOperatorTests extends AnyOperatorTestCase {

    private IndexReader reader;
    private final Directory directory = newDirectory();

    @After
    public void cleanup() throws IOException {
        IOUtils.close(reader, directory);
    }

    public void testSimple() {
        int numTimeSeries = 3;
        int numSamplesPerTS = 10;
        var ctx = driverContext();
        long timestampStart = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis("2024-01-01T00:00:00Z");
        var timeSeriesFactory = createSourceOperator(writer -> {
            long timestamp = timestampStart;
            for (int i = 0; i < numSamplesPerTS; i++) {
                for (int j = 0; j < numTimeSeries; j++) {
                    String hostname = String.format("host-%02d", j);
                    writeTS(writer, timestamp, new Object[] { "hostname", hostname }, new Object[] { "voltage", j + 5 });
                }
                timestamp += 10_000;
                writer.commit();
            }
            return numTimeSeries * numSamplesPerTS;
        });

        List<Page> results = new ArrayList<>();
        var hostnameField = new KeywordFieldMapper.KeywordFieldType("hostname");
        var timestampField = new DateFieldMapper.DateFieldType("@timestamp", DateFieldMapper.Resolution.MILLISECONDS);
        var voltageField = new NumberFieldMapper.NumberFieldType("voltage", NumberFieldMapper.NumberType.LONG);
        OperatorTestCase.runDriver(
            new Driver(
                ctx,
                timeSeriesFactory.get(ctx),
                List.of(
                    ValuesSourceReaderOperatorTests.factory(reader, hostnameField, ElementType.BYTES_REF).get(ctx),
                    ValuesSourceReaderOperatorTests.factory(reader, timestampField, ElementType.LONG).get(ctx),
                    ValuesSourceReaderOperatorTests.factory(reader, voltageField, ElementType.LONG).get(ctx)
                ),
                new TestResultPageSinkOperator(results::add),
                () -> {}
            )
        );
        OperatorTestCase.assertDriverContext(ctx);
        assertThat(results, hasSize(1));
        Page page = results.get(0);
        assertThat(page.getBlockCount(), equalTo(4));
        assertThat(page.getPositionCount(), equalTo(numTimeSeries));

        DocVector docVector = (DocVector) page.getBlock(0).asVector();
        assertThat(docVector.getPositionCount(), equalTo(numTimeSeries));

        BytesRefVector hostnameVector = (BytesRefVector) page.getBlock(1).asVector();
        assertThat(hostnameVector.getPositionCount(), equalTo(numTimeSeries));

        LongVector timestampVector = (LongVector) page.getBlock(2).asVector();
        assertThat(timestampVector.getPositionCount(), equalTo(numTimeSeries));

        LongVector voltageVector = (LongVector) page.getBlock(3).asVector();
        assertThat(voltageVector.getPositionCount(), equalTo(numTimeSeries));

        long expectedTimestamp = timestampStart + ((numSamplesPerTS - 1) * 10_000);
        assertThat(docVector.shards().getInt(0), equalTo(0));
        assertThat(hostnameVector.getBytesRef(0, new BytesRef()).utf8ToString(), equalTo("host-00"));
        assertThat(timestampVector.getLong(0), equalTo(expectedTimestamp));
        assertThat(voltageVector.getLong(0), equalTo(5L));

        assertThat(docVector.shards().getInt(1), equalTo(0));
        assertThat(hostnameVector.getBytesRef(1, new BytesRef()).utf8ToString(), equalTo("host-01"));
        assertThat(timestampVector.getLong(1), equalTo(expectedTimestamp));
        assertThat(voltageVector.getLong(1), equalTo(6L));

        assertThat(docVector.shards().getInt(2), equalTo(0));
        assertThat(hostnameVector.getBytesRef(2, new BytesRef()).utf8ToString(), equalTo("host-02"));
        assertThat(timestampVector.getLong(2), equalTo(expectedTimestamp));
        assertThat(voltageVector.getLong(2), equalTo(7L));
    }

    @Override
    protected Operator.OperatorFactory simple() {
        return createSourceOperator(writer -> {
            long timestamp = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis("2024-01-01T00:00:00Z");
            writeTS(writer, timestamp, new Object[] { "hostname", "host-01" }, new Object[] { "voltage", 2 });
            return 1;
        });
    }

    protected String expectedDescriptionOfSimple() {
        return "TimeSeriesLastSampleOperatorFactory[maxPageSize = 1, limit = 1]";
    }

    @Override
    protected String expectedToStringOfSimple() {
        return "Impl[maxPageSize=1, remainingDocs=1]";
    }

    TimeSeriesLastSampleOperatorFactory createSourceOperator(
        CheckedFunction<RandomIndexWriter, Integer, IOException> indexingLogic
    ) {
        int numDocs;
        Sort sort = new Sort(
            new SortField(TimeSeriesIdFieldMapper.NAME, SortField.Type.STRING, false),
            new SortedNumericSortField(DataStreamTimestampFieldMapper.DEFAULT_PATH, SortField.Type.LONG, true)
        );
        try (
            RandomIndexWriter writer = new RandomIndexWriter(
                random(),
                directory,
                newIndexWriterConfig().setIndexSort(sort).setMergePolicy(NoMergePolicy.INSTANCE)
            )
        ) {

            numDocs = indexingLogic.apply(writer);
            writer.forceMerge(1);
            reader = writer.getReader();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        var ctx = new LuceneSourceOperatorTests.MockShardContext(reader, 0);
        Function<ShardContext, Query> queryFunction = c -> new MatchAllDocsQuery();
        // int maxPageSize = between(10, Math.max(10, numDocs));
        return TimeSeriesLastSampleOperatorFactory.create(numDocs, numDocs, 1, List.of(ctx), queryFunction);
    }
}
