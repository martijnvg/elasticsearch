/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.search.aggregations;

import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.benchmark.internal.BenchmarkLogging;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.columnar.ColumNARDocValuesFormat;
import org.elasticsearch.columnar.ColumnarFieldType;
import org.elasticsearch.columnar.string.StringBinaryPayload;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.codec.Elasticsearch96Codec;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.fielddata.IndexOrdinalsFieldData;
import org.elasticsearch.index.fielddata.plain.ColumnarKeywordIndexFieldData;
import org.elasticsearch.index.mapper.BinaryDocValuesFormat;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.script.field.KeywordDocValuesField;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Builds global ordinals for a columnar keyword field spread over several segments: what the first search after a
 * refresh pays before a {@code terms} aggregation can collect by global ordinal. That is the ordinals of the segments
 * whose values have to be read, because they escape the dictionary or have none, and the ordinal map across segments.
 * Nothing is cached between rounds, so it is the whole cost, which overstates a refresh that adds one segment to many.
 * Collecting with the global ordinals once built is measured by {@link ColumnarTermsAggregatorBenchmark}.
 *
 * <p>It runs as a plain program through {@link LatencyHarness} rather than JMH, so each index is built once. Run it with
 * {@code ./gradlew -p benchmarks runMain -PbenchmarkClass=<this class> --args 'name=value[,value...] ...'}. Before timing,
 * it checks that the ordinal map holds every value indexed.
 */
public class ColumnarGlobalOrdinalsBenchmark implements Closeable {

    static {
        BenchmarkLogging.configure();
    }

    private static final String FIELD = "kw";

    /**
     * The parameters and their defaults.
     * <ul>
     *     <li>{@code distinctValues}: distinct values drawn from, not counting the ones held once.</li>
     *     <li>{@code segments}: how many segments the documents are spread over.</li>
     *     <li>{@code uniquePercent}: percent of the documents holding a value no other document holds, which escapes a
     *     segment's dictionary, so with it above zero every segment has its ordinals worked out.</li>
     * </ul>
     */
    private static final Map<String, String> DEFAULTS = Map.of(
        "distinctValues",
        "5,100,1000,5000",
        "segments",
        "1,10,50",
        "uniquePercent",
        "0,1",
        "docCount",
        "10000000"
    );

    private final Path path;
    private final Directory directory;
    private final DirectoryReader reader;
    private final ColumnarKeywordIndexFieldData fieldData;

    /** Results are added here so the JIT can't drop the work that produced them. */
    private static long sink;

    public static void main(String[] args) throws IOException {
        final LatencyHarness harness = new LatencyHarness(args, DEFAULTS);
        final int docCount = harness.intValue("docCount");
        for (String distinctValues : harness.values("distinctValues")) {
            for (String segments : harness.values("segments")) {
                for (String uniquePercent : harness.values("uniquePercent")) {
                    final String configuration = "distinctValues="
                        + distinctValues
                        + " segments="
                        + segments
                        + " uniquePercent="
                        + uniquePercent
                        + " docCount="
                        + docCount;
                    final long start = System.nanoTime();
                    try (
                        ColumnarGlobalOrdinalsBenchmark benchmark = new ColumnarGlobalOrdinalsBenchmark(
                            Integer.parseInt(distinctValues),
                            Integer.parseInt(segments),
                            Integer.parseInt(uniquePercent),
                            docCount
                        )
                    ) {
                        System.out.println(
                            configuration + ": indexed in " + TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - start) + "s"
                        );
                        final long[] millis = harness.measure(
                            configuration,
                            () -> sink += benchmark.buildGlobalOrdinals().getOrdinalMap().getValueCount()
                        );
                        LatencyHarness.report(configuration, millis);
                    }
                }
            }
        }
        System.out.println("sink " + sink);
    }

    /** Indexes {@code docCount} documents into {@code segments} segments of the same size. */
    private ColumnarGlobalOrdinalsBenchmark(int distinctValues, int segments, int uniquePercent, int docCount) throws IOException {
        path = Files.createTempDirectory("columnar-global-ordinals");
        directory = new MMapDirectory(path);
        final Random random = new Random(7);
        final FieldType type = new FieldType();
        type.setDocValuesType(DocValuesType.BINARY);
        type.freeze();
        final DocValuesFormat columnar = new ColumNARDocValuesFormat(field -> ColumnarFieldType.STRING);
        final int perSegment = Math.ceilDiv(docCount, segments);
        final IndexWriterConfig config = new IndexWriterConfig().setCodec(new Elasticsearch96Codec() {
            @Override
            public DocValuesFormat getDocValuesFormatForField(String field) {
                return columnar;
            }
        }).setUseCompoundFile(false).setMergePolicy(NoMergePolicy.INSTANCE);
        // Flushed by hand, so each segment holds the same share of the documents; no buffer limit is reached first.
        config.setMaxBufferedDocs(perSegment + 1).setRAMBufferSizeMB(IndexWriterConfig.DISABLE_AUTO_FLUSH).setRAMPerThreadHardLimitMB(2047);
        final Set<BytesRef> indexed = new HashSet<>();
        try (IndexWriter writer = new IndexWriter(directory, config)) {
            for (int i = 0; i < docCount; i++) {
                // A document's one value: drawn with a long tail, or with uniquePercent one of its own.
                final BytesRef value = random.nextInt(100) < uniquePercent
                    ? new BytesRef("unique-" + i)
                    : new BytesRef("value-" + (int) (distinctValues * Math.pow(random.nextDouble(), 2.0)));
                indexed.add(value);
                final Document doc = new Document();
                doc.add(new Field(FIELD, BytesRef.deepCopyOf(new StringBinaryPayload.Builder().encode(List.of(value))), type));
                writer.addDocument(doc);
                if ((i + 1) % perSegment == 0) {
                    writer.flush();
                }
            }
        }
        reader = DirectoryReader.open(directory);
        if (reader.leaves().size() != segments) {
            throw new IllegalStateException("expected " + segments + " segments, found " + reader.leaves().size());
        }

        fieldData = new ColumnarKeywordIndexFieldData.Builder(
            FIELD,
            CoreValuesSourceType.KEYWORD,
            KeywordDocValuesField::new,
            IndexVersion.current(),
            indexSettings(),
            BinaryDocValuesFormat.COLUMNAR_PAYLOAD
        ).build(new IndexFieldDataCache.None(), new NoneCircuitBreakerService());

        // Global ordinals that miss a value, or count one twice, aren't worth timing.
        final long valueCount = buildGlobalOrdinals().getOrdinalMap().getValueCount();
        if (valueCount != indexed.size()) {
            throw new IllegalStateException("expected " + indexed.size() + " global ordinals, built " + valueCount);
        }
    }

    private static IndexSettings indexSettings() {
        final Settings settings = Settings.builder()
            .put("index.number_of_replicas", 0)
            .put("index.number_of_shards", 1)
            .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
            .put(IndexSettings.COLUMNAR_GLOBAL_ORDINALS_ENABLED_SETTING.getKey(), true)
            .put(IndexSettings.COLUMNAR_GLOBAL_ORDINALS_MAX_MATERIALIZED_VALUES_SETTING.getKey(), Integer.MAX_VALUE)
            .build();
        return new IndexSettings(IndexMetadata.builder("index").settings(settings).build(), settings);
    }

    @Override
    public void close() throws IOException {
        IOUtils.close(reader, directory);
        IOUtils.rm(path);
    }

    private IndexOrdinalsFieldData buildGlobalOrdinals() {
        final IndexFieldData<?> forReader = fieldData.forReader(reader);
        if (forReader instanceof IndexOrdinalsFieldData ordinals) {
            try {
                return ordinals.loadGlobalDirect(reader);
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        }
        throw new IllegalStateException("the field offers no ordinals for this reader");
    }

}
