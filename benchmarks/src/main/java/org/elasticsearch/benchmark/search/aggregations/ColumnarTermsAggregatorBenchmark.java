/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.search.aggregations;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.LogDocMergePolicy;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.SerialMergeScheduler;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.benchmark.index.mapper.MapperServiceFactory;
import org.elasticsearch.benchmark.internal.BenchmarkLogging;
import org.elasticsearch.columnar.ColumNARDocValuesFormat;
import org.elasticsearch.columnar.ColumnarFieldType;
import org.elasticsearch.columnar.string.StringBinaryPayload;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.MemorySizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.MergePolicyConfig;
import org.elasticsearch.index.analysis.NameOrDefinition;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.cache.bitset.BitsetFilterCache;
import org.elasticsearch.index.codec.Elasticsearch96Codec;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.NestedLookup;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.mapper.NumberFieldMapper.NumberType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.support.NestedScope;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.indices.fielddata.cache.IndicesFieldDataCache;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.Sum;
import org.elasticsearch.search.aggregations.metrics.SumAggregationBuilder;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSourceRegistry;
import org.elasticsearch.search.internal.SubSearchContext;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.search.sort.BucketedSort;
import org.elasticsearch.search.sort.BucketedSort.ExtraData;
import org.elasticsearch.search.sort.SortAndFormats;
import org.elasticsearch.search.sort.SortBuilder;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * Runs a whole {@code terms} aggregation over a columnar keyword field, as {@code TermsAggregatorFactory} builds it,
 * collection and result building both, so reading the field by value can be weighed against reading it through global
 * ordinals with the aggregators that actually run. {@code map} is the {@code map} execution hint, which reads the
 * column's dictionary ordinals a segment at a time; {@code global_ordinals} needs
 * {@link IndexSettings#COLUMNAR_GLOBAL_ORDINALS_ENABLED_SETTING}, which is on here.
 *
 * <p>It runs as a plain program through {@link LatencyHarness} rather than JMH, so each index is built once and every
 * aggregation is run against it: {@code docValues}, {@code distinctValues}, {@code indexBuffer}, {@code commitEvery}
 * and {@code segments} shape the index, the other parameters the aggregation. Run it with
 * {@code ./gradlew -p benchmarks runMain -PbenchmarkClass=<this class> --args 'name=value[,value...] ...'}.
 *
 * <p>The field data cache is a real one that lives as long as an index, so global ordinals are built by the first
 * aggregation, which is a warmup round, and served from the cache after, as they are for every search but the first
 * after a refresh. Building them is measured by {@link ColumnarGlobalOrdinalsBenchmark}. Before timing an aggregation,
 * it checks that both hints give the same result, and prints which aggregator each one ran as.
 *
 * <p>Segments come out of indexing as they would on a logsdb shard rather than being sized by hand: the writer flushes
 * when it commits or its indexing buffer fills, and merges adjacent segments by document count, as many at once as a
 * time-based index merges by default. That mixes freshly flushed segments with merged ones, which get their ordinals
 * differently. Indexing stops at the commit that leaves {@code segments} segments, so how many documents an index holds
 * follows from how they were flushed and merged. The layout is printed once an index is built.
 */
public class ColumnarTermsAggregatorBenchmark implements Closeable {

    static {
        BenchmarkLogging.configure();
    }

    private static final String FIELD = "kw";
    private static final String NUMBER = "n";
    private static final String SHARE = "share";

    /**
     * The parameters and their defaults.
     * <ul>
     *     <li>{@code docValues}: {@code columnar} stores the field as a columnar keyword; {@code sorted_set} stores it as a
     *     keyword in a standard index, as a baseline for changes to the aggregators that both use.</li>
     *     <li>{@code distinctValues}: distinct values drawn from, with a long tail.</li>
     *     <li>{@code indexBuffer}: the indexing buffer the writer flushes a segment at, as a size or a share of the heap,
     *     like {@code indices.memory.index_buffer_size}, whose default it takes: everything a node buffers for indexing
     *     goes to the one shard that is indexing.</li>
     *     <li>{@code commitEvery}: documents indexed between commits, each of which flushes what is buffered as a
     *     segment, as the periodic flushes and refreshes of a shard do while documents arrive.</li>
     *     <li>{@code segments}: how many segments the index has when indexing stops, checked after every commit.</li>
     *     <li>{@code percent}: percent of the documents the query matches.</li>
     *     <li>{@code subAggregation}: whether each bucket has a {@code sum} sub-aggregation, which rules out the low
     *     cardinality aggregator.</li>
     *     <li>{@code collectMode}: how a sub-aggregation is collected: {@code default} lets the factory choose, as a
     *     search does; the others force {@code depth_first} or {@code breadth_first}, which records every document's
     *     bucket and replays the top ones.</li>
     *     <li>{@code order}: {@code count}, or {@code sum}, by the sub-aggregation, which collects it as documents are
     *     found and makes global ordinals map to buckets by hash. Skipped without a sub-aggregation.</li>
     *     <li>{@code executionHint}: {@code map} or {@code global_ordinals}, timed one after the other on the same
     *     index.</li>
     *     <li>{@code warmup}: untimed runs of each query before the timed ones. One rather than the harness's ten: each
     *     execution hint runs the same aggregation code across many queries in one JVM, so it is compiled after the
     *     first few queries, and the first run of each one loads what the field data cache keeps, global ordinals.</li>
     *     <li>{@code iterations}: timed runs of each query, after the warmup runs. Ten rather than the harness's thirty,
     *     since a round reads every matching document of millions.</li>
     * </ul>
     */
    private static final Map<String, String> DEFAULTS = Map.ofEntries(
        Map.entry("docValues", "columnar"),
        Map.entry("distinctValues", "100,1000,5000"),
        Map.entry("indexBuffer", "10%"),
        Map.entry("commitEvery", "100000"),
        Map.entry("segments", "50"),
        Map.entry("percent", "10,100"),
        Map.entry("subAggregation", "false,true"),
        Map.entry("collectMode", "default"),
        Map.entry("order", "count"),
        Map.entry("executionHint", "map,global_ordinals"),
        Map.entry("warmup", "1"),
        Map.entry("iterations", "10")
    );

    private final SearchModule searchModule = new SearchModule(Settings.EMPTY, List.of());
    private final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
    private final CircuitBreakerService breakerService = new NoneCircuitBreakerService();
    private final BigArrays bigArrays = new BigArrays(new PageCacheRecycler(Settings.EMPTY), breakerService, "request");
    private final IndicesFieldDataCache indicesFieldDataCache = new IndicesFieldDataCache(
        Settings.EMPTY,
        new IndexFieldDataCache.Listener() {}
    );

    private final Path path;
    private final Directory directory;
    private final DirectoryReader reader;
    private final IndexSearcher searcher;
    private final MapperService mapperService;
    private final MappedFieldType keyword;
    private final IndexFieldDataCache fieldDataCache;

    /** Results are added here so the JIT can't drop the work that produced them. */
    private static long sink;

    public static void main(String[] args) throws IOException {
        final LatencyHarness harness = new LatencyHarness(args, DEFAULTS);
        final int segments = harness.intValue("segments");
        final int commitEvery = harness.intValue("commitEvery");
        for (String docValues : harness.values("docValues")) {
            for (String distinctValues : harness.values("distinctValues")) {
                for (String indexBuffer : harness.values("indexBuffer")) {
                    final String index = "docValues="
                        + docValues
                        + " distinctValues="
                        + distinctValues
                        + " indexBuffer="
                        + indexBuffer
                        + " commitEvery="
                        + commitEvery
                        + " segments="
                        + segments;
                    final long start = System.nanoTime();
                    try (
                        ColumnarTermsAggregatorBenchmark benchmark = new ColumnarTermsAggregatorBenchmark(
                            docValues,
                            Integer.parseInt(distinctValues),
                            MemorySizeValue.parseBytesSizeValueOrHeapRatio(indexBuffer, "indexBuffer"),
                            commitEvery,
                            segments
                        )
                    ) {
                        System.out.println(
                            index
                                + ": indexed in "
                                + TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - start)
                                + "s into "
                                + benchmark.segments()
                        );
                        benchmark.run(harness, index);
                    }
                }
            }
        }
        System.out.println("sink " + sink);
    }

    /** Times every aggregation the parameters describe against this index. */
    private void run(LatencyHarness harness, String index) throws IOException {
        for (String percent : harness.values("percent")) {
            for (String subAggregation : harness.values("subAggregation")) {
                for (String collectMode : harness.values("collectMode")) {
                    for (String order : harness.values("order")) {
                        final Request request = new Request(
                            Integer.parseInt(percent),
                            Boolean.parseBoolean(subAggregation),
                            collectMode,
                            order
                        );
                        final String configuration = index + " " + request;
                        if (order.equals("sum") && request.subAggregation == false) {
                            System.out.println(configuration + ": skipped, ordering by sum needs the sum sub-aggregation");
                            continue;
                        }
                        // Both hints have to agree before either is worth timing.
                        final Outcome map = aggregate(request, "map");
                        final Outcome global = aggregate(request, "global_ordinals");
                        if (request.buckets(map.result).equals(request.buckets(global.result)) == false) {
                            throw new IllegalStateException(
                                "map and global ordinals differ:\n" + request.buckets(map.result) + "\n" + request.buckets(global.result)
                            );
                        }
                        System.out.println(
                            configuration + ": map ran as " + map.aggregator + ", global_ordinals ran as " + global.aggregator
                        );
                        for (String hint : harness.values("executionHint")) {
                            final String measured = configuration + " executionHint=" + hint;
                            final long[] millis = harness.measure(measured, () -> sink += aggregate(request, hint).result.hashCode());
                            LatencyHarness.report(measured, millis);
                        }
                    }
                }
            }
        }
    }

    /**
     * Indexes documents until a commit leaves {@code segments} segments, committing every {@code commitEvery} and
     * flushing whenever {@code indexBuffer} fills.
     */
    private ColumnarTermsAggregatorBenchmark(String docValues, int distinctValues, ByteSizeValue indexBuffer, int commitEvery, int segments)
        throws IOException {
        if (segments < 1) {
            throw new IllegalArgumentException("[segments] must be at least 1 but was [" + segments + "]");
        }
        final boolean columnarField = switch (docValues) {
            case "columnar" -> true;
            case "sorted_set" -> false;
            default -> throw new IllegalArgumentException("unknown doc values [" + docValues + "]");
        };
        final Settings.Builder indexSettings = Settings.builder();
        if (columnarField) {
            indexSettings.put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR)
                .put(IndexSettings.COLUMNAR_GLOBAL_ORDINALS_ENABLED_SETTING.getKey(), true);
        }
        // The baseline isn't indexed, so it runs the same aggregators as the columnar field rather than a filter per term.
        final String mapping = columnarField ? """
            { "_doc": { "properties": { "kw": { "type": "keyword" } } } }""" : """
            { "_doc": { "properties": { "kw": { "type": "keyword", "index": false } } } }""";
        mapperService = MapperServiceFactory.create(mapping, List.of(), indexSettings.build());
        keyword = mapperService.fieldType(FIELD);
        final KeywordFieldMapper.KeywordFieldType.DocValuesDiskFormat format = ((KeywordFieldMapper.KeywordFieldType) keyword).diskFormat();
        if (format.isColumnar() != columnarField) {
            throw new IllegalStateException("expected " + docValues + " doc values, got " + format);
        }
        fieldDataCache = indicesFieldDataCache.buildIndexFieldDataCache(
            new IndexFieldDataCache.Listener() {},
            mapperService.getIndexSettings().getIndex(),
            FIELD
        );

        path = Files.createTempDirectory("columnar-terms-aggregator");
        directory = new MMapDirectory(path);
        final Random random = new Random(7);
        final FieldType type = new FieldType();
        type.setDocValuesType(DocValuesType.BINARY);
        type.freeze();
        final DocValuesFormat columnar = new ColumNARDocValuesFormat(field -> ColumnarFieldType.STRING);
        final IndexWriterConfig config = new IndexWriterConfig().setCodec(new Elasticsearch96Codec() {
            @Override
            public DocValuesFormat getDocValuesFormatForField(String field) {
                return columnarField && field.equals(FIELD) ? columnar : super.getDocValuesFormatForField(field);
            }
        })
            .setMergePolicy(logDocMergePolicy())
            // Merging in the indexing thread rather than beside it, so the same parameters give the same segments every run.
            .setMergeScheduler(new SerialMergeScheduler())
            .setRAMBufferSizeMB(indexBuffer.getMbFrac());
        try (IndexWriter writer = new IndexWriter(directory, config)) {
            for (long i = 0;; i++) {
                final Document doc = new Document();
                final BytesRef value = new BytesRef("value-" + (int) (distinctValues * Math.pow(random.nextDouble(), 2.0)));
                if (columnarField) {
                    doc.add(new Field(FIELD, BytesRef.deepCopyOf(new StringBinaryPayload.Builder().encode(List.of(value))), type));
                } else {
                    doc.add(new SortedSetDocValuesField(FIELD, value));
                }
                doc.add(new SortedNumericDocValuesField(NUMBER, random.nextInt(1000)));
                // A share of 0 to 99, so one index serves every percent: a query for a percent matches the shares below it.
                doc.add(new IntPoint(SHARE, random.nextInt(100)));
                writer.addDocument(doc);
                if ((i + 1) % commitEvery == 0) {
                    writer.commit();
                    // Counted on the writer rather than the commit: a commit is written before the merges it sets off.
                    if (liveSegments(writer) >= segments) {
                        break;
                    }
                }
            }
        }
        // The field data cache keys global ordinals on the reader's shard, as it does in a search.
        reader = ElasticsearchDirectoryReader.wrap(DirectoryReader.open(directory), new ShardId("index", "_na_", 0));
        searcher = new IndexSearcher(reader);
        // A cached result would skip the scorer being measured.
        searcher.setQueryCache(null);
    }

    /** The segments the writer has now, merges it ran after its last commit included. */
    private static int liveSegments(IndexWriter writer) throws IOException {
        try (DirectoryReader live = DirectoryReader.open(writer)) {
            return live.leaves().size();
        }
    }

    /**
     * Merges only adjacent segments, like the {@code LogByteSizeMergePolicy} a logsdb index gets, but sizes them by their
     * documents rather than their bytes, so segments of the two doc values formats merge alike although their bytes differ.
     * It merges as many segments at once as a time-based index does by default.
     */
    private static LogDocMergePolicy logDocMergePolicy() {
        final LogDocMergePolicy mergePolicy = new LogDocMergePolicy();
        mergePolicy.setMergeFactor(MergePolicyConfig.DEFAULT_MERGE_FACTOR);
        return mergePolicy;
    }

    /** The segments indexing left, in order, with how many documents each holds and whether it was flushed or merged. */
    private String segments() {
        final List<String> segments = new ArrayList<>();
        for (LeafReaderContext leaf : reader.leaves()) {
            final SegmentReader segment = Lucene.segmentReader(leaf.reader());
            segments.add(segment.maxDoc() + " " + segment.getSegmentInfo().info.getDiagnostics().get("source"));
        }
        return segments.size() + " segments " + segments;
    }

    @Override
    public void close() throws IOException {
        indicesFieldDataCache.close();
        IOUtils.close(reader, directory, mapperService);
        IOUtils.rm(path);
    }

    /** The aggregation to run, apart from its execution hint. */
    private record Request(int percent, boolean subAggregation, String collectMode, String order) {
        Query query() {
            return percent == 100 ? new MatchAllDocsQuery() : IntPoint.newRangeQuery(SHARE, 0, percent - 1);
        }

        TermsAggregationBuilder builder(String hint) {
            final TermsAggregationBuilder terms = new TermsAggregationBuilder("t").field(FIELD).size(10).executionHint(hint);
            if (subAggregation) {
                terms.subAggregation(new SumAggregationBuilder("s").field(NUMBER));
            }
            if (order.equals("sum")) {
                terms.order(BucketOrder.aggregation("s", false));
            }
            if (collectMode.equals("default") == false) {
                terms.collectMode(Aggregator.SubAggCollectionMode.parse(collectMode, LoggingDeprecationHandler.INSTANCE));
            }
            return terms;
        }

        /** The buckets of a result, with their counts and sums, as text that two results can be compared by. */
        String buckets(InternalAggregation result) {
            final StringBuilder buckets = new StringBuilder();
            for (StringTerms.Bucket bucket : ((StringTerms) result).getBuckets()) {
                buckets.append(bucket.getKeyAsString()).append('=').append(bucket.getDocCount());
                if (subAggregation) {
                    buckets.append('/').append(((Sum) bucket.getAggregations().get("s")).value());
                }
                buckets.append('\n');
            }
            return buckets.toString();
        }

        @Override
        public String toString() {
            return "percent=" + percent + " subAggregation=" + subAggregation + " collectMode=" + collectMode + " order=" + order;
        }
    }

    private record Outcome(String aggregator, InternalAggregation result) {}

    /** Runs the aggregation once: builds it as a search does, collects every matching document, and builds its result. */
    private Outcome aggregate(Request request, String hint) throws IOException {
        final Query query = request.query();
        try (BenchmarkAggregationContext context = new BenchmarkAggregationContext(query)) {
            final Aggregator aggregator = new AggregatorFactories.Builder().addAggregator(request.builder(hint))
                .build(context, null)
                .createTopLevelAggregators()[0];
            aggregator.preCollection();
            searcher.search(query, new CollectorManager<Collector, Void>() {
                private boolean handedOut;

                @Override
                public Collector newCollector() {
                    if (handedOut) {
                        throw new IllegalStateException("the search was expected to run in one slice");
                    }
                    handedOut = true;
                    return aggregator.asCollector();
                }

                @Override
                public Void reduce(Collection<Collector> collectors) {
                    return null;
                }
            });
            aggregator.postCollection();
            return new Outcome(aggregator.getClass().getSimpleName(), aggregator.buildTopLevel());
        }
    }

    /** What a search hands its aggregations, reduced to what a {@code terms} and a {@code sum} aggregation over two fields use. */
    private class BenchmarkAggregationContext extends AggregationContext {
        private final List<Releasable> releaseMe = new ArrayList<>();
        private final Query query;

        BenchmarkAggregationContext(Query query) {
            this.query = query;
        }

        @Override
        public Query query() {
            return query;
        }

        @Override
        public Aggregator profileIfEnabled(Aggregator agg) {
            return agg;
        }

        @Override
        public boolean profiling() {
            return false;
        }

        @Override
        public long nowInMillis() {
            return 0;
        }

        @Override
        public Analyzer getNamedAnalyzer(String analyzer) {
            return null;
        }

        @Override
        public Analyzer buildCustomAnalyzer(
            IndexSettings indexSettings,
            boolean normalizer,
            NameOrDefinition tokenizer,
            List<NameOrDefinition> charFilters,
            List<NameOrDefinition> tokenFilters
        ) {
            return null;
        }

        @Override
        protected IndexFieldData<?> buildFieldData(MappedFieldType ft) {
            final FieldDataContext fieldDataContext = new FieldDataContext("index", mapperService.getIndexSettings(), () -> {
                throw new UnsupportedOperationException();
            }, Set::of, () -> false, MappedFieldType.FielddataOperation.SEARCH);
            return ft.fielddataBuilder(fieldDataContext).build(fieldDataCache, breakerService);
        }

        @Override
        public MappedFieldType getFieldType(String path) {
            return switch (path) {
                case FIELD -> keyword;
                case NUMBER -> new NumberFieldMapper.NumberFieldType(NUMBER, NumberType.LONG);
                default -> throw new UnsupportedOperationException(path);
            };
        }

        @Override
        public Set<String> getMatchingFieldNames(String pattern) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <FactoryType> FactoryType compile(Script script, ScriptContext<FactoryType> context) {
            throw new UnsupportedOperationException();
        }

        @Override
        public SearchLookup lookup() {
            throw new UnsupportedOperationException();
        }

        @Override
        public ValuesSourceRegistry getValuesSourceRegistry() {
            return searchModule.getValuesSourceRegistry();
        }

        @Override
        public BigArrays bigArrays() {
            return bigArrays;
        }

        @Override
        public IndexSearcher searcher() {
            return searcher;
        }

        @Override
        public Query buildQuery(QueryBuilder builder) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Query filterQuery(Query query) {
            return query;
        }

        @Override
        public IndexSettings getIndexSettings() {
            return mapperService.getIndexSettings();
        }

        @Override
        public ClusterSettings getClusterSettings() {
            return clusterSettings;
        }

        @Override
        public Optional<SortAndFormats> buildSort(List<SortBuilder<?>> sortBuilders) {
            throw new UnsupportedOperationException();
        }

        @Override
        public NestedLookup nestedLookup() {
            return NestedLookup.EMPTY;
        }

        @Override
        public NestedScope nestedScope() {
            throw new UnsupportedOperationException();
        }

        @Override
        public SubSearchContext subSearchContext() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void addReleasable(Aggregator aggregator) {
            releaseMe.add(aggregator);
        }

        @Override
        public void removeReleasable(Aggregator aggregator) {
            releaseMe.remove(aggregator);
        }

        @Override
        public int maxBuckets() {
            return Integer.MAX_VALUE;
        }

        @Override
        public BitsetFilterCache bitsetFilterCache() {
            throw new UnsupportedOperationException();
        }

        @Override
        public BucketedSort buildBucketedSort(SortBuilder<?> sort, int size, ExtraData values) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int shardRandomSeed() {
            return 0;
        }

        @Override
        public long getRelativeTimeInMillis() {
            return 0;
        }

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public CircuitBreaker breaker() {
            return breakerService.getBreaker(CircuitBreaker.REQUEST);
        }

        @Override
        public Analyzer getIndexAnalyzer(Function<String, NamedAnalyzer> unindexedFieldAnalyzer) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isCacheable() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean enableRewriteToFilterByFilter() {
            // As a search has it by default. A columnar keyword field is not indexed, so it is never rewritten.
            return true;
        }

        @Override
        public boolean isInSortOrderExecutionRequired() {
            return false;
        }

        @Override
        public Set<String> sourcePath(String fullName) {
            return Set.of(fullName);
        }

        @Override
        public void close() {
            Releasables.close(releaseMe);
        }
    }
}
