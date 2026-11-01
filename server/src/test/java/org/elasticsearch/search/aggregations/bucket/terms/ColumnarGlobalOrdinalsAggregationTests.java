/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.aggregations.bucket.terms;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.indices.IndicesRequestCache;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregation;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeValuesSourceBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.TermsValuesSourceBuilder;
import org.elasticsearch.search.aggregations.metrics.Cardinality;
import org.elasticsearch.search.aggregations.metrics.Sum;
import org.elasticsearch.search.profile.ProfileResult;
import org.elasticsearch.search.profile.SearchProfileShardResult;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailuresAndResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.startsWith;

/**
 * Aggregations over a columnar keyword field read through global ordinals, when
 * {@link IndexSettings#COLUMNAR_GLOBAL_ORDINALS_ENABLED_SETTING} is on, have to give what an ordinary keyword field gives for the
 * same documents. The documents have few distinct values and some held once, so each refresh writes a segment whose column
 * either has values escaping its dictionary or is stored plainly, and those have their ordinals worked out. Each test also checks
 * which aggregator ran, so that a test of global ordinals that quietly read by value fails.
 */
public class ColumnarGlobalOrdinalsAggregationTests extends ESSingleNodeTestCase {

    private static final String COLUMNAR = "columnar-index";
    private static final String PLAIN = "plain-index";
    private static final String GLOBAL_ORDINALS = GlobalOrdinalsStringTermsAggregator.class.getSimpleName();
    private static final String MAP = MapStringTermsAggregator.class.getSimpleName();

    private static final String[] COMMON_VALUES = { "alpha", "bravo", "charlie", "delta", "echo" };

    @Before
    public void indexTheSameDocumentsTwice() {
        createIndex(COLUMNAR, columnarSettings(), "@timestamp", "type=date", "kw", "type=keyword", "n", "type=long");
        createIndex(PLAIN, noRequestCache(Settings.builder()).build(), "@timestamp", "type=date", "kw", "type=keyword", "n", "type=long");
        final int batches = randomIntBetween(2, 5);
        int doc = 0;
        for (int batch = 0; batch < batches; batch++) {
            final int size = randomIntBetween(500, 3000);
            final BulkRequestBuilder columnar = client().prepareBulk();
            final BulkRequestBuilder plain = client().prepareBulk();
            for (int i = 0; i < size; i++, doc++) {
                final Map<String, Object> source = source(doc);
                columnar.add(prepareIndex(COLUMNAR).setId(Integer.toString(doc)).setSource(source));
                plain.add(prepareIndex(PLAIN).setId(Integer.toString(doc)).setSource(source));
            }
            assertFalse(columnar.get().hasFailures());
            assertFalse(plain.get().hasFailures());
            indicesAdmin().prepareRefresh(COLUMNAR, PLAIN).get();
        }
    }

    private static Settings columnarSettings() {
        return noRequestCache(
            Settings.builder()
                .put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR)
                .put(IndexSettings.COLUMNAR_GLOBAL_ORDINALS_ENABLED_SETTING.getKey(), true)
        ).build();
    }

    /** The request cache would keep results across the tests' setting changes; see {@code ColumnarTermsAggregationTests}. */
    private static Settings.Builder noRequestCache(Settings.Builder settings) {
        return settings.put(IndicesRequestCache.INDEX_CACHE_REQUEST_ENABLED_SETTING.getKey(), false);
    }

    /**
     * Mostly one or two values from a few, some documents with none, and one in fifteen a value held once. A document that
     * holds two values holds the same one twice in one of three.
     */
    private static Map<String, Object> source(int doc) {
        final Map<String, Object> source = new TreeMap<>();
        source.put("@timestamp", "2024-01-01T00:00:00Z");
        source.put("n", doc % 7);
        if (doc % 20 == 0) {
            return source; // no kw
        }
        if (doc % 15 == 1) {
            source.put("kw", "unique-" + doc);
        } else if (doc % 4 == 0) {
            final String value = COMMON_VALUES[doc % COMMON_VALUES.length];
            source.put("kw", doc % 3 == 0 ? List.of(value, value) : List.of(value, COMMON_VALUES[(doc + 1) % COMMON_VALUES.length]));
        } else {
            source.put("kw", COMMON_VALUES[doc % COMMON_VALUES.length]);
        }
        return source;
    }

    public void testTermsMatchesAnOrdinaryKeywordField() {
        final AggregationBuilder terms = AggregationBuilders.terms("t").field("kw").size(10_000);
        final Map<String, Long> expected = counts(PLAIN, request -> {}, terms);
        assertThat(expected.size(), greaterThan(COMMON_VALUES.length));
        assertThat(counts(COLUMNAR, request -> {}, terms), equalTo(expected));
        assertThat(termsAggregator(terms), startsWith(GLOBAL_ORDINALS));
    }

    /** The top terms by count, and by key either way, are the same terms. */
    public void testOrderAndSizeGiveTheSameTerms() {
        for (BucketOrder order : List.of(BucketOrder.count(false), BucketOrder.key(true), BucketOrder.key(false))) {
            final AggregationBuilder terms = AggregationBuilders.terms("t").field("kw").size(randomIntBetween(1, 20)).order(order);
            assertThat(order.toString(), keys(COLUMNAR, terms), equalTo(keys(PLAIN, terms)));
        }
        assertThat(termsAggregator(AggregationBuilders.terms("t").field("kw").order(BucketOrder.key(true))), startsWith(GLOBAL_ORDINALS));
    }

    /** A sub-aggregation, and ordering by it, see the same documents in each bucket. */
    public void testSubAggregationAndOrderingByIt() {
        final AggregationBuilder terms = AggregationBuilders.terms("t")
            .field("kw")
            .size(10_000)
            .order(BucketOrder.aggregation("s", false))
            .subAggregation(AggregationBuilders.sum("s").field("n"));
        assertThat(sums(COLUMNAR, terms), equalTo(sums(PLAIN, terms)));
        assertThat(keys(COLUMNAR, terms), equalTo(keys(PLAIN, terms)));
    }

    /** Include and exclude, by pattern, by terms and by partition, leave out the same terms. */
    public void testIncludeAndExclude() {
        final List<IncludeExclude> filters = List.of(
            new IncludeExclude("unique-1.*|alpha", null, null, null),
            new IncludeExclude(null, "alpha|bravo", null, null),
            new IncludeExclude(null, null, new TreeSet<>(List.of(new BytesRef("echo"))), null),
            new IncludeExclude(randomIntBetween(0, 2), 3)
        );
        for (IncludeExclude filter : filters) {
            final AggregationBuilder terms = AggregationBuilders.terms("t").field("kw").size(10_000).includeExclude(filter);
            assertThat(filter.toString(), counts(COLUMNAR, request -> {}, terms), equalTo(counts(PLAIN, request -> {}, terms)));
        }
    }

    /** {@code min_doc_count: 0} fills in the terms no matching document holds, from the ordinals of every segment. */
    public void testMinDocCountZero() {
        final AggregationBuilder terms = AggregationBuilders.terms("t").field("kw").size(10_000).minDocCount(0);
        final Consumer<SearchRequestBuilder> query = request -> request.setQuery(QueryBuilders.termQuery("n", 3));
        assertThat(counts(COLUMNAR, query, terms), equalTo(counts(PLAIN, query, terms)));
    }

    /** A missing value is a bucket of its own. */
    public void testMissing() {
        final AggregationBuilder terms = AggregationBuilders.terms("t").field("kw").size(10_000).missing("none");
        final Map<String, Long> expected = counts(PLAIN, request -> {}, terms);
        assertThat(expected.get("t/none"), greaterThan(0L));
        assertThat(counts(COLUMNAR, request -> {}, terms), equalTo(expected));
    }

    /** Under another aggregation, terms is collected for many owning buckets. */
    public void testNestedUnderAnotherAggregation() {
        final AggregationBuilder outer = AggregationBuilders.terms("outer")
            .field("n")
            .size(100)
            .subAggregation(AggregationBuilders.terms("t").field("kw").size(10_000));
        assertThat(nested(COLUMNAR, outer), equalTo(nested(PLAIN, outer)));
    }

    /** {@code significant_terms} counts the foreground and the background of each term. */
    public void testSignificantTerms() {
        final AggregationBuilder significant = AggregationBuilders.significantTerms("s").field("kw").size(10_000).minDocCount(1);
        final Consumer<SearchRequestBuilder> foreground = request -> request.setQuery(QueryBuilders.termQuery("n", 3));
        assertThat(counts(COLUMNAR, foreground, significant), equalTo(counts(PLAIN, foreground, significant)));
    }

    /** {@code cardinality} counts the same distinct values. */
    public void testCardinality() {
        final AggregationBuilder cardinality = AggregationBuilders.cardinality("c").field("kw").precisionThreshold(40_000);
        assertThat(cardinality(COLUMNAR, cardinality), equalTo(cardinality(PLAIN, cardinality)));
    }

    /** A {@code composite} terms source pages through the same keys. */
    public void testComposite() {
        final List<CompositeValuesSourceBuilder<?>> sources = List.of(new TermsValuesSourceBuilder("kw").field("kw"));
        assertThat(composite(COLUMNAR, sources), equalTo(composite(PLAIN, sources)));
    }

    /** With the setting off, or a segment past the bound, the field is read by value, and counts the same. */
    public void testReadByValueWhenNotEnabledOrPastTheBound() {
        final AggregationBuilder terms = AggregationBuilders.terms("t").field("kw").size(10_000);
        final Map<String, Long> expected = counts(PLAIN, request -> {}, terms);

        // Every segment holds values held once, so each has to have some read, and none is within a bound of zero.
        updateColumnarSettings(Settings.builder().put(IndexSettings.COLUMNAR_GLOBAL_ORDINALS_MAX_MATERIALIZED_VALUES_SETTING.getKey(), 0));
        assertThat(termsAggregator(terms), equalTo(MAP));
        assertThat(counts(COLUMNAR, request -> {}, terms), equalTo(expected));

        updateColumnarSettings(
            Settings.builder()
                .putNull(IndexSettings.COLUMNAR_GLOBAL_ORDINALS_MAX_MATERIALIZED_VALUES_SETTING.getKey())
                .put(IndexSettings.COLUMNAR_GLOBAL_ORDINALS_ENABLED_SETTING.getKey(), false)
        );
        assertThat(termsAggregator(terms), equalTo(MAP));
        assertThat(counts(COLUMNAR, request -> {}, terms), equalTo(expected));
    }

    /** Deleted documents are left out, and a force merged index, of one segment, counts the same. */
    public void testDeletesAndForceMerge() {
        final BulkRequestBuilder deletes = client().prepareBulk();
        final BulkRequestBuilder plainDeletes = client().prepareBulk();
        for (int doc = 0; doc < 2000; doc += randomIntBetween(1, 40)) {
            deletes.add(client().prepareDelete(COLUMNAR, Integer.toString(doc)));
            plainDeletes.add(client().prepareDelete(PLAIN, Integer.toString(doc)));
        }
        assertFalse(deletes.get().hasFailures());
        assertFalse(plainDeletes.get().hasFailures());
        indicesAdmin().prepareRefresh(COLUMNAR, PLAIN).get();
        final AggregationBuilder terms = AggregationBuilders.terms("t").field("kw").size(10_000);
        assertThat(counts(COLUMNAR, request -> {}, terms), equalTo(counts(PLAIN, request -> {}, terms)));

        indicesAdmin().prepareForceMerge(COLUMNAR, PLAIN).setMaxNumSegments(1).get();
        indicesAdmin().prepareRefresh(COLUMNAR, PLAIN).get();
        assertThat(counts(COLUMNAR, request -> {}, terms), equalTo(counts(PLAIN, request -> {}, terms)));
        assertThat(termsAggregator(terms), startsWith(GLOBAL_ORDINALS));
    }

    private void updateColumnarSettings(Settings.Builder settings) {
        assertTrue(indicesAdmin().prepareUpdateSettings(COLUMNAR).setSettings(settings).get().isAcknowledged());
    }

    /**
     * The name of the aggregator that ran {@code terms} on the columnar index, from a profiled search. Global ordinals run as
     * {@link GlobalOrdinalsStringTermsAggregator} or one of its variants, whose names start with its own.
     */
    private String termsAggregator(AggregationBuilder terms) {
        final List<String> names = new ArrayList<>();
        assertNoFailuresAndResponse(client().prepareSearch(COLUMNAR).setSize(0).setProfile(true).addAggregation(terms), response -> {
            for (SearchProfileShardResult shard : response.getSearchProfileShardResults().values()) {
                for (ProfileResult result : shard.getAggregationProfileResults().getProfileResults()) {
                    names.add(result.getQueryName());
                }
            }
        });
        assertThat(names.size(), equalTo(1));
        return names.get(0);
    }

    /** Aggregation name and key to doc count, and for a significant term its subset and superset counts too. */
    private Map<String, Long> counts(String index, Consumer<SearchRequestBuilder> customize, AggregationBuilder aggregation) {
        final Map<String, Long> counts = new TreeMap<>();
        final SearchRequestBuilder request = client().prepareSearch(index).setSize(0).setTrackTotalHits(false).addAggregation(aggregation);
        customize.accept(request);
        assertNoFailuresAndResponse(request, response -> {
            final MultiBucketsAggregation result = response.getAggregations().get(aggregation.getName());
            for (MultiBucketsAggregation.Bucket bucket : result.getBuckets()) {
                final String key = aggregation.getName() + "/" + bucket.getKeyAsString();
                counts.put(key, bucket.getDocCount());
                if (bucket instanceof SignificantTerms.Bucket significant) {
                    counts.put(key + "#subsetDf", significant.getSubsetDf());
                    counts.put(key + "#supersetDf", significant.getSupersetDf());
                }
            }
        });
        return counts;
    }

    /** The keys of the buckets, in the order the aggregation gave them. */
    private List<String> keys(String index, AggregationBuilder terms) {
        final List<String> keys = new ArrayList<>();
        assertNoFailuresAndResponse(client().prepareSearch(index).setSize(0).addAggregation(terms), response -> {
            final StringTerms result = response.getAggregations().get(terms.getName());
            for (StringTerms.Bucket bucket : result.getBuckets()) {
                keys.add(bucket.getKeyAsString());
            }
        });
        return keys;
    }

    /** Key to the sum of the sub-aggregation {@code s}. */
    private Map<String, Double> sums(String index, AggregationBuilder terms) {
        final Map<String, Double> sums = new TreeMap<>();
        assertNoFailuresAndResponse(client().prepareSearch(index).setSize(0).addAggregation(terms), response -> {
            final StringTerms result = response.getAggregations().get(terms.getName());
            for (StringTerms.Bucket bucket : result.getBuckets()) {
                final Sum sum = bucket.getAggregations().get("s");
                sums.put(bucket.getKeyAsString(), sum.value());
            }
        });
        return sums;
    }

    /** Outer key and inner key to doc count. */
    private Map<String, Long> nested(String index, AggregationBuilder outer) {
        final Map<String, Long> buckets = new TreeMap<>();
        assertNoFailuresAndResponse(client().prepareSearch(index).setSize(0).addAggregation(outer), response -> {
            final LongTerms outerTerms = response.getAggregations().get("outer");
            for (LongTerms.Bucket bucket : outerTerms.getBuckets()) {
                final StringTerms inner = bucket.getAggregations().get("t");
                for (StringTerms.Bucket innerBucket : inner.getBuckets()) {
                    buckets.put(bucket.getKeyAsString() + "/" + innerBucket.getKeyAsString(), innerBucket.getDocCount());
                }
            }
        });
        return buckets;
    }

    private long cardinality(String index, AggregationBuilder cardinality) {
        final long[] value = new long[1];
        assertNoFailuresAndResponse(client().prepareSearch(index).setSize(0).addAggregation(cardinality), response -> {
            final Cardinality result = response.getAggregations().get(cardinality.getName());
            value[0] = result.getValue();
        });
        return value[0];
    }

    /** Every page of a composite aggregation, key to doc count. */
    private Map<String, Long> composite(String index, List<CompositeValuesSourceBuilder<?>> sources) {
        final Map<String, Long> buckets = new TreeMap<>();
        Map<String, Object> after = null;
        while (true) {
            final var composite = AggregationBuilders.composite("c", sources).size(randomIntBetween(3, 50));
            if (after != null) {
                composite.aggregateAfter(after);
            }
            final AtomicReference<Map<String, Object>> next = new AtomicReference<>();
            assertNoFailuresAndResponse(client().prepareSearch(index).setSize(0).addAggregation(composite), response -> {
                final CompositeAggregation result = response.getAggregations().get("c");
                for (CompositeAggregation.Bucket bucket : result.getBuckets()) {
                    buckets.put(bucket.getKeyAsString(), bucket.getDocCount());
                }
                next.set(result.getBuckets().isEmpty() ? null : result.afterKey());
            });
            if (next.get() == null) {
                return buckets;
            }
            after = next.get();
        }
    }
}
