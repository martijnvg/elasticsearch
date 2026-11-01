/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.fielddata.plain;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.perfield.PerFieldDocValuesFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.columnar.ColumNARDocValuesFormat;
import org.elasticsearch.columnar.ColumnarFieldType;
import org.elasticsearch.columnar.string.StringBinaryPayload;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.fielddata.IndexOrdinalsFieldData;
import org.elasticsearch.index.fielddata.LeafFieldData;
import org.elasticsearch.index.fielddata.ordinals.GlobalOrdinalsIndexFieldData;
import org.elasticsearch.index.fielddata.plain.ColumnarOrdinalsIndexFieldData.SegmentKind;
import org.elasticsearch.index.mapper.BinaryDocValuesFormat;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.script.field.KeywordDocValuesField;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;

/**
 * A columnar keyword column read through ordinals has to read as {@link SortedSetDocValues} of the same values: each
 * document's distinct values in term order, and ordinals that number the segment's distinct values in term order. Each
 * test writes real columnar segments, so the codec decides how the column is stored, and checks which way the segment
 * was classified before checking what it reads as.
 *
 * <p>A document is {@code null} for no field, or its slots separated by {@code |}, with {@link #NULL_SLOT} for a null.
 */
public class ColumnarOrdinalsIndexFieldDataTests extends ESTestCase {

    private static final String FIELD = "kw";
    private static final String NULL_SLOT = "<null>";
    private static final String[] COMMON = { "alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf" };

    /** Values that repeat throughout, so the dictionary names every one. */
    public void testDictionaryNamingEveryValue() throws IOException {
        final String[] docs = docs(randomIntBetween(500, 2000), 0, true);
        try (Index index = new Index(List.<String[]>of(docs), Set.of())) {
            assertThat(index.kinds(Long.MAX_VALUE), equalTo(List.of(SegmentKind.DICTIONARY)));
            index.assertReadsAsSortedSet(fieldData(Long.MAX_VALUE));
        }
    }

    /** One slot a document reads as single valued, so that callers can take their single valued paths. */
    public void testOneSlotADocumentIsSingleValued() throws IOException {
        final String[] docs = docs(randomIntBetween(500, 2000), randomBoolean() ? 0 : 0.02, false);
        try (Index index = new Index(List.<String[]>of(docs), Set.of())) {
            final ColumnarOrdinalsIndexFieldData fieldData = fieldData(Long.MAX_VALUE);
            final LeafReaderContext leaf = index.reader.leaves().get(0);
            assertThat(DocValues.unwrapSingleton(fieldData.load(leaf).getOrdinalsValues()), notNullValue());
            index.assertReadsAsSortedSet(fieldData);
        }
    }

    /** A few values held once escape the dictionary, and are ordered among its terms. */
    public void testDictionaryWithEscapes() throws IOException {
        final String[] docs = docs(randomIntBetween(1000, 3000), 0.02, randomBoolean());
        try (Index index = new Index(List.<String[]>of(docs), Set.of())) {
            assertThat(index.kinds(Long.MAX_VALUE), equalTo(List.of(SegmentKind.ESCAPED_DICTIONARY)));
            index.assertReadsAsSortedSet(fieldData(Long.MAX_VALUE));
        }
    }

    /** Values that are mostly held once are stored plainly, and are all read and ordered. */
    public void testPlainColumn() throws IOException {
        final String[] docs = docs(randomIntBetween(200, 1000), 1.0, randomBoolean());
        try (Index index = new Index(List.<String[]>of(docs), Set.of())) {
            assertThat(index.kinds(Long.MAX_VALUE), equalTo(List.of(SegmentKind.PLAIN)));
            index.assertReadsAsSortedSet(fieldData(Long.MAX_VALUE));
        }
    }

    /** A segment that would have to read more values than the bound cannot offer ordinals; one that reads none still can. */
    public void testTheBoundIsOnValuesThatHaveToBeRead() throws IOException {
        final String[] dictionary = docs(500, 0, true);
        final String[] escaped = docs(1000, 0.02, true);
        final String[] plain = docs(200, 1.0, true);
        try (Index index = new Index(List.of(dictionary, escaped, plain), Set.of())) {
            assertThat(
                index.kinds(Long.MAX_VALUE),
                equalTo(List.of(SegmentKind.DICTIONARY, SegmentKind.ESCAPED_DICTIONARY, SegmentKind.PLAIN))
            );
            assertThat(index.kinds(0), equalTo(List.of(SegmentKind.DICTIONARY, SegmentKind.UNSUPPORTED, SegmentKind.UNSUPPORTED)));
        }
    }

    /** A segment without the field has no ordinals, rather than none to offer. */
    public void testSegmentWithoutTheField() throws IOException {
        final String[] none = new String[randomIntBetween(1, 50)];
        try (Index index = new Index(List.of(docs(500, 0, true), none), Set.of())) {
            assertThat(index.kinds(Long.MAX_VALUE), equalTo(List.of(SegmentKind.DICTIONARY, SegmentKind.ABSENT)));
            index.assertReadsAsSortedSet(fieldData(Long.MAX_VALUE));
        }
    }

    /**
     * Global ordinals across segments of every kind number the union of their values in term order, and map each
     * segment's ordinals onto it. Deleted documents keep their values in their segment's ordinals, as they do for any
     * other field.
     */
    public void testGlobalOrdinalsAcrossEveryKindOfSegment() throws Exception {
        final List<String[]> segments = List.of(docs(800, 0, true), docs(1500, 0.02, true), docs(300, 1.0, false), new String[10]);
        final int total = segments.stream().mapToInt(s -> s.length).sum();
        final Set<Integer> deleted = new HashSet<>();
        for (int i = 0; i < total / 10; i++) {
            deleted.add(randomIntBetween(0, total - 1));
        }
        try (Index index = new Index(segments, deleted)) {
            final ColumnarOrdinalsIndexFieldData fieldData = fieldData(Long.MAX_VALUE);
            final IndexOrdinalsFieldData global = fieldData.loadGlobalDirect(index.reader);
            assertThat(global, instanceOf(GlobalOrdinalsIndexFieldData.class));
            final NavigableSet<String> union = new TreeSet<>();
            for (String[] segment : segments) {
                union.addAll(distinct(segment));
            }
            assertThat(global.getOrdinalMap().getValueCount(), equalTo((long) union.size()));

            final IndexOrdinalsFieldData consumer = ((GlobalOrdinalsIndexFieldData) global).newConsumer(index.reader);
            int docBase = 0;
            for (int s = 0; s < segments.size(); s++) {
                final LeafReaderContext leaf = index.reader.leaves().get(s);
                final SortedSetDocValues globalOrds = consumer.load(leaf).getOrdinalsValues();
                assertThat(globalOrds.getValueCount(), equalTo((long) union.size()));
                final String[] segment = segments.get(s);
                for (int doc = 0; doc < segment.length; doc++) {
                    final List<String> expected = new ArrayList<>(values(segment[doc]));
                    assertThat(globalOrds.advanceExact(doc), equalTo(expected.isEmpty() == false));
                    if (expected.isEmpty()) {
                        continue;
                    }
                    final List<String> actual = new ArrayList<>();
                    for (int i = 0; i < globalOrds.docValueCount(); i++) {
                        final long ord = globalOrds.nextOrd();
                        assertThat(ord, equalTo((long) union.headSet(globalOrds.lookupOrd(ord).utf8ToString()).size()));
                        actual.add(globalOrds.lookupOrd(ord).utf8ToString());
                    }
                    assertThat("doc [" + (docBase + doc) + "]", actual, equalTo(expected));
                }
                docBase += segment.length;
            }
        }
    }

    /**
     * Ordinals a segment needs working out are worked out once and held by the cache, charged to the field data
     * breaker at the size they report. A dictionary that names every value needs nothing.
     */
    public void testWorkedOutOrdinalsAreCachedAndCharged() throws IOException {
        try (Index index = new Index(List.of(docs(500, 0, true), docs(1000, 0.02, true), docs(200, 1.0, true)), Set.of())) {
            final CountingCache cache = new CountingCache();
            final CountingBreaker breaker = new CountingBreaker();
            final ColumnarOrdinalsIndexFieldData fieldData = new ColumnarOrdinalsIndexFieldData(
                values(),
                cache,
                new NoneCircuitBreakerService() {
                    @Override
                    public CircuitBreaker getBreaker(String name) {
                        return breaker;
                    }
                },
                Long.MAX_VALUE
            );
            for (int round = 0; round < 3; round++) {
                for (LeafReaderContext leaf : index.reader.leaves()) {
                    fieldData.load(leaf).getOrdinalsValues();
                }
            }
            assertThat("the two segments that needed them, once each", cache.loads, equalTo(2));
            final long held = cache.held.values().stream().mapToLong(Accountable::ramBytesUsed).sum();
            assertThat(held, greaterThan(0L));
            assertThat(breaker.charged, equalTo(held));
        }
    }

    /** The field data the tests read with, through the cache that does none. */
    private static ColumnarOrdinalsIndexFieldData fieldData(long maxMaterializedValues) {
        return new ColumnarOrdinalsIndexFieldData(
            values(),
            new IndexFieldDataCache.None(),
            new NoneCircuitBreakerService(),
            maxMaterializedValues
        );
    }

    private static BytesBinaryIndexFieldData values() {
        return new BytesBinaryIndexFieldData(
            FIELD,
            CoreValuesSourceType.KEYWORD,
            KeywordDocValuesField::new,
            IndexVersion.current(),
            BinaryDocValuesFormat.COLUMNAR_PAYLOAD
        );
    }

    /**
     * Random documents: about one in twenty without the field, and otherwise values from {@link #COMMON}, or with
     * probability {@code unique} a value held once. With {@code multiValued}, a document holds up to four slots, which
     * can repeat a value or be null; otherwise it holds one, which can be null.
     */
    private static String[] docs(int numDocs, double unique, boolean multiValued) {
        final String[] docs = new String[numDocs];
        int next = 0;
        for (int i = 0; i < numDocs; i++) {
            if (randomIntBetween(0, 19) == 0) {
                continue;
            }
            final int slots = multiValued ? randomIntBetween(1, 4) : 1;
            final List<String> values = new ArrayList<>();
            for (int s = 0; s < slots; s++) {
                if (randomIntBetween(0, 29) == 0) {
                    values.add(NULL_SLOT);
                } else if (random().nextDouble() < unique) {
                    values.add("unique-" + (next++) + "-" + randomAlphaOfLength(8));
                } else {
                    values.add(randomFrom(COMMON));
                }
            }
            docs[i] = String.join("|", values);
        }
        return docs;
    }

    /** A document's distinct values, in term order. */
    private static NavigableSet<String> values(String doc) {
        final NavigableSet<String> values = new TreeSet<>();
        if (doc != null) {
            for (String slot : doc.split("\\|")) {
                if (slot.equals(NULL_SLOT) == false) {
                    values.add(slot);
                }
            }
        }
        return values;
    }

    private static NavigableSet<String> distinct(String[] docs) {
        final NavigableSet<String> distinct = new TreeSet<>();
        for (String doc : docs) {
            distinct.addAll(values(doc));
        }
        return distinct;
    }

    /** Segments written with the columnar codec, one for each array of documents, and some documents deleted. */
    private static final class Index implements AutoCloseable {
        private final List<String[]> segments;
        private final Directory dir;
        private final DirectoryReader reader;

        Index(List<String[]> segments, Set<Integer> deleted) throws IOException {
            this.segments = segments;
            this.dir = newDirectory();
            final IndexWriterConfig config = new IndexWriterConfig().setCodec(columnarCodec()).setMergePolicy(NoMergePolicy.INSTANCE);
            try (IndexWriter writer = new IndexWriter(dir, config)) {
                int id = 0;
                for (String[] segment : segments) {
                    for (String value : segment) {
                        final Document doc = new Document();
                        doc.add(new StringField("id", Integer.toString(id++), Field.Store.NO));
                        if (value != null) {
                            doc.add(new Field(FIELD, encode(value), binaryDocValuesType()));
                        }
                        writer.addDocument(doc);
                    }
                    writer.commit(); // one segment each
                }
                for (int doc : deleted) {
                    writer.deleteDocuments(new Term("id", Integer.toString(doc)));
                }
            }
            this.reader = DirectoryReader.open(dir);
            assertThat(reader.leaves().size(), equalTo(segments.size()));
        }

        List<SegmentKind> kinds(long maxMaterializedValues) throws IOException {
            final List<SegmentKind> kinds = new ArrayList<>();
            for (LeafReaderContext leaf : reader.leaves()) {
                kinds.add(ColumnarOrdinalsIndexFieldData.classify(leaf.reader(), FIELD, maxMaterializedValues));
            }
            return kinds;
        }

        /**
         * Each segment reads as sorted set doc values of its documents' values: by {@code advanceExact} and by
         * {@code nextDoc}, its ordinals numbering its distinct values in term order, and its terms enum seeking them.
         */
        void assertReadsAsSortedSet(ColumnarOrdinalsIndexFieldData fieldData) throws IOException {
            for (int s = 0; s < segments.size(); s++) {
                final String[] docs = segments.get(s);
                final LeafReaderContext leaf = reader.leaves().get(s);
                final NavigableSet<String> distinct = distinct(docs);

                final SortedSetDocValues ordinals = fieldData.load(leaf).getOrdinalsValues();
                assertThat(ordinals.getValueCount(), equalTo((long) distinct.size()));
                long ord = 0;
                for (String term : distinct) {
                    assertThat(ordinals.lookupOrd(ord++).utf8ToString(), equalTo(term));
                }

                for (int doc = 0; doc < docs.length; doc++) {
                    final List<String> expected = new ArrayList<>(values(docs[doc]));
                    assertThat("doc [" + doc + "]", ordinals.advanceExact(doc), equalTo(expected.isEmpty() == false));
                    if (expected.isEmpty() == false) {
                        assertThat("doc [" + doc + "]", read(ordinals, distinct), equalTo(expected));
                    }
                }

                final SortedSetDocValues iterated = fieldData.load(leaf).getOrdinalsValues();
                for (int doc = 0; doc < docs.length; doc++) {
                    final List<String> expected = new ArrayList<>(values(docs[doc]));
                    if (expected.isEmpty()) {
                        continue;
                    }
                    assertThat(iterated.nextDoc(), equalTo(doc));
                    assertThat("doc [" + doc + "]", read(iterated, distinct), equalTo(expected));
                }
                assertThat(iterated.nextDoc(), equalTo(DocIdSetIterator.NO_MORE_DOCS));

                final TermsEnum terms = fieldData.load(leaf).getOrdinalsValues().termsEnum();
                for (String term : distinct) {
                    assertTrue(terms.seekExact(new BytesRef(term)));
                    assertThat(terms.ord(), equalTo((long) distinct.headSet(term).size()));
                }
                final String probe = randomFrom(COMMON) + "0";
                final String ceiling = distinct.ceiling(probe);
                final TermsEnum.SeekStatus status = terms.seekCeil(new BytesRef(probe));
                if (ceiling == null) {
                    assertThat(status, equalTo(TermsEnum.SeekStatus.END));
                } else {
                    assertThat(status, equalTo(TermsEnum.SeekStatus.NOT_FOUND));
                    assertThat(terms.term().utf8ToString(), equalTo(ceiling));
                }
                assertFalse(terms.seekExact(new BytesRef("not-a-value")));
            }
        }

        /** A document's values by its ordinals, checking they ascend and are each the ordinal of their term. */
        private static List<String> read(SortedSetDocValues ordinals, NavigableSet<String> distinct) throws IOException {
            final List<String> actual = new ArrayList<>();
            long previous = -1;
            for (int i = 0; i < ordinals.docValueCount(); i++) {
                final long ord = ordinals.nextOrd();
                assertThat("ascending and distinct", ord > previous, equalTo(true));
                previous = ord;
                final String term = ordinals.lookupOrd(ord).utf8ToString();
                assertThat(ord, equalTo((long) distinct.headSet(term).size()));
                actual.add(term);
            }
            return actual;
        }

        @Override
        public void close() throws IOException {
            reader.close();
            dir.close();
        }
    }

    /** Caches leaf field data by segment, and counts how often it had to load some. */
    private static final class CountingCache implements IndexFieldDataCache {
        final Map<Object, LeafFieldData> held = new HashMap<>();
        int loads;

        @Override
        @SuppressWarnings("unchecked")
        public <FD extends LeafFieldData, IFD extends IndexFieldData<FD>> FD load(LeafReaderContext context, IFD indexFieldData)
            throws Exception {
            final Object key = context.reader().getCoreCacheHelper().getKey();
            LeafFieldData fieldData = held.get(key);
            if (fieldData == null) {
                loads++;
                fieldData = indexFieldData.loadDirect(context);
                held.put(key, fieldData);
            }
            return (FD) fieldData;
        }

        @Override
        public <FD extends LeafFieldData, IFD extends IndexFieldData.Global<FD>> IFD load(DirectoryReader indexReader, IFD indexFieldData) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void clear() {}

        @Override
        public void clear(String fieldName) {}
    }

    /** Adds up what it is charged. */
    private static final class CountingBreaker extends NoopCircuitBreaker {
        long charged;

        CountingBreaker() {
            super(CircuitBreaker.FIELDDATA);
        }

        @Override
        public void addEstimateBytesAndMaybeBreak(long bytes, String label) {
            charged += bytes;
        }
    }

    private static BytesRef encode(String value) {
        final List<BytesRef> slots = new ArrayList<>();
        for (String slot : value.split("\\|")) {
            slots.add(slot.equals(NULL_SLOT) ? null : new BytesRef(slot));
        }
        return BytesRef.deepCopyOf(new StringBinaryPayload.Builder().encode(slots));
    }

    private static FieldType binaryDocValuesType() {
        final FieldType type = new FieldType();
        type.setDocValuesType(DocValuesType.BINARY);
        type.freeze();
        return type;
    }

    private static Codec columnarCodec() {
        final Codec base = TestUtil.getDefaultCodec();
        final DocValuesFormat columnar = new ColumNARDocValuesFormat(field -> ColumnarFieldType.STRING);
        return new FilterCodec(base.getName(), base) {
            private final DocValuesFormat perField = new PerFieldDocValuesFormat() {
                @Override
                public DocValuesFormat getDocValuesFormatForField(String field) {
                    return columnar;
                }
            };

            @Override
            public DocValuesFormat docValuesFormat() {
                return perField;
            }
        };
    }
}
