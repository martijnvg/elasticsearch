/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ingest;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.fielddata.AtomicFieldData;
import org.elasticsearch.index.fielddata.AtomicNumericFieldData;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.ingest.AbstractProcessor;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

public final class DecorateProcessor2 extends AbstractProcessor {

    public static final String TYPE = "decorate2";

    private final Function<String, Engine.Searcher> searcherProvider;
    private final BiFunction<String, String, IndexFieldData<?>> fieldDataProvider;

    DecorateProcessor2(String tag,
                       Function<String, Engine.Searcher> searcherProvider,
                       BiFunction<String, String, IndexFieldData<?>> fieldDataProvider) {
        super(tag);
        this.searcherProvider = searcherProvider;
        this.fieldDataProvider = fieldDataProvider;
    }

    @Override
    public IngestDocument execute(IngestDocument ingestDocument) throws Exception {
        final String domainField = "Domain";
        final String targetField = "lookup";
        final String indexName = "lookup";
        final String fieldName = "Domain";
        final String[] extractFields = new String[]{"GlobalRank"};

        final String value = ingestDocument.getFieldValue(domainField, String.class);
        Engine.Searcher searcher = ingestDocument.searcherMap.computeIfAbsent(indexName, searcherProvider);
        if (searcher.getDirectoryReader().leaves().size() > 1) {
            throw new RuntimeException("lookup shard should only have 1 segment");
        }

        final LeafReaderContext leaf = searcher.reader().leaves().get(0);
        Terms terms = leaf.reader().terms(fieldName);
        TermsEnum tenum = terms.iterator();
        if (tenum.seekExact(new BytesRef(value))) {
            final PostingsEnum penum = tenum.postings(null, PostingsEnum.NONE);
            final int docId = penum.nextDoc();
            assert docId != PostingsEnum.NO_MORE_DOCS;
            assert penum.freq() == 1;

            final Map<String, Object> additionalData = new HashMap<>();
            for (final String extractField : extractFields) {
                final IndexFieldData<?> indexFieldData = ingestDocument.fieldDataMap.computeIfAbsent(Tuple.tuple(indexName, extractField),
                    key -> fieldDataProvider.apply(key.v1(), key.v2()));
                final AtomicFieldData data = indexFieldData.load(leaf);

                List<Object> values = new LinkedList<>();
                if (indexFieldData instanceof IndexNumericFieldData) {
                    final IndexNumericFieldData.NumericType numericType = ((IndexNumericFieldData) indexFieldData).getNumericType();
                    if (numericType.isFloatingPoint()) {
                        final SortedNumericDoubleValues doubleValues = ((AtomicNumericFieldData) data).getDoubleValues();
                        if (doubleValues.advanceExact(docId)) {
                            for (int i = 0; i < doubleValues.docValueCount(); i++) {
                                values.add(doubleValues.nextValue());
                            }
                        }
                    } else {
                        final SortedNumericDocValues longValues = ((AtomicNumericFieldData) data).getLongValues();
                        if (longValues.advanceExact(docId)) {
                            for (int i = 0; i < longValues.docValueCount(); i++) {
                                values.add(longValues.nextValue());
                            }
                        }
                    }
                } else {
                    SortedBinaryDocValues binaryValues = data.getBytesValues();
                    if (binaryValues.advanceExact(docId)) {
                        for (int i = 0; i != binaryValues.docValueCount(); i++) {
                            values.add(binaryValues.nextValue().utf8ToString());
                        }
                    }
                }

                final int size = values.size();
                if (size == 1) {
                    additionalData.put(extractField, values.get(0));
                } else if (size != 0) {
                    additionalData.put(extractField, values);
                }
            }
            ingestDocument.setFieldValue(targetField, additionalData);
        }

        return ingestDocument;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    public static final class Factory implements Processor.Factory {

        private final Function<String, Engine.Searcher> searcherProvider;
        private final BiFunction<String, String, IndexFieldData<?>> fieldDateProvider;

        Factory(Function<String, Engine.Searcher> searcherProvider,
                BiFunction<String, String, IndexFieldData<?>> fieldDateProvider) {
            this.searcherProvider = searcherProvider;
            this.fieldDateProvider = fieldDateProvider;
        }

        @Override
        public Processor create(Map<String, Processor.Factory> processorFactories, String tag, Map<String, Object> config) throws Exception {
            return new DecorateProcessor2(tag, searcherProvider, fieldDateProvider);
        }
    }
}
