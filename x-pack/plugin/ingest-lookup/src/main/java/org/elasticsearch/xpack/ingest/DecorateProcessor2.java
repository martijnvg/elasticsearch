/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ingest;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.util.BytesRef;
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
        final String targetField = "GlobalRank";
        final String indexName = "lookup";
        final String fieldName = "Domain";
        final String[] extractFields = new String[] {"GlobalRank"};

        final String value = ingestDocument.getFieldValue(domainField, String.class);
        final TermQuery termQuery = new TermQuery(new Term(fieldName, new BytesRef(value)));

        // TODO: part of what searcherProvider is doing should be done once per shard bulk request
        // (E.g. open searcher once per shard bulk request, index / alias lookup)
        try (Engine.Searcher searcher = searcherProvider.apply(indexName)) {
            if (searcher.getDirectoryReader().leaves().size() > 1) {
                throw new RuntimeException("lookup shard should only have 1 segment");
            }

            TopDocs topDocs = searcher.searcher().search(termQuery, 1);
            if (topDocs.totalHits.value == 1) {
                final Map<String, Object> additionalData = new HashMap<>();
                final LeafReaderContext leaf = searcher.reader().leaves().get(0);

                for (final String extractField : extractFields) {
                    // TODO: part of what fieldDataProvider is doing should be done once per shard bulk request
                    final IndexFieldData<?> indexFieldData = fieldDataProvider.apply(indexName, extractField);
                    final AtomicFieldData data = indexFieldData.load(leaf);

                    List<Object> values = new LinkedList<>();
                    if (indexFieldData instanceof IndexNumericFieldData) {
                        final IndexNumericFieldData.NumericType numericType = ((IndexNumericFieldData) indexFieldData).getNumericType();
                        if (numericType.isFloatingPoint()) {
                            final SortedNumericDoubleValues doubleValues = ((AtomicNumericFieldData) data).getDoubleValues();
                            if (doubleValues.advanceExact(topDocs.scoreDocs[0].doc)) {
                                for (int i = 0; i < doubleValues.docValueCount(); i++) {
                                    values.add(doubleValues.nextValue());
                                }
                            }
                        } else {
                            final SortedNumericDocValues longValues = ((AtomicNumericFieldData) data).getLongValues();
                            if (longValues.advanceExact(topDocs.scoreDocs[0].doc)) {
                                for (int i = 0; i < longValues.docValueCount(); i++) {
                                    values.add(longValues.nextValue());
                                }
                            }
                        }
                    } else {
                        SortedBinaryDocValues binaryValues = data.getBytesValues();
                        if (binaryValues.advanceExact(topDocs.scoreDocs[0].doc)) {
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
            } else if (topDocs.totalHits.value > 1) {
                throw new RuntimeException();
            }
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
