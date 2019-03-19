/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ingest;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.IntsRefBuilder;
import org.apache.lucene.util.fst.ByteSequenceOutputs;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.Util;
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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

public final class DecorateProcessor3 extends AbstractProcessor {

    public static final String TYPE = "decorate3";

    private final Function<String, Engine.Searcher> searcherProvider;
    private final BiFunction<String, String, IndexFieldData<?>> fieldDataProvider;

    DecorateProcessor3(String tag,
                       Function<String, Engine.Searcher> searcherProvider,
                       BiFunction<String, String, IndexFieldData<?>> fieldDataProvider) {
        super(tag);
        this.searcherProvider = searcherProvider;
        this.fieldDataProvider = fieldDataProvider;
    }

    FST<BytesRef> fst;

    @Override
    public IngestDocument execute(IngestDocument ingestDocument) throws Exception {
        final String domainField = "Domain";
        final String targetField = "lookup";
        final String indexName = "lookup";
        final String fieldName = "Domain";
        final String[] extractFields = new String[] {"GlobalRank"};

        final String value = ingestDocument.getFieldValue(domainField, String.class);
        if (fst == null) {
            synchronized (this) {
                if (fst == null) {
                    try (Engine.Searcher searcher = searcherProvider.apply(indexName)) {
                        if (searcher.getDirectoryReader().leaves().size() > 1) {
                            throw new RuntimeException("lookup shard should only have 1 segment");
                        }

                        IntsRefBuilder intsSpare = new IntsRefBuilder();
                        ByteSequenceOutputs outputs = ByteSequenceOutputs.getSingleton();
                        org.apache.lucene.util.fst.Builder<BytesRef> builder = new org.apache.lucene.util.fst.Builder<>(
                            FST.INPUT_TYPE.BYTE4, outputs);

                        LeafReaderContext leafReader = searcher.getDirectoryReader().leaves().get(0);
                        TermsEnum tenum = leafReader.reader().terms(fieldName).iterator();
                        for (BytesRef term = tenum.next(); term != null; term = tenum.next()) {
                            PostingsEnum postingsEnum = tenum.postings(null);
                            int docId = postingsEnum.nextDoc();
                            BytesRef val = fetchValues(leafReader, docId, indexName, extractFields);
                            intsSpare.copyUTF8Bytes(term);
                            builder.add(intsSpare.get(), val);
                            assert postingsEnum.nextDoc() == DocIdSetIterator.NO_MORE_DOCS;
                        }
                        fst = builder.finish();
                    }
                }
            }
        }

        Map<String, Object> additionalData = new HashMap<>();

        IntsRefBuilder intsSpare = new IntsRefBuilder();
        intsSpare.copyUTF8Bytes(new BytesRef(value));
        BytesRef result = Util.get(fst, intsSpare.get());
        if (result != null) {
            additionalData.put(extractFields[0], result.utf8ToString());
        }

        ingestDocument.setFieldValue(targetField, additionalData);

        return ingestDocument;
    }

    private BytesRef fetchValues(LeafReaderContext leaf, int docId, String indexName, String[] extractFields) throws IOException {
        BytesRefBuilder builder = new BytesRefBuilder();

        for (String extractField : extractFields) {
            final IndexFieldData<?> indexFieldData = fieldDataProvider.apply(indexName, extractField);
            final AtomicFieldData data = indexFieldData.load(leaf);

            if (indexFieldData instanceof IndexNumericFieldData) {
                final IndexNumericFieldData.NumericType numericType = ((IndexNumericFieldData) indexFieldData).getNumericType();
                if (numericType.isFloatingPoint()) {
                    final SortedNumericDoubleValues doubleValues = ((AtomicNumericFieldData) data).getDoubleValues();
                    if (doubleValues.advanceExact(docId)) {
                        for (int i = 0; i < doubleValues.docValueCount(); i++) {
                            builder.append(new BytesRef(String.valueOf(doubleValues.nextValue())));
                        }
                    }
                } else {
                    final SortedNumericDocValues longValues = ((AtomicNumericFieldData) data).getLongValues();
                    if (longValues.advanceExact(docId)) {
                        for (int i = 0; i < longValues.docValueCount(); i++) {
                            builder.append(new BytesRef(String.valueOf(longValues.nextValue())));
                        }
                    }
                }
            } else {
                SortedBinaryDocValues binaryValues = data.getBytesValues();
                if (binaryValues.advanceExact(docId)) {
                    for (int i = 0; i != binaryValues.docValueCount(); i++) {
                        builder.append(binaryValues.nextValue());
                    }
                }
            }
        }

        return builder.toBytesRef();
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
            return new DecorateProcessor3(tag, searcherProvider, fieldDateProvider);
        }
    }
}
