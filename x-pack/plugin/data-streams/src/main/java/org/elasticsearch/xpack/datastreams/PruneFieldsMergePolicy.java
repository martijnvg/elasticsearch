/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.datastreams;

import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.PointsReader;
import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.FilterCodecReader;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.OneMergeWrappingMergePolicy;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.index.Terms;
import org.apache.lucene.util.FilterIterator;
import org.elasticsearch.index.engine.FilterDocValuesProducer;
import org.elasticsearch.index.engine.FilterFieldsProducer;
import org.elasticsearch.index.engine.FilterStoredFieldVisitor;
import org.elasticsearch.index.engine.FilterStoredFieldsReader;
import org.elasticsearch.index.engine.FilteredPointsReader;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.SeqNoFieldMapper;

import java.io.IOException;
import java.util.Iterator;
import java.util.Set;
import java.util.function.LongSupplier;
import java.util.stream.StreamSupport;

/**
 * A merge policy that prunes _id, _seq_no and _primary_term fields from segments with max seqno value that
 * is higher than the minimum retained seqno.
 */
public final class PruneFieldsMergePolicy extends OneMergeWrappingMergePolicy {

    private static final Set<String> FIELDS_TO_PRUNE = Set.of(
        IdFieldMapper.NAME,
        SeqNoFieldMapper.NAME,
        SeqNoFieldMapper.PRIMARY_TERM_NAME
    );

    public PruneFieldsMergePolicy(MergePolicy in, LongSupplier minRetainedSeqNoSupplier) {
        super(in, toWrap -> new OneMerge(toWrap.segments) {

            @Override
            public CodecReader wrapForMerge(CodecReader reader) throws IOException {
                CodecReader wrapped = toWrap.wrapForMerge(reader);
                return wrap(wrapped, minRetainedSeqNoSupplier);
            }

        });
    }

    static CodecReader wrap(CodecReader in, LongSupplier minRetainedSeqNoSupplier) throws IOException {
        PointValues pointValues = in.getPointValues(SeqNoFieldMapper.NAME);
        if (pointValues == null) {
            return in;
        }

        long minRetainedSeqNo = minRetainedSeqNoSupplier.getAsLong();
        long maxSeqNo = LongPoint.decodeDimension(pointValues.getMaxPackedValue(), 0);
        if (minRetainedSeqNo <= maxSeqNo) {
            return in;
        }

        FieldInfo[] filteredInfos = StreamSupport.stream(in.getFieldInfos().spliterator(), false)
            .filter(fi -> FIELDS_TO_PRUNE.contains(fi.name) == false)
            .toArray(FieldInfo[]::new);
        final FieldInfos filteredFieldInfos = new FieldInfos(filteredInfos);
        return new FilterCodecReader(in) {

            @Override
            public FieldInfos getFieldInfos() {
                return filteredFieldInfos;
            }

            @Override
            public StoredFieldsReader getFieldsReader() {
                StoredFieldsReader fieldsReader = super.getFieldsReader();
                return new FilterStoredFieldsReader(fieldsReader) {
                    @Override
                    public void visitDocument(int docID, StoredFieldVisitor visitor) throws IOException {
                        super.visitDocument(docID, new FilterStoredFieldVisitor(visitor) {
                            @Override
                            public Status needsField(FieldInfo fieldInfo) throws IOException {
                                if (FIELDS_TO_PRUNE.contains(fieldInfo.name)) {
                                    return Status.NO;
                                }
                                return super.needsField(fieldInfo);
                            }
                        });
                    }
                };
            }

            @Override
            public DocValuesProducer getDocValuesReader() {
                DocValuesProducer docValuesReader = super.getDocValuesReader();
                return new FilterDocValuesProducer(docValuesReader) {
                    @Override
                    public NumericDocValues getNumeric(FieldInfo field) throws IOException {
                        NumericDocValues numeric = super.getNumeric(field);
                        if (FIELDS_TO_PRUNE.contains(field.name)) {
                            return DocValues.emptyNumeric();
                        } else {
                            return numeric;
                        }
                    }
                };
            }

            @Override
            public PointsReader getPointsReader() {
                PointsReader pointsReader = super.getPointsReader();
                return new FilteredPointsReader(pointsReader) {
                    @Override
                    public PointValues getValues(String field) throws IOException {
                        PointValues pointValues = super.getValues(field);
                        if (FIELDS_TO_PRUNE.contains(field)) {
                            return null;
                        } else {
                            return pointValues;
                        }
                    }
                };
            }

            @Override
            public FieldsProducer getPostingsReader() {
                FieldsProducer fieldsProducer = super.getPostingsReader();
                return new FilterFieldsProducer(fieldsProducer) {

                    @Override
                    public int size() {
                        return -1;
                    }

                    @Override
                    public Iterator<String> iterator() {
                        return new FilterIterator<>(super.iterator()) {
                            @Override
                            protected boolean predicateFunction(String field) {
                                return FIELDS_TO_PRUNE.contains(field) == false;
                            }
                        };
                    }

                    @Override
                    public Terms terms(String field) throws IOException {
                        if (FIELDS_TO_PRUNE.contains(field)) {
                            return null;
                        } else {
                            return super.terms(field);
                        }
                    }
                };
            }

            @Override
            public CacheHelper getCoreCacheHelper() {
                return null;
            }

            @Override
            public CacheHelper getReaderCacheHelper() {
                return null;
            }
        };
    }
}
