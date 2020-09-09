/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.engine;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.ForceMergePolicy;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.LogByteSizeMergePolicy;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.SerialMergeScheduler;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.SeqNoFieldMapper;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.BitSet;

import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class PruneFieldsMergePolicyTests extends ESTestCase {

    public void testPruningFields() throws Exception {
        try (Directory dir = newDirectory()) {
            long[] minRetainedSeqNo = new long[1];
            IndexWriterConfig iwc = newIndexWriterConfig();
            iwc.setMergeScheduler(new SerialMergeScheduler());
            iwc.setMergePolicy(new PruneFieldsMergePolicy(new LogByteSizeMergePolicy(), () -> minRetainedSeqNo[0]));
            try (IndexWriter writer = new IndexWriter(dir, iwc)) {
                writer.addDocument(doc("001", 0, 0, "name1"));
                writer.addDocument(doc("002", 0, 0, "name2"));
                writer.addDocument(doc("003", 0, 0, "name3"));
                writer.flush();
                writer.addDocument(doc("004", 0, 0, "name4"));
                writer.addDocument(doc("005", 1, 0, "name5"));
                writer.addDocument(doc("006", 1, 0, "name6"));
                writer.flush();
                writer.addDocument(doc("007", 1, 0, "name7"));
                writer.addDocument(doc("008", 1, 0, "name8"));
                writer.addDocument(doc("009", 1, 0, "name9"));

                // Ensure that the index is in expected state:
                try (DirectoryReader reader = DirectoryReader.open(writer)) {
                    assertThat(reader.leaves().size(), equalTo(3));

                    LeafReader leaf = reader.leaves().get(0).reader();
                    assertTerms(leaf, 0, IdFieldMapper.NAME, "001");
                    assertTerms(leaf, 1, IdFieldMapper.NAME, "002");
                    assertTerms(leaf, 2, IdFieldMapper.NAME, "003");
                    assertStoredField(leaf, 0, IdFieldMapper.NAME, "001");
                    assertStoredField(leaf, 1, IdFieldMapper.NAME, "002");
                    assertStoredField(leaf, 2, IdFieldMapper.NAME, "003");
                    assertPointValues(leaf, 0, SeqNoFieldMapper.NAME, 0);
                    assertPointValues(leaf, 1, SeqNoFieldMapper.NAME, 0);
                    assertPointValues(leaf, 2, SeqNoFieldMapper.NAME, 0);
                    assertNumericDocValues(leaf, 0, SeqNoFieldMapper.NAME, 0);
                    assertNumericDocValues(leaf, 1, SeqNoFieldMapper.NAME, 0);
                    assertNumericDocValues(leaf, 2, SeqNoFieldMapper.NAME, 0);
                    assertNumericDocValues(leaf, 0, SeqNoFieldMapper.PRIMARY_TERM_NAME, 0);
                    assertNumericDocValues(leaf, 1, SeqNoFieldMapper.PRIMARY_TERM_NAME, 0);
                    assertNumericDocValues(leaf, 2, SeqNoFieldMapper.PRIMARY_TERM_NAME, 0);
                    assertStoredField(leaf, 0, "name", "name1");
                    assertStoredField(leaf, 1, "name", "name2");
                    assertStoredField(leaf, 2, "name", "name3");

                    leaf = reader.leaves().get(1).reader();
                    assertTerms(leaf, 0, IdFieldMapper.NAME, "004");
                    assertTerms(leaf, 1, IdFieldMapper.NAME, "005");
                    assertTerms(leaf, 2, IdFieldMapper.NAME, "006");
                    assertStoredField(leaf, 0, IdFieldMapper.NAME, "004");
                    assertStoredField(leaf, 1, IdFieldMapper.NAME, "005");
                    assertStoredField(leaf, 2, IdFieldMapper.NAME, "006");
                    assertPointValues(leaf, 0, SeqNoFieldMapper.NAME, 0);
                    assertPointValues(leaf, 1, SeqNoFieldMapper.NAME, 1);
                    assertPointValues(leaf, 2, SeqNoFieldMapper.NAME, 1);
                    assertNumericDocValues(leaf, 0, SeqNoFieldMapper.NAME, 0);
                    assertNumericDocValues(leaf, 1, SeqNoFieldMapper.NAME, 1);
                    assertNumericDocValues(leaf, 2, SeqNoFieldMapper.NAME, 1);
                    assertNumericDocValues(leaf, 0, SeqNoFieldMapper.PRIMARY_TERM_NAME, 0);
                    assertNumericDocValues(leaf, 1, SeqNoFieldMapper.PRIMARY_TERM_NAME, 0);
                    assertNumericDocValues(leaf, 2, SeqNoFieldMapper.PRIMARY_TERM_NAME, 0);
                    assertStoredField(leaf, 0, "name", "name4");
                    assertStoredField(leaf, 1, "name", "name5");
                    assertStoredField(leaf, 2, "name", "name6");

                    leaf = reader.leaves().get(2).reader();
                    assertTerms(leaf, 0, IdFieldMapper.NAME, "007");
                    assertTerms(leaf, 1, IdFieldMapper.NAME, "008");
                    assertTerms(leaf, 2, IdFieldMapper.NAME, "009");
                    assertStoredField(leaf, 0, IdFieldMapper.NAME, "007");
                    assertStoredField(leaf, 1, IdFieldMapper.NAME, "008");
                    assertStoredField(leaf, 2, IdFieldMapper.NAME, "009");
                    assertPointValues(leaf, 0, SeqNoFieldMapper.NAME, 1);
                    assertPointValues(leaf, 1, SeqNoFieldMapper.NAME, 1);
                    assertPointValues(leaf, 2, SeqNoFieldMapper.NAME, 1);
                    assertNumericDocValues(leaf, 0, SeqNoFieldMapper.NAME, 1);
                    assertNumericDocValues(leaf, 1, SeqNoFieldMapper.NAME, 1);
                    assertNumericDocValues(leaf, 2, SeqNoFieldMapper.NAME, 1);
                    assertNumericDocValues(leaf, 0, SeqNoFieldMapper.PRIMARY_TERM_NAME, 0);
                    assertNumericDocValues(leaf, 1, SeqNoFieldMapper.PRIMARY_TERM_NAME, 0);
                    assertNumericDocValues(leaf, 2, SeqNoFieldMapper.PRIMARY_TERM_NAME, 0);
                    assertStoredField(leaf, 0, "name", "name7");
                    assertStoredField(leaf, 1, "name", "name8");
                    assertStoredField(leaf, 2, "name", "name9");
                }

                minRetainedSeqNo[0] = 1;
                writer.forceMerge(1);
                // Check that docs with seqno 0 have no _id, _seq_no and _primary_term fields.
                try (DirectoryReader reader = DirectoryReader.open(writer)) {
                    assertThat(reader.leaves().size(), equalTo(1));

                    LeafReader leaf = reader.leaves().get(0).reader();
                    assertTerms(leaf, 0, IdFieldMapper.NAME, null);
                    assertTerms(leaf, 1, IdFieldMapper.NAME, null);
                    assertTerms(leaf, 2, IdFieldMapper.NAME, null);
                    assertStoredField(leaf, 0, IdFieldMapper.NAME, null);
                    assertStoredField(leaf, 1, IdFieldMapper.NAME, null);
                    assertStoredField(leaf, 2, IdFieldMapper.NAME, null);
                    assertPointValues(leaf, 0, SeqNoFieldMapper.NAME, -1);
                    assertPointValues(leaf, 1, SeqNoFieldMapper.NAME, -1);
                    assertPointValues(leaf, 2, SeqNoFieldMapper.NAME, -1);
                    assertNumericDocValues(leaf, 0, SeqNoFieldMapper.NAME, -1);
                    assertNumericDocValues(leaf, 1, SeqNoFieldMapper.NAME, -1);
                    assertNumericDocValues(leaf, 2, SeqNoFieldMapper.NAME, -1);
                    assertNumericDocValues(leaf, 0, SeqNoFieldMapper.PRIMARY_TERM_NAME, -1);
                    assertNumericDocValues(leaf, 1, SeqNoFieldMapper.PRIMARY_TERM_NAME, -1);
                    assertNumericDocValues(leaf, 2, SeqNoFieldMapper.PRIMARY_TERM_NAME, -1);
                    assertStoredField(leaf, 0, "name", "name1");
                    assertStoredField(leaf, 1, "name", "name2");
                    assertStoredField(leaf, 2, "name", "name3");

                    assertTerms(leaf, 3, IdFieldMapper.NAME, "004");
                    assertTerms(leaf, 4, IdFieldMapper.NAME, "005");
                    assertTerms(leaf, 5, IdFieldMapper.NAME, "006");
                    assertStoredField(leaf, 3, IdFieldMapper.NAME, "004");
                    assertStoredField(leaf, 4, IdFieldMapper.NAME, "005");
                    assertStoredField(leaf, 5, IdFieldMapper.NAME, "006");
                    assertPointValues(leaf, 3, SeqNoFieldMapper.NAME, 0);
                    assertPointValues(leaf, 4, SeqNoFieldMapper.NAME, 1);
                    assertPointValues(leaf, 5, SeqNoFieldMapper.NAME, 1);
                    assertNumericDocValues(leaf, 3, SeqNoFieldMapper.NAME, 0);
                    assertNumericDocValues(leaf, 4, SeqNoFieldMapper.NAME, 1);
                    assertNumericDocValues(leaf, 5, SeqNoFieldMapper.NAME, 1);
                    assertNumericDocValues(leaf, 3, SeqNoFieldMapper.PRIMARY_TERM_NAME, 0);
                    assertNumericDocValues(leaf, 4, SeqNoFieldMapper.PRIMARY_TERM_NAME, 0);
                    assertNumericDocValues(leaf, 5, SeqNoFieldMapper.PRIMARY_TERM_NAME, 0);
                    assertStoredField(leaf, 3, "name", "name4");
                    assertStoredField(leaf, 4, "name", "name5");
                    assertStoredField(leaf, 5, "name", "name6");

                    assertTerms(leaf, 6, IdFieldMapper.NAME, "007");
                    assertTerms(leaf, 7, IdFieldMapper.NAME, "008");
                    assertTerms(leaf, 8, IdFieldMapper.NAME, "009");
                    assertStoredField(leaf, 6, IdFieldMapper.NAME, "007");
                    assertStoredField(leaf, 7, IdFieldMapper.NAME, "008");
                    assertStoredField(leaf, 8, IdFieldMapper.NAME, "009");
                    assertPointValues(leaf, 6, SeqNoFieldMapper.NAME, 1);
                    assertPointValues(leaf, 7, SeqNoFieldMapper.NAME, 1);
                    assertPointValues(leaf, 8, SeqNoFieldMapper.NAME, 1);
                    assertNumericDocValues(leaf, 6, SeqNoFieldMapper.NAME, 1);
                    assertNumericDocValues(leaf, 7, SeqNoFieldMapper.NAME, 1);
                    assertNumericDocValues(leaf, 8, SeqNoFieldMapper.NAME, 1);
                    assertNumericDocValues(leaf, 6, SeqNoFieldMapper.PRIMARY_TERM_NAME, 0);
                    assertNumericDocValues(leaf, 7, SeqNoFieldMapper.PRIMARY_TERM_NAME, 0);
                    assertNumericDocValues(leaf, 8, SeqNoFieldMapper.PRIMARY_TERM_NAME, 0);
                    assertStoredField(leaf, 6, "name", "name7");
                    assertStoredField(leaf, 7, "name", "name8");
                    assertStoredField(leaf, 8, "name", "name9");
                }

                writer.addDocument(doc("010", 1, 0, "name10"));
                writer.flush();
                minRetainedSeqNo[0] = 2;
                writer.forceMerge(1);
                // Check that all docs have no _id, _seq_no and _primary_term fields.
                // Since minRetainedSeqNo is higher than all seqno in the index,
                // the data structures should not exist for these fields.
                try (DirectoryReader reader = DirectoryReader.open(writer)) {
                    assertThat(reader.leaves().size(), equalTo(1));
                    assertThat(reader.maxDoc(), equalTo(10));

                    LeafReader leaf = reader.leaves().get(0).reader();
                    assertThat(leaf.terms(IdFieldMapper.NAME), nullValue());
                    assertStoredField(leaf, 0, IdFieldMapper.NAME, null);
                    assertStoredField(leaf, 1, IdFieldMapper.NAME, null);
                    assertStoredField(leaf, 2, IdFieldMapper.NAME, null);
                    assertThat(leaf.getPointValues(SeqNoFieldMapper.NAME), nullValue());
                    assertThat(leaf.getNumericDocValues(SeqNoFieldMapper.NAME), nullValue());
                    assertThat(leaf.getNumericDocValues(SeqNoFieldMapper.PRIMARY_TERM_NAME), nullValue());
                    assertStoredField(leaf, 0, "name", "name1");
                    assertStoredField(leaf, 1, "name", "name2");
                    assertStoredField(leaf, 2, "name", "name3");

                    assertStoredField(leaf, 3, IdFieldMapper.NAME, null);
                    assertStoredField(leaf, 4, IdFieldMapper.NAME, null);
                    assertStoredField(leaf, 5, IdFieldMapper.NAME, null);
                    assertStoredField(leaf, 3, "name", "name4");
                    assertStoredField(leaf, 4, "name", "name5");
                    assertStoredField(leaf, 5, "name", "name6");

                    assertStoredField(leaf, 6, IdFieldMapper.NAME, null);
                    assertStoredField(leaf, 7, IdFieldMapper.NAME, null);
                    assertStoredField(leaf, 8, IdFieldMapper.NAME, null);
                    assertStoredField(leaf, 6, "name", "name7");
                    assertStoredField(leaf, 7, "name", "name8");
                    assertStoredField(leaf, 8, "name", "name9");
                    assertStoredField(leaf, 9, "name", "name10");
                }
            }
        }
    }

    private static Document doc(String id, long seqNo, long primaryTerm, String name) {
        Document document = new Document();
        document.add(new Field(IdFieldMapper.NAME, id, IdFieldMapper.Defaults.FIELD_TYPE));

        SeqNoFieldMapper.SequenceIDFields seqID = SeqNoFieldMapper.SequenceIDFields.emptySeqID();
        seqID.primaryTerm.setLongValue(primaryTerm);
        seqID.seqNo.setLongValue(seqNo);
        seqID.seqNoDocValue.setLongValue(seqNo);
        document.add(seqID.seqNo);
        document.add(seqID.seqNoDocValue);
        document.add(seqID.primaryTerm);

        document.add(new StoredField("name", name));
        return document;
    }

    private static void assertStoredField(LeafReader reader, int docId, String fieldName, String expectedValue) throws IOException {
        Document document = reader.document(docId);
        assertThat(document.get(fieldName), equalTo(expectedValue));
    }

    private static void assertNumericDocValues(LeafReader reader, int docId, String fieldName, long expectedValue) throws IOException {
        NumericDocValues docValues = reader.getNumericDocValues(fieldName);
        assertThat(docValues, notNullValue());
        if (expectedValue != -1) {
            assertThat(docValues.advanceExact(docId), is(true));
            assertThat(docValues.longValue(), equalTo(expectedValue));
        } else {
            assertThat(docValues.advanceExact(docId), is(false));
        }
    }

    private static void assertPointValues(LeafReader reader, int expectedDocId, String fieldName, long expectedValue) throws IOException {
        PointValues pointValues = reader.getPointValues(fieldName);
        assertThat(pointValues, notNullValue());

        long[] value = new long[]{-1};
        pointValues.intersect(new PointValues.IntersectVisitor() {
            @Override
            public void visit(int docID) throws IOException {
                throw new UnsupportedEncodingException();
            }

            @Override
            public void visit(int docID, byte[] packedValue) throws IOException {
                if (docID == expectedDocId) {
                    value[0] = LongPoint.decodeDimension(packedValue, 0);
                }
            }

            @Override
            public PointValues.Relation compare(byte[] minPacked, byte[] maxPacked) {
                return PointValues.Relation.CELL_CROSSES_QUERY;
            }
        });
        assertThat(value[0], equalTo(expectedValue));
    }

    private static void assertTerms(LeafReader reader, int expectedDocId, String fieldName, String expectedValue) throws IOException {
        Terms terms = reader.terms(fieldName);
        assertThat(terms, notNullValue());
        TermsEnum tenum = terms.iterator();
        String value = null;
        for (BytesRef term = tenum.next(); term != null; term = tenum.next()) {
            PostingsEnum penum = tenum.postings(null);
            for (int docId = penum.nextDoc(); docId != DocIdSetIterator.NO_MORE_DOCS; docId = penum.nextDoc()) {
                if (docId == expectedDocId) {
                    value = term.utf8ToString();
                    break;
                }
            }
        }
        assertThat(value, equalTo(expectedValue));
    }
}
