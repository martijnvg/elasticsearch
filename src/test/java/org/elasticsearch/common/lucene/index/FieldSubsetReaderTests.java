/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.lucene.index;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.TestUtil;
import org.elasticsearch.index.mapper.internal.FieldNamesFieldMapper;
import org.elasticsearch.index.mapper.internal.SourceFieldMapper;
import org.elasticsearch.test.ElasticsearchLuceneTestCase;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/** Simple tests for this filterreader */
public class FieldSubsetReaderTests extends ElasticsearchLuceneTestCase {
    
    /**
     * test filtering two string fields
     */
    public void testIndexed() throws Exception {
        Directory dir = newDirectory();
        IndexWriterConfig iwc = new IndexWriterConfig(null);
        IndexWriter iw = new IndexWriter(dir, iwc);
        
        // add document with 2 fields
        Document doc = new Document();
        doc.add(new StringField("fieldA", "test", Field.Store.NO));
        doc.add(new StringField("fieldB", "test", Field.Store.NO));
        iw.addDocument(doc);
        
        // open reader
        Set<String> fields = Collections.singleton("fieldA");
        DirectoryReader ir = FieldSubsetReader.wrap(DirectoryReader.open(iw, true), fields, fields);
        
        // see only one field
        LeafReader segmentReader = ir.leaves().get(0).reader();
        Set<String> seenFields = new HashSet<>();
        for (String field : segmentReader.fields()) {
            seenFields.add(field);
        }
        assertEquals(Collections.singleton("fieldA"), seenFields);
        
        TestUtil.checkReader(ir);
        IOUtils.close(ir, iw, dir);
    }
    
    /**
     * test filtering two stored fields
     */
    public void testStoredFields() throws Exception {
        Directory dir = newDirectory();
        IndexWriterConfig iwc = new IndexWriterConfig(null);
        IndexWriter iw = new IndexWriter(dir, iwc);
        
        // add document with 2 fields
        Document doc = new Document();
        doc.add(new StoredField("fieldA", "testA"));
        doc.add(new StoredField("fieldB", "testB"));
        iw.addDocument(doc);
        
        // open reader
        Set<String> fields = Collections.singleton("fieldA");
        DirectoryReader ir = FieldSubsetReader.wrap(DirectoryReader.open(iw, true), fields, fields);
        
        // see only one field
        Document d2 = ir.document(0);
        assertEquals(1, d2.getFields().size());
        assertEquals("testA", d2.get("fieldA"));
        
        TestUtil.checkReader(ir);
        IOUtils.close(ir, iw, dir);
    }
    
    /**
     * test filtering two vector fields
     */
    public void testVectors() throws Exception {
        Directory dir = newDirectory();
        IndexWriterConfig iwc = new IndexWriterConfig(null);
        IndexWriter iw = new IndexWriter(dir, iwc);
        
        // add document with 2 fields
        Document doc = new Document();
        FieldType ft = new FieldType(StringField.TYPE_NOT_STORED);
        ft.setStoreTermVectors(true);
        doc.add(new Field("fieldA", "testA", ft));
        doc.add(new Field("fieldB", "testB", ft));
        iw.addDocument(doc);
        
        // open reader
        Set<String> fields = Collections.singleton("fieldA");
        DirectoryReader ir = FieldSubsetReader.wrap(DirectoryReader.open(iw, true), fields, fields);
        
        // see only one field
        Fields vectors = ir.getTermVectors(0);
        Set<String> seenFields = new HashSet<>();
        for (String field : vectors) {
            seenFields.add(field);
        }
        assertEquals(Collections.singleton("fieldA"), seenFields);
        
        TestUtil.checkReader(ir);
        IOUtils.close(ir, iw, dir);
    }
    
    /**
     * test filtering two text fields
     */
    public void testNorms() throws Exception {
        Directory dir = newDirectory();
        IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
        IndexWriter iw = new IndexWriter(dir, iwc);
        
        // add document with 2 fields
        Document doc = new Document();
        doc.add(new TextField("fieldA", "test", Field.Store.NO));
        doc.add(new TextField("fieldB", "test", Field.Store.NO));
        iw.addDocument(doc);
        
        // open reader
        Set<String> fields = Collections.singleton("fieldA");
        DirectoryReader ir = FieldSubsetReader.wrap(DirectoryReader.open(iw, true), fields, fields);
        
        // see only one field
        LeafReader segmentReader = ir.leaves().get(0).reader();
        assertNotNull(segmentReader.getNormValues("fieldA"));
        assertNull(segmentReader.getNormValues("fieldB"));
        
        TestUtil.checkReader(ir);
        IOUtils.close(ir, iw, dir);
    }
    
    /**
     * test filtering two numeric dv fields
     */
    public void testNumericDocValues() throws Exception {
        Directory dir = newDirectory();
        IndexWriterConfig iwc = new IndexWriterConfig(null);
        IndexWriter iw = new IndexWriter(dir, iwc);
        
        // add document with 2 fields
        Document doc = new Document();
        doc.add(new NumericDocValuesField("fieldA", 1));
        doc.add(new NumericDocValuesField("fieldB", 2));
        iw.addDocument(doc);
        
        // open reader
        Set<String> fields = Collections.singleton("fieldA");
        DirectoryReader ir = FieldSubsetReader.wrap(DirectoryReader.open(iw, true), fields, fields);
        
        // see only one field
        LeafReader segmentReader = ir.leaves().get(0).reader();
        assertNotNull(segmentReader.getNumericDocValues("fieldA"));
        assertEquals(1, segmentReader.getNumericDocValues("fieldA").get(0));
        assertNull(segmentReader.getNumericDocValues("fieldB"));
        
        TestUtil.checkReader(ir);
        IOUtils.close(ir, iw, dir);
    }
    
    /**
     * test filtering two binary dv fields
     */
    public void testBinaryDocValues() throws Exception {
        Directory dir = newDirectory();
        IndexWriterConfig iwc = new IndexWriterConfig(null);
        IndexWriter iw = new IndexWriter(dir, iwc);
        
        // add document with 2 fields
        Document doc = new Document();
        doc.add(new BinaryDocValuesField("fieldA", new BytesRef("testA")));
        doc.add(new BinaryDocValuesField("fieldB", new BytesRef("testB")));
        iw.addDocument(doc);
        
        // open reader
        Set<String> fields = Collections.singleton("fieldA");
        DirectoryReader ir = FieldSubsetReader.wrap(DirectoryReader.open(iw, true), fields, fields);
        
        // see only one field
        LeafReader segmentReader = ir.leaves().get(0).reader();
        assertNotNull(segmentReader.getBinaryDocValues("fieldA"));
        assertEquals(new BytesRef("testA"), segmentReader.getBinaryDocValues("fieldA").get(0));
        assertNull(segmentReader.getBinaryDocValues("fieldB"));

        TestUtil.checkReader(ir);
        IOUtils.close(ir, iw, dir);
    }
    
    /**
     * test filtering two sorted dv fields
     */
    public void testSortedDocValues() throws Exception {
        Directory dir = newDirectory();
        IndexWriterConfig iwc = new IndexWriterConfig(null);
        IndexWriter iw = new IndexWriter(dir, iwc);
        
        // add document with 2 fields
        Document doc = new Document();
        doc.add(new SortedDocValuesField("fieldA", new BytesRef("testA")));
        doc.add(new SortedDocValuesField("fieldB", new BytesRef("testB")));
        iw.addDocument(doc);
        
        // open reader
        Set<String> fields = Collections.singleton("fieldA");
        DirectoryReader ir = FieldSubsetReader.wrap(DirectoryReader.open(iw, true), fields, fields);
        
        // see only one field
        LeafReader segmentReader = ir.leaves().get(0).reader();
        assertNotNull(segmentReader.getSortedDocValues("fieldA"));
        assertEquals(new BytesRef("testA"), segmentReader.getSortedDocValues("fieldA").get(0));
        assertNull(segmentReader.getSortedDocValues("fieldB"));
        
        TestUtil.checkReader(ir);
        IOUtils.close(ir, iw, dir);
    }
    
    /**
     * test filtering two sortedset dv fields
     */
    public void testSortedSetDocValues() throws Exception {
        Directory dir = newDirectory();
        IndexWriterConfig iwc = new IndexWriterConfig(null);
        IndexWriter iw = new IndexWriter(dir, iwc);
        
        // add document with 2 fields
        Document doc = new Document();
        doc.add(new SortedSetDocValuesField("fieldA", new BytesRef("testA")));
        doc.add(new SortedSetDocValuesField("fieldB", new BytesRef("testB")));
        iw.addDocument(doc);
        
        // open reader
        Set<String> fields = Collections.singleton("fieldA");
        DirectoryReader ir = FieldSubsetReader.wrap(DirectoryReader.open(iw, true), fields, fields);
        
        // see only one field
        LeafReader segmentReader = ir.leaves().get(0).reader();
        SortedSetDocValues dv = segmentReader.getSortedSetDocValues("fieldA");
        assertNotNull(dv);
        dv.setDocument(0);
        assertEquals(0, dv.nextOrd());
        assertEquals(SortedSetDocValues.NO_MORE_ORDS, dv.nextOrd());
        assertEquals(new BytesRef("testA"), dv.lookupOrd(0));
        assertNull(segmentReader.getSortedSetDocValues("fieldB"));
        
        TestUtil.checkReader(ir);
        IOUtils.close(ir, iw, dir);
    }
    
    /**
     * test filtering two sortednumeric dv fields
     */
    public void testSortedNumericDocValues() throws Exception {
        Directory dir = newDirectory();
        IndexWriterConfig iwc = new IndexWriterConfig(null);
        IndexWriter iw = new IndexWriter(dir, iwc);
        
        // add document with 2 fields
        Document doc = new Document();
        doc.add(new SortedNumericDocValuesField("fieldA", 1));
        doc.add(new SortedNumericDocValuesField("fieldB", 2));
        iw.addDocument(doc);
        
        // open reader
        Set<String> fields = Collections.singleton("fieldA");
        DirectoryReader ir = FieldSubsetReader.wrap(DirectoryReader.open(iw, true), fields, fields);
        
        // see only one field
        LeafReader segmentReader = ir.leaves().get(0).reader();
        SortedNumericDocValues dv = segmentReader.getSortedNumericDocValues("fieldA");
        assertNotNull(dv);
        dv.setDocument(0);
        assertEquals(1, dv.count());
        assertEquals(1, dv.valueAt(0));
        assertNull(segmentReader.getSortedSetDocValues("fieldB"));
        
        TestUtil.checkReader(ir);
        IOUtils.close(ir, iw, dir);
    }
    
    /**
     * test we have correct fieldinfos metadata
     */
    public void testFieldInfos() throws Exception {
        Directory dir = newDirectory();
        IndexWriterConfig iwc = new IndexWriterConfig(null);
        IndexWriter iw = new IndexWriter(dir, iwc);
        
        // add document with 2 fields
        Document doc = new Document();
        doc.add(new StringField("fieldA", "test", Field.Store.NO));
        doc.add(new StringField("fieldB", "test", Field.Store.NO));
        iw.addDocument(doc);
        
        // open reader
        Set<String> fields = Collections.singleton("fieldA");
        DirectoryReader ir = FieldSubsetReader.wrap(DirectoryReader.open(iw, true), fields, fields);
        
        // see only one field
        LeafReader segmentReader = ir.leaves().get(0).reader();
        FieldInfos infos = segmentReader.getFieldInfos();
        assertEquals(1, infos.size());
        assertNotNull(infos.fieldInfo("fieldA"));
        assertNull(infos.fieldInfo("fieldB"));
        
        TestUtil.checkReader(ir);
        IOUtils.close(ir, iw, dir);
    }
    
    /**
     * test special handling for _source field.
     */
    public void testSourceFiltering() throws Exception {
        Directory dir = newDirectory();
        IndexWriterConfig iwc = new IndexWriterConfig(null);
        IndexWriter iw = new IndexWriter(dir, iwc);
        
        // add document with 2 fields
        Document doc = new Document();
        doc.add(new StringField("fieldA", "testA", Field.Store.NO));
        doc.add(new StringField("fieldB", "testB", Field.Store.NO));
        byte bytes[] = "{\"fieldA\":\"testA\", \"fieldB\":\"testB\"}".getBytes(StandardCharsets.UTF_8);
        doc.add(new StoredField(SourceFieldMapper.NAME, bytes, 0, bytes.length));
        iw.addDocument(doc);
        
        // open reader
        Set<String> fields = new HashSet<>();
        fields.add("fieldA");
        fields.add(SourceFieldMapper.NAME);
        DirectoryReader ir = FieldSubsetReader.wrap(DirectoryReader.open(iw, true), fields, fields);
        
        // see only one field
        Document d2 = ir.document(0);
        assertEquals(1, d2.getFields().size());
        assertEquals("{\"fieldA\":\"testA\"}", d2.getBinaryValue(SourceFieldMapper.NAME).utf8ToString());
        
        TestUtil.checkReader(ir);
        IOUtils.close(ir, iw, dir);
    }
    
    /**
     * test special handling for _field_names field.
     */
    public void testFieldNames() throws Exception {
        Directory dir = newDirectory();
        IndexWriterConfig iwc = new IndexWriterConfig(null);
        IndexWriter iw = new IndexWriter(dir, iwc);
        
        // add document with 2 fields
        Document doc = new Document();
        doc.add(new StringField("fieldA", "test", Field.Store.NO));
        doc.add(new StringField("fieldB", "test", Field.Store.NO));
        doc.add(new StringField(FieldNamesFieldMapper.NAME, "fieldA", Field.Store.NO));
        doc.add(new StringField(FieldNamesFieldMapper.NAME, "fieldB", Field.Store.NO));
        iw.addDocument(doc);
        
        // open reader
        Set<String> fields = new HashSet<>();
        fields.add("fieldA");
        fields.add(FieldNamesFieldMapper.NAME);
        DirectoryReader ir = FieldSubsetReader.wrap(DirectoryReader.open(iw, true), fields, fields);
        
        // see only one field
        LeafReader segmentReader = ir.leaves().get(0).reader();
        Terms terms = segmentReader.terms(FieldNamesFieldMapper.NAME);
        TermsEnum termsEnum = terms.iterator(null);
        assertEquals(new BytesRef("fieldA"), termsEnum.next());
        assertNull(termsEnum.next());
        
        TestUtil.checkReader(ir);
        IOUtils.close(ir, iw, dir);
    }
    
    /** test that core cache key (needed for NRT) is working */
    public void testCoreCacheKey() throws Exception {
        Directory dir = newDirectory();
        IndexWriterConfig iwc = new IndexWriterConfig(null);
        iwc.setMaxBufferedDocs(100);
        iwc.setMergePolicy(NoMergePolicy.INSTANCE);
        IndexWriter iw = new IndexWriter(dir, iwc);
        
        // add two docs, id:0 and id:1
        Document doc = new Document();
        Field idField = new StringField("id", "", Field.Store.NO);
        doc.add(idField);
        idField.setStringValue("0");
        iw.addDocument(doc);
        idField.setStringValue("1");
        iw.addDocument(doc);
        
        // open reader
        Set<String> fields = Collections.singleton("id");
        DirectoryReader ir = FieldSubsetReader.wrap(DirectoryReader.open(iw, true), fields, fields);
        assertEquals(2, ir.numDocs());
        assertEquals(1, ir.leaves().size());

        // delete id:0 and reopen
        iw.deleteDocuments(new Term("id", "0"));
        DirectoryReader ir2 = DirectoryReader.openIfChanged(ir);
        
        // we should have the same cache key as before
        assertEquals(1, ir2.numDocs());
        assertEquals(1, ir2.leaves().size());
        assertSame(ir.leaves().get(0).reader().getCoreCacheKey(), ir2.leaves().get(0).reader().getCoreCacheKey());
        
        // this is kind of stupid, but for now its here
        assertNotSame(ir.leaves().get(0).reader().getCombinedCoreAndDeletesKey(), ir2.leaves().get(0).reader().getCombinedCoreAndDeletesKey());
        
        TestUtil.checkReader(ir);
        IOUtils.close(ir, ir2, iw, dir);
    }
}
