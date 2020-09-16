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

import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;

import java.io.IOException;

public abstract class FilterDocValuesProducer extends DocValuesProducer {
    private final DocValuesProducer in;

    protected FilterDocValuesProducer(DocValuesProducer in) {
        this.in = in;
    }

    @Override
    public NumericDocValues getNumeric(FieldInfo field) throws IOException {
        return in.getNumeric(field);
    }

    @Override
    public BinaryDocValues getBinary(FieldInfo field) throws IOException {
        return in.getBinary(field);
    }

    @Override
    public SortedDocValues getSorted(FieldInfo field) throws IOException {
        return in.getSorted(field);
    }

    @Override
    public SortedNumericDocValues getSortedNumeric(FieldInfo field) throws IOException {
        return in.getSortedNumeric(field);
    }

    @Override
    public SortedSetDocValues getSortedSet(FieldInfo field) throws IOException {
        return in.getSortedSet(field);
    }

    @Override
    public void checkIntegrity() throws IOException {
        in.checkIntegrity();
    }

    @Override
    public void close() throws IOException {
        in.close();
    }

    @Override
    public long ramBytesUsed() {
        return in.ramBytesUsed();
    }
}
