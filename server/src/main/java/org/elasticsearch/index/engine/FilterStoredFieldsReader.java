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

import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.index.StoredFieldVisitor;

import java.io.IOException;

abstract class FilterStoredFieldsReader extends StoredFieldsReader {

    private final StoredFieldsReader fieldsReader;

    protected FilterStoredFieldsReader(StoredFieldsReader fieldsReader) {
        this.fieldsReader = fieldsReader;
    }

    @Override
    public long ramBytesUsed() {
        return fieldsReader.ramBytesUsed();
    }

    @Override
    public void close() throws IOException {
        fieldsReader.close();
    }

    @Override
    public void visitDocument(int docID, StoredFieldVisitor visitor) throws IOException {
        fieldsReader.visitDocument(docID, visitor);
    }

    @Override
    public StoredFieldsReader clone() {
        return fieldsReader.clone();
    }

    @Override
    public void checkIntegrity() throws IOException {
        fieldsReader.checkIntegrity();
    }
}
