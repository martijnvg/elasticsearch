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

public abstract class FilterStoredFieldsReader extends StoredFieldsReader {

    protected final StoredFieldsReader in;

    protected FilterStoredFieldsReader(StoredFieldsReader in) {
        this.in = in;
    }

    @Override
    public long ramBytesUsed() {
        return in.ramBytesUsed();
    }

    @Override
    public void close() throws IOException {
        in.close();
    }

    @Override
    public void visitDocument(int docID, StoredFieldVisitor visitor) throws IOException {
        in.visitDocument(docID, visitor);
    }

    public abstract StoredFieldsReader clone();

    @Override
    public void checkIntegrity() throws IOException {
        in.checkIntegrity();
    }
}
