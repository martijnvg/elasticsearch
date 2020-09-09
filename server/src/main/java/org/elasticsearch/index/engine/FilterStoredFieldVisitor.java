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

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.StoredFieldVisitor;

import java.io.IOException;

abstract class FilterStoredFieldVisitor extends StoredFieldVisitor {
    private final StoredFieldVisitor visitor;

    protected FilterStoredFieldVisitor(StoredFieldVisitor visitor) {
        this.visitor = visitor;
    }

    @Override
    public void binaryField(FieldInfo fieldInfo, byte[] value) throws IOException {
        visitor.binaryField(fieldInfo, value);
    }

    @Override
    public void stringField(FieldInfo fieldInfo, byte[] value) throws IOException {
        visitor.stringField(fieldInfo, value);
    }

    @Override
    public void intField(FieldInfo fieldInfo, int value) throws IOException {
        visitor.intField(fieldInfo, value);
    }

    @Override
    public void longField(FieldInfo fieldInfo, long value) throws IOException {
        visitor.longField(fieldInfo, value);
    }

    @Override
    public void floatField(FieldInfo fieldInfo, float value) throws IOException {
        visitor.floatField(fieldInfo, value);
    }

    @Override
    public void doubleField(FieldInfo fieldInfo, double value) throws IOException {
        visitor.doubleField(fieldInfo, value);
    }

    @Override
    public Status needsField(FieldInfo fieldInfo) throws IOException {
        return visitor.needsField(fieldInfo);
    }
}
