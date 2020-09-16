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

import org.apache.lucene.codecs.PointsReader;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.util.Accountable;

import java.io.IOException;
import java.util.Collection;

public abstract class FilteredPointsReader extends PointsReader {

    private final PointsReader in;

    protected FilteredPointsReader(PointsReader in) {
        this.in = in;
    }

    @Override
    public void checkIntegrity() throws IOException {
        in.checkIntegrity();
    }

    @Override
    public PointValues getValues(String field) throws IOException {
        return in.getValues(field);
    }

    @Override
    public void close() throws IOException {
        in.close();
    }

    @Override
    public long ramBytesUsed() {
        return in.ramBytesUsed();
    }

    @Override
    public Collection<Accountable> getChildResources() {
        return in.getChildResources();
    }
}
