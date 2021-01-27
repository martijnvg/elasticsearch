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

package org.elasticsearch.action.admin.indices.alias.get;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.DataStreamAlias;
import org.elasticsearch.cluster.metadata.DataStreamMetadata;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class GetAliasesResponse extends ActionResponse {

    private final ImmutableOpenMap<String, List<AliasMetadata>> aliases;
    private final List<DataStreamAlias> dataStreamAliases;

    public GetAliasesResponse(ImmutableOpenMap<String, List<AliasMetadata>> aliases, List<DataStreamAlias> dataStreamAliases) {
        this.aliases = aliases;
        this.dataStreamAliases = dataStreamAliases;
    }

    public GetAliasesResponse(StreamInput in) throws IOException {
        super(in);
        aliases = in.readImmutableMap(StreamInput::readString, i -> i.readList(AliasMetadata::new));
        dataStreamAliases = in.getVersion().onOrAfter(DataStreamMetadata.DATA_STREAM_ALIAS_VERSION) ?
            in.readList(DataStreamAlias::new) : null;
    }

    public ImmutableOpenMap<String, List<AliasMetadata>> getAliases() {
        return aliases;
    }

    public List<DataStreamAlias> getDataStreamAliases() {
        return dataStreamAliases;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(aliases, StreamOutput::writeString, StreamOutput::writeList);
        if (out.getVersion().onOrAfter(DataStreamMetadata.DATA_STREAM_ALIAS_VERSION)) {
            out.writeList(dataStreamAliases);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        GetAliasesResponse that = (GetAliasesResponse) o;
        return Objects.equals(aliases, that.aliases) &&
            Objects.equals(dataStreamAliases, that.dataStreamAliases);
    }

    @Override
    public int hashCode() {
        return Objects.hash(aliases, dataStreamAliases);
    }
}
