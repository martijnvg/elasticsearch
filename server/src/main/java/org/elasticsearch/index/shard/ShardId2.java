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

package org.elasticsearch.index.shard;

import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.Index;

import java.io.IOException;

/**
 * Allows for shard level components to be injected with the shard id.
 */
public class ShardId2 implements Comparable<ShardId2>, ToXContentFragment, Writeable {

    private final String indexUUID;
    private final int shardId;
    private final int hashCode;

    public ShardId2(ShardId shardId) {
        this.indexUUID = shardId.getIndex().getUUID();
        this.shardId = shardId.getId();
        this.hashCode = computeHashCode();
    }

    public ShardId2(Index index, int shardId) {
        this.indexUUID = index.getUUID();
        this.shardId = shardId;
        this.hashCode = computeHashCode();
    }

    public ShardId2(String index, String indexUUID, int shardId) {
        this(new Index(index, indexUUID), shardId);
    }

    public ShardId2(StreamInput in) throws IOException {
        indexUUID = in.readString();
        shardId = in.readVInt();
        hashCode = computeHashCode();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(indexUUID);
        out.writeVInt(shardId);
    }

    public String getIndexUUID() {
        return indexUUID;
    }

    public int id() {
        return this.shardId;
    }

    public int getId() {
        return id();
    }

    @Override
    public String toString() {
        return "[" + indexUUID + "][" + shardId + "]";
    }

    /**
     * Parse the string representation of this shardId back to an object.
     * We lose index uuid information here, but since we use toString in
     * rest responses, this is the best we can do to reconstruct the object
     * on the client side.
     */
    public static ShardId2 fromString(String shardIdString) {
        int splitPosition = shardIdString.indexOf("][");
        if (splitPosition <= 0 || shardIdString.charAt(0) != '[' || shardIdString.charAt(shardIdString.length() - 1) != ']') {
            throw new IllegalArgumentException("Unexpected shardId string format, expected [indexName][shardId] but got " + shardIdString);
        }
        String indexName = shardIdString.substring(1, splitPosition);
        int shardId = Integer.parseInt(shardIdString.substring(splitPosition + 2, shardIdString.length() - 1));
        return new ShardId2(new Index(indexName, IndexMetaData.INDEX_UUID_NA_VALUE), shardId);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ShardId2 shardId1 = (ShardId2) o;
        return shardId == shardId1.shardId && indexUUID.equals(shardId1.indexUUID);
    }

    @Override
    public int hashCode() {
        return hashCode;
    }

    private int computeHashCode() {
        int result = indexUUID != null ? indexUUID.hashCode() : 0;
        result = 31 * result + shardId;
        return result;
    }

    @Override
    public int compareTo(ShardId2 o) {
        if (o.getId() == shardId) {
            int compare = indexUUID.compareTo(o.getIndexUUID());
            if (compare != 0) {
                return compare;
            }
            return indexUUID.compareTo(o.getIndexUUID());
        }
        return Integer.compare(shardId, o.getId());
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.value(toString());
    }
}
