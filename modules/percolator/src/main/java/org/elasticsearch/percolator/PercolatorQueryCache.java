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
package org.elasticsearch.percolator;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.common.cache.RemovalListener;
import org.elasticsearch.common.cache.RemovalNotification;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.query.BaseTermQueryBuilder;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.BoostingQueryBuilder;
import org.elasticsearch.index.query.ConstantScoreQueryBuilder;
import org.elasticsearch.index.query.DisMaxQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

class PercolatorQueryCache implements RemovalListener<PercolatorQueryCache.Key, QueryBuilder>, IndexReader.ClosedListener {

    static final Setting<ByteSizeValue> INDICES_PERCOLATOR_CACHE_SIZE_KEY =
        Setting.memorySizeSetting("indices.percolator.cache.size", "10%", Setting.Property.NodeScope);

    static class Key {

        private final ShardId shardId;
        private final IndexReader.CacheKey readerKey;
        private final int docId;

        Key(ShardId shardId, IndexReader.CacheKey readerKey, int docId) {
            this.shardId = shardId;
            this.readerKey = readerKey;
            this.docId = docId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Key key = (Key) o;
            return Objects.equals(shardId, key.shardId) &&
                Objects.equals(readerKey, key.readerKey) &&
                Objects.equals(docId, key.docId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(shardId, readerKey, docId);
        }
    }

    private final long maxCacheSizeInBytes;
    private final Cache<Key, QueryBuilder> cache;

    PercolatorQueryCache(Settings settings) {
        this.maxCacheSizeInBytes = INDICES_PERCOLATOR_CACHE_SIZE_KEY.get(settings).getBytes();
        this.cache = CacheBuilder.<Key, QueryBuilder>builder()
            .setMaximumWeight(maxCacheSizeInBytes)
            .weigher(PercolatorQueryCache::estimateSizeInBytes)
            .removalListener(this)
            .build();
    }

    QueryBuilder computeIfAbsent(LeafReaderContext ctx, int docId, Supplier<QueryBuilder> loader) throws ExecutionException {
        if (maxCacheSizeInBytes == 0L) {
            return loader.get();
        }

        final ShardId shardId = ShardUtils.extractShardId(ctx.reader());
        final IndexReader.CacheHelper cacheHelper = ctx.reader().getCoreCacheHelper();
        if (cacheHelper == null) {
            throw new IllegalArgumentException("Reader " + ctx.reader() + " does not support caching");
        }
        Key key = new Key(shardId, cacheHelper.getKey(), docId);
        return cache.computeIfAbsent(key, key1 -> {
            cacheHelper.addClosedListener(PercolatorQueryCache.this);
            return loader.get();
        });
    }

    @Override
    public void onRemoval(RemovalNotification<Key, QueryBuilder> notification) {

    }

    @Override
    public void onClose(IndexReader.CacheKey key) throws IOException {

    }

    static long estimateSizeInBytes(Key key, QueryBuilder queryBuilder) {
        return estimateQueryBuilderSize(queryBuilder);
    }

    static long estimateQueryBuilderSize(QueryBuilder queryBuilder) {
        final long shallowSizeOfInstance = RamUsageEstimator.shallowSizeOfInstance(queryBuilder.getClass());
        if (queryBuilder instanceof BoolQueryBuilder) {
            BoolQueryBuilder boolQueryBuilder = (BoolQueryBuilder) queryBuilder;
            List<QueryBuilder> clauses = new ArrayList<>();
            clauses.addAll(boolQueryBuilder.filter());
            clauses.addAll(boolQueryBuilder.must());
            clauses.addAll(boolQueryBuilder.mustNot());
            clauses.addAll(boolQueryBuilder.should());
            long summedEstimatedSize = 0L;
            for (QueryBuilder clause : clauses) {
                summedEstimatedSize += estimateQueryBuilderSize(clause);
            }
            return shallowSizeOfInstance + summedEstimatedSize;
        } else if (queryBuilder instanceof ConstantScoreQueryBuilder) {
            return shallowSizeOfInstance + estimateQueryBuilderSize(((ConstantScoreQueryBuilder) queryBuilder).innerQuery());
        } else if (queryBuilder instanceof FunctionScoreQueryBuilder) {
            return shallowSizeOfInstance + estimateQueryBuilderSize(((FunctionScoreQueryBuilder) queryBuilder).query());
        } else if (queryBuilder instanceof BoostingQueryBuilder) {
            long summedEstimatedSize = estimateQueryBuilderSize(((BoostingQueryBuilder) queryBuilder).negativeQuery());
            summedEstimatedSize += estimateQueryBuilderSize(((BoostingQueryBuilder) queryBuilder).positiveQuery());
            return shallowSizeOfInstance + summedEstimatedSize;
        } else if (queryBuilder instanceof DisMaxQueryBuilder) {
            DisMaxQueryBuilder disMaxQueryBuilder = (DisMaxQueryBuilder) queryBuilder;
            long summedEstimatedSize = 0L;
            for (QueryBuilder innerQueryBuilder : disMaxQueryBuilder.innerQueries()) {
                summedEstimatedSize += shallowSizeOfInstance + estimateQueryBuilderSize(innerQueryBuilder);
            }
            return shallowSizeOfInstance + summedEstimatedSize;
        } else if (queryBuilder instanceof BaseTermQueryBuilder) {
            BaseTermQueryBuilder termQueryBuilder = (BaseTermQueryBuilder) queryBuilder;
            return shallowSizeOfInstance +
                sizeOf(termQueryBuilder.fieldName()) +
                sizeOf(termQueryBuilder.value());
        } else {
            try {
                String strippedContent = stripAndToString(queryBuilder);
                return shallowSizeOfInstance + sizeOf(strippedContent);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    static long sizeOf(Object object) {
        if (object instanceof Integer) {
            return sizeOf((Integer) object);
        } else if (object instanceof Long) {
            return sizeOf((Long) object);
        } else if (object instanceof Float) {
            return sizeOf((Float) object);
        } else if (object instanceof Double) {
            return sizeOf((Double) object);
        } else if (object instanceof String) {
            return sizeOf((String) object);
        } else {
            return 1L;
        }
    }

    static long sizeOf(Integer val) {
        return Integer.BYTES;
    }

    static long sizeOf(Long val) {
        return RamUsageEstimator.sizeOf(val);
    }

    static long sizeOf(Float val) {
        return Float.BYTES;
    }

    static long sizeOf(Double val) {
        return Double.BYTES;
    }

    static long sizeOf(String val) {
        return Character.BYTES * val.length();
    }

    static String stripAndToString(ToXContent queryBuilder) throws IOException {
        XContentBuilder builder = JsonXContent.contentBuilder();
        queryBuilder.toXContent(builder, ToXContent.EMPTY_PARAMS);
        String json = builder.string();
        return json.replace("{", "").replace("}", "").replace(":", "");
    }

}
