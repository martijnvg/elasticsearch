/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.histogram;

import org.elasticsearch.index.TimestampBounds;

public interface TimestampBoundsAware {

    default boolean contains(long bucket, TimestampBounds bounds) {
        long[] boundary = getBucketBoundary(bucket);
        return boundary[0] >= bounds.startTime()  && boundary[1] <= bounds.endTime();
    }

    long[] getBucketBoundary(long bucket);

}
