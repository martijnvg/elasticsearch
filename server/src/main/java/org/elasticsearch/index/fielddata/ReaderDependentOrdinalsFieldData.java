/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.fielddata;

import org.apache.lucene.index.IndexReader;

/**
 * Field data that can only offer ordinals for some readers. Whether a field has ordinals usually follows from its
 * mapping, but a field whose segments each decide how to store its values may have ordinals in one reader and not in
 * the next.
 *
 * <p>Aggregations decide whether to use ordinals from the type of the field data they are given, so the decision is
 * asked for once, against the reader they will search, before they are built. That keeps every aggregation built for a
 * request in agreement about what the field offers.
 */
public interface ReaderDependentOrdinalsFieldData {

    /**
     * The field data to read {@code reader} with: an {@link IndexOrdinalsFieldData} when every segment of
     * {@code reader} can offer ordinals, or field data that reads values otherwise.
     */
    IndexFieldData<?> forReader(IndexReader reader);
}
