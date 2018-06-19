/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ccr.action;

import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.AbstractStreamableTestCase;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.nullValue;

public class ShardChangesRequestTests extends AbstractStreamableTestCase<ShardChangesAction.Request> {

    @Override
    protected ShardChangesAction.Request createTestInstance() {
        ShardChangesAction.Request request = new ShardChangesAction.Request(new ShardId("_index", "_indexUUID", 0));
        request.setFromSeqNo(randomNonNegativeLong());
        request.setSize(randomNonNegativeLong());
        request.setMaxTranslogsBytes(randomNonNegativeLong());
        return request;
    }

    @Override
    protected ShardChangesAction.Request createBlankInstance() {
        return new ShardChangesAction.Request();
    }

    public void testValidate() {
        ShardChangesAction.Request request = new ShardChangesAction.Request(new ShardId("_index", "_indexUUID", 0));
        request.setFromSeqNo(-1);
        assertThat(request.validate().getMessage(), containsString("fromSeqNo [-1] cannot be lower than 0"));

        request.setFromSeqNo(8);
        request.setSize(-1);
        assertThat(request.validate().getMessage(), containsString("size [-1] cannot be lower than 0"));

        request.setSize(10);
        request.setMaxTranslogsBytes(-1);
        assertThat(request.validate().getMessage(), containsString("maxTranslogsBytes [-1] cannot be lower than 0"));

        request.setMaxTranslogsBytes(100000);
        assertThat(request.validate(), nullValue());
    }
}
