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
package org.elasticsearch.protocol.xpack.ml;

import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.protocol.xpack.ml.job.config.Job;

import java.io.IOException;
import java.util.Objects;

/**
 * Response containing the newly created {@link Job}
 */
public class PutJobResponse implements ToXContentObject {

    private Job job;

    public static PutJobResponse fromXContent(XContentParser parser) throws IOException {
        return new PutJobResponse(Job.PARSER.parse(parser, null).build());
    }

    PutJobResponse(Job job) {
        this.job = job;
    }

    public Job getResponse() {
        return job;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        job.toXContent(builder, params);
        return builder;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (object == null || getClass() != object.getClass()) {
            return false;
        }
        PutJobResponse response = (PutJobResponse) object;
        return Objects.equals(job, response.job);
    }

    @Override
    public int hashCode() {
        return Objects.hash(job);
    }
}
