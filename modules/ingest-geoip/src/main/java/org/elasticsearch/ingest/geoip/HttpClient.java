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

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.ingest.geoip;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.SpecialPermission;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.rest.RestStatus;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;

public class HttpClient implements Closeable {

    CloseableHttpClient client = HttpClients.createDefault();

    public byte[] getBytes(String url) throws IOException {
        return get(url).toByteArray();
    }

    public String getString(String url) throws IOException {
        return get(url).toString(StandardCharsets.UTF_8);
    }

    private ByteArrayOutputStream get(String url) throws IOException {
        return doPrivileged(() -> {
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            try (CloseableHttpResponse response = client.execute(new HttpGet(url))) {
                int statusCode = response.getStatusLine().getStatusCode();
                if (statusCode == 404) {
                    throw new ResourceNotFoundException("{} not found", url);
                } else if (statusCode != 200) {
                    throw new ElasticsearchStatusException("error during downloading {}", RestStatus.fromCode(statusCode), url);
                }
                response.getEntity().writeTo(outputStream);
                return outputStream;
            }
        });
    }

    @Override
    public void close() throws IOException {
        client.close();
    }

    private static <R> R doPrivileged(CheckedSupplier<R, IOException> supplier) throws IOException {
        SpecialPermission.check();
        try {
            return AccessController.doPrivileged((PrivilegedExceptionAction<R>) supplier::get);
        } catch (PrivilegedActionException e) {
            throw (IOException) e.getCause();
        }
    }
}
