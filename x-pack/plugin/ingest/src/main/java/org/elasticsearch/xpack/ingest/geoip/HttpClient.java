/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ingest.geoip;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.elasticsearch.xpack.core.common.socket.SocketAccess;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class HttpClient implements Closeable {

    CloseableHttpClient client = HttpClients.createDefault();

    public byte[] getBytes(String url) throws IOException {
        return get(url).toByteArray();
    }

    public String getString(String url) throws IOException {
        return get(url).toString(StandardCharsets.UTF_8);
    }

    private ByteArrayOutputStream get(String url) throws IOException {
        return SocketAccess.doPrivileged(() -> {
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            try (CloseableHttpResponse response = client.execute(new HttpGet(url))) {
                response.getEntity().writeTo(outputStream);
                return outputStream;
            }
        });
    }

    @Override
    public void close() throws IOException {
        client.close();
    }
}
