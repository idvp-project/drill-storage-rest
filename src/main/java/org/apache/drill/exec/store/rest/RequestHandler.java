/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.store.rest;

import com.google.common.base.Stopwatch;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.store.rest.config.RuntimeQueryConfig;
import org.apache.drill.exec.store.rest.read.RestMetric;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.utils.HttpClientUtils;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.CloseableHttpClient;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.concurrent.TimeUnit;

/**
 * @author Oleg Zinoviev
 * @since 26.06.2017.
 */
public final class RequestHandler {

    private final RuntimeQueryConfig config;

    RequestHandler(RuntimeQueryConfig config) {
        this.config = config;

    }

    public CloseableHttpResponse execute(RestSubScan scan, OperatorContext context) throws URISyntaxException, IOException, ExecutionSetupException {
        Stopwatch stopwatch = Stopwatch.createStarted();
        try {
            CloseableHttpClient client = RestClientProvider.INSTANCE.getClient(scan.getStoragePlugin());
            HttpUriRequest request = RestClientProvider.INSTANCE.createRequest(config, scan.getSpec());
            CloseableHttpResponse response = client.execute(request);
            handleResponseStatus(response);

            return response;

        } finally {
            long requestTime = stopwatch.stop().elapsed(TimeUnit.MILLISECONDS);
            context.getStats().addLongStat(RestMetric.TIME_REQUEST, requestTime);
        }
    }

    public ContentType extractContentType(HttpResponse response) {
        ContentType contentType = ContentType.TEXT_PLAIN;
        if (!config.isIgnoreContentType()) {
            contentType = ContentType.getOrDefault(response.getEntity());
        }
        return contentType;
    }

    private void handleResponseStatus(CloseableHttpResponse response) throws ExecutionSetupException {
        if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
            HttpClientUtils.closeQuietly(response);
            throw new ExecutionSetupException(response.getStatusLine().getReasonPhrase());
        }
    }
}
