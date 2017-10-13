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

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import org.apache.commons.lang3.StringUtils;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.store.rest.config.RuntimeQueryConfig;
import org.apache.drill.exec.store.rest.helpers.HandlebarsHelper;
import org.apache.drill.exec.store.rest.query.ParameterValue;
import org.apache.drill.exec.store.rest.read.RestMetric;
import org.apache.drill.exec.store.rest.read.RestRecordReader;
import org.apache.http.Header;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.BasicResponseHandler;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultHttpRequestRetryHandler;
import org.apache.http.impl.client.HttpClientBuilder;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
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

    public Result execute(RestSubScan scan,
                          OperatorContext context) throws URISyntaxException, IOException, ExecutionSetupException {
        Stopwatch stopwatch = Stopwatch.createStarted();
        try (CloseableHttpClient client = HttpClientBuilder.create()
                .useSystemProperties()
                .setRetryHandler(new DefaultHttpRequestRetryHandler(0, false))
                .build()) {

            HttpUriRequest request = createRequest(config, scan.getSpec());
            try (CloseableHttpResponse response = client.execute(request)) {

                ResponseHandler<String> responseHandler = new BasicResponseHandler();

                String body = responseHandler.handleResponse(response);
                ContentType contentType = ContentType.getOrDefault(response.getEntity());

                Map<String, String> headers = new HashMap<>();
                for (Header header : response.getAllHeaders()) {
                    headers.put(header.getName(), header.getValue());
                }

                return new Result(contentType, body, headers);
            }

        } finally {
            long requestTime = stopwatch.stop().elapsed(TimeUnit.MILLISECONDS);
            context.getStats().addLongStat(RestMetric.TIME_REQUEST, requestTime);
        }
    }

    private HttpUriRequest createRequest(RuntimeQueryConfig config,
                                         RestScanSpec spec) throws URISyntaxException, IOException {

        Map<String, ParameterValue> parameterValues = Collections.emptyMap();
        if (StringUtils.isNotBlank(spec.getParameters())) {
            parameterValues = RestRecordReader.MAPPER.readValue(spec.getParameters(), new TypeReference<Map<String, ParameterValue>>() {});
        }

        Map<String, Object> parameters = new HashMap<>();
        for (Map.Entry<String, ParameterValue> entry : parameterValues.entrySet()) {
            if (entry.getValue() != null && entry.getValue().getType() == ParameterValue.Type.QUERY) {
                throw new IllegalStateException("QUERY parameters not yet supported");
            }

            parameters.put(entry.getKey(), entry.getValue() == null ? null : entry.getValue().getValue());
        }

        URI uri = createURI(config, parameters);
        String body = null;
        if (StringUtils.isNotBlank(config.getBody())) {
            body = HandlebarsHelper.merge(config.getBody(), parameters);
        }

        HttpUriRequest request;
        switch (config.getMethod()) {
            case GET: {
                request = new HttpGet(uri);
                break;
            }
            case POST: {
                HttpPost post = new HttpPost(uri);
                if (body != null) {
                    post.setEntity(new ByteArrayEntity(body.getBytes(StandardCharsets.UTF_8)));
                }
                request = post;
                break;
            }
            default: {
                throw new IllegalArgumentException("Unsupported method " + config.getMethod());
            }
        }

        for (Map.Entry<String, String> header : config.getHeaders().entrySet()) {
            request.addHeader(header.getKey(), header.getValue());
        }

        return request;
    }



    private URI createURI(RuntimeQueryConfig config, Map<String, Object> parameters) throws URISyntaxException {

        String localUri = HandlebarsHelper.merge(config.getUrl(), parameters);
        URI scanUri = new URI(localUri);
        if (!scanUri.isAbsolute()) {
            Preconditions.checkNotNull(config.getBaseUrl(), "config.baseUrl");
            String baseUri = HandlebarsHelper.merge(config.getBaseUrl(), parameters);
            scanUri = new URI(baseUri).resolve(scanUri);
        }

        return scanUri;
    }

    public final static class Result {
        private final ContentType contentType;
        private final String content;
        private final Map<String, String> headers;


        Result(ContentType contentType,
               String content,
               Map<String, String> headers) {
            this.contentType = contentType;
            this.content = content;
            this.headers = headers;
        }

        public ContentType getContentType() {
            return contentType;
        }

        public String getContent() {
            return content;
        }

        public Map<String, String> getHeaders() {
            return headers;
        }
    }
}
