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

import org.apache.commons.io.IOUtils;
import org.apache.drill.exec.store.rest.config.RuntimeQueryConfig;
import org.apache.drill.exec.store.rest.helpers.VelocityHelper;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultHttpRequestRetryHandler;
import org.apache.http.impl.client.HttpClientBuilder;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;

/**
 * @author Oleg Zinoviev
 * @since 21.06.2017.
 */
final class RestClientProvider {

    final static RestClientProvider INSTANCE = new RestClientProvider();

    private final Map<String, CloseableHttpClient> httpClientMap = new ConcurrentHashMap<>();
    private final Map<String, Integer> configHashes = new ConcurrentHashMap<>();

    private RestClientProvider() {
    }

    CloseableHttpClient getClient(RestStoragePlugin plugin) {
        return httpClientMap.compute(plugin.getName(), new HttpClientCreator(plugin.getConfig()));
    }

    HttpUriRequest createRequest(RuntimeQueryConfig config, RestScanSpec spec) throws URISyntaxException {
        URI uri = createURI(config, spec);
        String body = null;
        if (config.getMethod().isSupportBody()) {
            body = VelocityHelper.INSTANCE.merge(config.getBody(), spec.getParameters());
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



    private URI createURI(RuntimeQueryConfig config, RestScanSpec spec) throws URISyntaxException {

        String localUri = VelocityHelper.INSTANCE.merge(config.getUrl(), spec.getParameters());
        URI scanUri = new URI(localUri);
        if (!scanUri.isAbsolute()) {
            String baseUri = VelocityHelper.INSTANCE.merge(config.getBaseUrl(), spec.getParameters());
            scanUri = new URI(baseUri).resolve(scanUri);
        }

        return scanUri;
    }


    private class HttpClientCreator implements BiFunction<String, CloseableHttpClient, CloseableHttpClient> {

        private final RestStoragePluginConfig currentConfig;

        HttpClientCreator(RestStoragePluginConfig currentConfig) {
            this.currentConfig = currentConfig;
        }

        @Override
        public CloseableHttpClient apply(String name, CloseableHttpClient httpClient) {
            Integer hash = configHashes.get(name);
            if (hash == null || hash != currentConfig.hashCode() || httpClient == null) {
                synchronized (INSTANCE) {
                    try {
                        hash = configHashes.get(name);
                        //Если лежит уже валидный хеш, то возвращаем клиента из мапы
                        if (hash != null && hash == currentConfig.hashCode()) {
                            CloseableHttpClient client = httpClientMap.get(name);
                            if (client != null) {
                                return client;
                            }
                        }

                        return createClient(name);
                    } finally {
                        IOUtils.closeQuietly(httpClient);
                    }
                }
            }
            return httpClient;
        }

        private CloseableHttpClient createClient(String name) {
            configHashes.put(name, currentConfig.hashCode());
            //TODO use RestStoragePluginConfig config section
            return HttpClientBuilder.create()
                    .useSystemProperties()
                    .setRetryHandler(new DefaultHttpRequestRetryHandler(0, false))
                    .build();
        }
    }

}
