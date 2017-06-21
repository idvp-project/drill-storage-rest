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
package org.apache.drill.exec.store.rest.config;

import com.google.common.base.Preconditions;
import org.apache.drill.exec.store.rest.RestStoragePluginConfig;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Oleg Zinoviev
 * @since 21.06.2017.
 */
public final class RuntimeConfigBuilder {

    private String query;
    private RestStoragePluginConfig config;

    public RuntimeConfigBuilder withQuery(String query) {
        this.query = query;
        return this;
    }

    public RuntimeConfigBuilder withRootConfig(RestStoragePluginConfig config) {
        this.config = config;
        return this;
    }

    public RuntimeQueryConfig build() {
        String query = Preconditions.checkNotNull(this.query);
        RestStoragePluginConfig config = Preconditions.checkNotNull(this.config);

        String url = query;
        HttpMethod method = HttpMethod.GET;
        Map<String, String> headersBuilder = new HashMap<>(config.getHeaders());
        Map<String, Object> configBuilder = new HashMap<>(config.getConfig());
        String body  = null;

        QueryConfig existingConfig = config.getQueries().get(query);
        if (existingConfig != null) {
            url = existingConfig.getUrl();
            method = existingConfig.getMethod();
            headersBuilder.putAll(existingConfig.getHeaders());
            configBuilder.putAll(existingConfig.getConfig());
            body = existingConfig.getBody();
        }

        return new RuntimeQueryConfig(url,
                config.getUrl(),
                headersBuilder,
                method,
                body,
                configBuilder);
    }

}
