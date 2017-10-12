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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import org.apache.drill.common.logical.StoragePluginConfigBase;
import org.apache.drill.exec.store.rest.config.QueryConfig;
import org.apache.drill.exec.store.rest.config.RuntimeConfigBuilder;
import org.apache.drill.exec.store.rest.config.RuntimeQueryConfig;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

/**
 * @author Oleg Zinoviev
 * @since 15.06.2017.
 */
@JsonTypeName(RestStoragePluginConfig.NAME)
public class RestStoragePluginConfig extends StoragePluginConfigBase {

    static final String NAME = "rest";
    private final String url;
    private final Map<String, String> headers;
    private final Map<String, Object> config;
    private final Map<String, QueryConfig> queries;

    @JsonCreator
    public RestStoragePluginConfig(@JsonProperty(value = "url", required = true) String url,
                                   @JsonProperty(value = "headers") Map<String, String> headers,
                                   @JsonProperty(value = "queries") Map<String, QueryConfig> queries,
                                   @JsonProperty(value = "config") Map<String, Object> config) {
        this.url = Preconditions.checkNotNull(url, "url cannot be null");
        this.headers = headers == null ? Collections.emptyMap() : headers;
        this.config = config == null ? Collections.emptyMap() : config;
        this.queries = queries == null ? Collections.emptyMap() : queries;
    }

    @JsonProperty
    public String getUrl() {
        return url;
    }

    @JsonProperty
    public Map<String, String> getHeaders() {
        return headers;
    }

    @JsonProperty
    public Map<String, QueryConfig> getQueries() {
        return queries;
    }

    @JsonIgnore
    RuntimeQueryConfig getRuntimeConfig(String query) {
        return new RuntimeConfigBuilder()
                .withQuery(query)
                .withRootConfig(this)
                .build();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RestStoragePluginConfig that = (RestStoragePluginConfig) o;
        return Objects.equals(url, that.url)
                && Objects.equals(config, that.config)
                && Objects.equals(headers, that.headers)
                && Objects.equals(queries, that.queries);
    }

    @Override
    public int hashCode() {
        return 56
                ^ Objects.hashCode(url)
                ^ Objects.hashCode(config)
                ^ Objects.hashCode(headers)
                ^ Objects.hashCode(queries);
    }
}
