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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.drill.common.logical.StoragePluginConfig;

import java.util.Collections;
import java.util.Map;

/**
 * @author Oleg Zinoviev
 * @since 21.06.2017.
 */
public abstract class ServiceConfigBase extends StoragePluginConfig {
    protected final String url;
    protected final Map<String, String> headers;
    protected final Map<String, Object> config;

    protected ServiceConfigBase(String url,
                                Map<String, String> headers,
                                Map<String, Object> config) {
        this.url = Preconditions.checkNotNull(url, "url cannot be null");
        this.headers = headers == null ? Collections.emptyMap() : headers;
        this.config = config == null ? Collections.emptyMap() : config;
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
    public Map<String, Object> getConfig() {
        return config;
    }

}
