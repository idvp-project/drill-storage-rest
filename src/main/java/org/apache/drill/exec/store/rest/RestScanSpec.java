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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

import java.util.Collections;
import java.util.Map;

/**
 * @author Oleg Zinoviev
 * @since 15.06.2017.
 */
@JsonTypeName("rest-scan-spec")
@SuppressWarnings({"WeakerAccess", "unused"})
public class RestScanSpec {

    private final String query;
    private final Map<String, Object> parameters;
    private final FilterPushDown filterPushDown;

    @JsonCreator
    public RestScanSpec(@JsonProperty(value = "query", required = true) final String query,
                        @JsonProperty(value = "parameters") final Map<String, Object> parameters,
                        @JsonProperty(value = "filterPushDown") final FilterPushDown filterPushDown) {
        this.query = query;
        this.parameters = parameters == null ? Collections.emptyMap() : Collections.unmodifiableMap(parameters);
        this.filterPushDown = filterPushDown == null ? FilterPushDown.NONE : filterPushDown;
    }

    @JsonProperty
    public String getQuery() {
        return query;
    }

    @JsonProperty
    public Map<String, Object> getParameters() {
        return parameters;
    }

    @JsonProperty
    public FilterPushDown getFilterPushDown() {
        return filterPushDown;
    }
}
