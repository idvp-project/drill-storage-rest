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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;

/**
 * @author Oleg Zinoviev
 * @since 21.06.2017.
 */
public final class RuntimeQueryConfig extends QueryConfig {

    private final static Logger logger = LoggerFactory.getLogger(RuntimeQueryConfig.class);

    private final String baseUrl;

    RuntimeQueryConfig(String url,
                       String baseUrl,
                       Map<String, String> headers,
                       HttpMethod method,
                       String body,
                       Map<String, Object> config) {
        super(url, headers, method, body, config);
        this.baseUrl = baseUrl;
    }

    public String getBaseUrl() {
        return baseUrl;
    }

    public boolean isIgnoreContentType() {
        if (config.containsKey("ignore.content.type")) {
            Object value = config.get("ignore.content.type");
            if (value instanceof Boolean) {
                return (Boolean) value;
            }
            try {
                if (value instanceof String) {
                    return Boolean.parseBoolean((String) value);
                } else {
                    logger.error("ignore.content.type value " + Objects.toString(value) + " not supported");
                }
            } catch (Exception e) {
                logger.error("ignore.content.type value " + Objects.toString(value) + " not supported", e);
            }

        }
        return false;
    }
}
