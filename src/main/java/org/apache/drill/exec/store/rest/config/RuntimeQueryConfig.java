package org.apache.drill.exec.store.rest.config;

import com.fasterxml.jackson.annotation.JsonCreator;

import java.util.Map;

/**
 * @author Oleg Zinoviev
 * @since 21.06.2017.
 */
public class RuntimeQueryConfig extends QueryConfig {
    private final String baseUrl;

    public RuntimeQueryConfig(String url,
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
}
