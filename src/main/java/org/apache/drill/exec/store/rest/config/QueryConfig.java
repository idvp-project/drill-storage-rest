package org.apache.drill.exec.store.rest.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;
import java.util.Objects;

/**
 * @author Oleg Zinoviev
 * @since 21.06.2017.
 */
public class QueryConfig extends ServiceConfigBase {

    private final HttpMethod method;
    private final String body;

    @JsonCreator
    public QueryConfig(@JsonProperty(value = "url", required = true) String url,
                       @JsonProperty(value = "headers") Map<String, String> headers,
                       @JsonProperty(value = "method") HttpMethod method,
                       @JsonProperty(value = "body") String body,
                       @JsonProperty(value = "config") Map<String, Object> config) {
        super(url, headers, config);
        this.method = method == null ? HttpMethod.GET : method;
        this.body = body;
    }

    @JsonProperty
    public HttpMethod getMethod() {
        return method;
    }

    @JsonProperty
    public String getBody() {
        return body;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else if (o == null || getClass() != o.getClass()) {
            return false;
        }
        QueryConfig that = (QueryConfig) o;
        return Objects.equals(url, that.url)
                && Objects.equals(config, that.config)
                && Objects.equals(headers, that.headers)
                && Objects.equals(method, that.method)
                && Objects.equals(body, that.body);
    }

    @Override
    public int hashCode() {
        return 56
                ^ Objects.hashCode(url)
                ^ Objects.hashCode(config)
                ^ Objects.hashCode(headers)
                ^ Objects.hashCode(method)
                ^ Objects.hashCode(body);
    }
}
