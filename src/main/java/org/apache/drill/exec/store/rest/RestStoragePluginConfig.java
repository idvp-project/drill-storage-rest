package org.apache.drill.exec.store.rest;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.drill.exec.store.rest.config.QueryConfig;
import org.apache.drill.exec.store.rest.config.RuntimeConfigBuilder;
import org.apache.drill.exec.store.rest.config.RuntimeQueryConfig;
import org.apache.drill.exec.store.rest.config.ServiceConfigBase;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

/**
 * @author Oleg Zinoviev
 * @since 15.06.2017.
 */
@JsonTypeName(RestStoragePluginConfig.NAME)
public class RestStoragePluginConfig extends ServiceConfigBase {

    static final String NAME = "rest";
    private final Map<String, QueryConfig> queries;

    @JsonCreator
    public RestStoragePluginConfig(@JsonProperty(value = "url", required = true) String url,
                                   @JsonProperty(value = "headers") Map<String, String> headers,
                                   @JsonProperty(value = "queries") Map<String, QueryConfig> queries,
                                   @JsonProperty(value = "config") Map<String, Object> config) {
        super(url, headers, config);
        this.queries = queries == null ? Collections.emptyMap() : queries;
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
