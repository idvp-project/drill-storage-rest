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

    @JsonCreator
    public RestScanSpec(@JsonProperty(value = "query", required = true) final String query,
                        @JsonProperty(value = "parameters") final Map<String, Object> parameters) {
        this.query = query;
        this.parameters = parameters == null ? Collections.emptyMap() : Collections.unmodifiableMap(parameters);
    }

    @JsonProperty
    public String getQuery() {
        return query;
    }

    @JsonProperty
    public Map<String, Object> getParameters() {
        return parameters;
    }
}
