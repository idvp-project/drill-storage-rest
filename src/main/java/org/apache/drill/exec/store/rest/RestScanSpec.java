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
