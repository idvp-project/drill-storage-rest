package org.apache.drill.exec.store.rest;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

/**
 * @author Oleg Zinoviev
 * @since 15.06.2017.
 */
@JsonTypeName("rest-scan-spec")
@SuppressWarnings({"WeakerAccess", "unused"})
public class RestScanSpec {

    private final String query;

    @JsonCreator
    public RestScanSpec(String query) {
        this.query = query;
    }

    @JsonProperty
    public String getQuery() {
        return query;
    }
}
