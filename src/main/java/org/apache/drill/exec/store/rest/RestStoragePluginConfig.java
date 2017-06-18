package org.apache.drill.exec.store.rest;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import org.apache.drill.common.logical.StoragePluginConfigBase;

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
    private final Map<String, Object> config;
    private final Map<String, String> headers;

    @JsonCreator
    public RestStoragePluginConfig(@JsonProperty(value = "url", required = true) String url,
                                   @JsonProperty(value = "config") Map<String, Object> config,
                                   @JsonProperty(value = "headers") Map<String, String> headers) {

        this.url = Preconditions.checkNotNull(url, "url cannot be null");
        this.config = config;
        this.headers = headers;
    }

    @SuppressWarnings("unused")
    @JsonProperty
    public String getUrl() {
        return url;
    }

    @SuppressWarnings("unused")
    @JsonProperty
    public Map<String, Object> getConfig() {
        return config;
    }

    @SuppressWarnings("unused")
    @JsonProperty
    public Map<String, String> getHeaders() {
        return headers;
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
                && Objects.equals(headers, that.headers);
    }

    @Override
    public int hashCode() {
        return 56
                ^ Objects.hashCode(url)
                ^ Objects.hashCode(config)
                ^ Objects.hashCode(headers);
    }
}
