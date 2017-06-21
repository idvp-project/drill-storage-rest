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
