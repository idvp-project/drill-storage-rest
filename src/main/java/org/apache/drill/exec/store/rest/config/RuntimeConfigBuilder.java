package org.apache.drill.exec.store.rest.config;

import com.google.common.base.Preconditions;
import org.apache.drill.exec.store.rest.RestStoragePluginConfig;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Oleg Zinoviev
 * @since 21.06.2017.
 */
public final class RuntimeConfigBuilder {

    private String query;
    private RestStoragePluginConfig config;

    public RuntimeConfigBuilder withQuery(String query) {
        this.query = query;
        return this;
    }

    public RuntimeConfigBuilder withRootConfig(RestStoragePluginConfig config) {
        this.config = config;
        return this;
    }

    public RuntimeQueryConfig build() {
        String query = Preconditions.checkNotNull(this.query);
        RestStoragePluginConfig config = Preconditions.checkNotNull(this.config);

        String url = query;
        HttpMethod method = HttpMethod.GET;
        Map<String, String> headersBuilder = new HashMap<>(config.getHeaders());
        Map<String, Object> configBuilder = new HashMap<>(config.getConfig());
        String body  = null;

        QueryConfig existingConfig = config.getQueries().get(query);
        if (existingConfig != null) {
            url = existingConfig.getUrl();
            method = existingConfig.getMethod();
            headersBuilder.putAll(existingConfig.getHeaders());
            configBuilder.putAll(existingConfig.getConfig());
            body = existingConfig.getBody();
        }

        return new RuntimeQueryConfig(url,
                config.getUrl(),
                headersBuilder,
                method,
                body,
                configBuilder);
    }

}
