package org.apache.drill.exec.store.rest.config;

import org.codehaus.jackson.annotate.JsonValue;

/**
 * @author Oleg Zinoviev
 * @since 21.06.2017.
 */
public enum HttpMethod {
    GET("get", false),
    POST("post", true);

    private final String value;
    private final boolean supportBody;

    HttpMethod(String value, boolean supportBody) {
        this.value = value;
        this.supportBody = supportBody;
    }

    @JsonValue
    public String value() {
        return value;
    }

    public boolean isSupportBody() {
        return supportBody;
    }
}
