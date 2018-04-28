package org.apache.drill.exec.store.rest.helpers;

import com.github.jknack.handlebars.Handlebars;
import com.github.jknack.handlebars.Helper;
import com.github.jknack.handlebars.Options;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.Objects;

import static org.apache.commons.lang3.Validate.notNull;

/**
 * @author Oleg Zinoviev
 * @since 28.04.18.
 */
public enum EncodeHelpers implements Helper<Object> {
    encodeUrl {
        @Override
        protected CharSequence safeApply(Object context, Options options) {
            String source = Objects.toString(context);
            String encoding = options.param(0, "UTF-8");

            try {
                return URLEncoder.encode(source, encoding);
            } catch (UnsupportedEncodingException e) {
                return e.getMessage();
            }
        }
    },

    decodeUrl {
        @Override
        protected CharSequence safeApply(Object context, Options options) {
            String source = Objects.toString(context);
            String encoding = options.param(0, "UTF-8");

            try {
                return URLDecoder.decode(source, encoding);
            } catch (UnsupportedEncodingException e) {
                return e.getMessage();
            }
        }
    };


    @Override
    public Object apply(Object context, Options options) {
        if (options.isFalsy(context)) {
            Object param = options.param(0, null);
            return param == Objects.toString(param, null);
        }
        return safeApply(context, options);
    }

    /**
     * Apply the helper to the context.
     *
     * @param context The context object (param=0).
     * @param options The options object.
     * @return A string result.
     */
    protected abstract CharSequence safeApply(final Object context, final Options options);

    /**
     * Register the helper in a handlebars instance.
     *
     * @param handlebars A handlebars object. Required.
     */
    private void registerHelper(final Handlebars handlebars) {
        notNull(handlebars, "The handlebars is required.");
        handlebars.registerHelper(name(), this);
    }

    /**
     * Register all the text helpers.
     *
     * @param handlebars The helper's owner. Required.
     */
    public static void register(final Handlebars handlebars) {
        notNull(handlebars, "A handlebars object is required.");
        EncodeHelpers[] helpers = values();
        for (EncodeHelpers helper : helpers) {
            helper.registerHelper(handlebars);
        }
    }
}
