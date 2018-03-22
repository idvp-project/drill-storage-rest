package org.apache.drill.exec.store.rest.helpers;

import com.github.jknack.handlebars.Formatter;
import org.apache.commons.collections.IteratorUtils;

import java.lang.reflect.Array;
import java.util.*;

/**
 * @author Oleg Zinoviev
 * @since 22.03.18.
 */
public class CollectionFormatter implements Formatter {
    @Override
    public Object format(Object value, Chain next) {
        if (value == null) {
            return next.format(null);
        }

        List<Object> result = null;
        if (value.getClass().isArray()) {
            int length = Array.getLength(value);
            result = new ArrayList<>(length);
            for (int i = 0; i < length; i++) {
                Object v = Array.get(value, i);
                result.add(v);
            }
        } else if (value instanceof Collection<?>) {
            result = new ArrayList<>((Collection<?>) value);
        } else if (value instanceof Iterable<?>) {
            //noinspection unchecked
            result = new ArrayList<>(IteratorUtils.toList(((Iterable<?>) value).iterator()));
        }

        if (result == null) {
            return next.format(value);
        } else {
            StringJoiner joiner = new StringJoiner(",");
            for (Object v : result) {
                Object formatted = next.format(v);
                joiner.add(Objects.toString(formatted));
            }
            return joiner.toString();
        }
    }
}
