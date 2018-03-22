/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
