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

import com.github.jknack.handlebars.Context;
import com.github.jknack.handlebars.Handlebars;
import com.github.jknack.handlebars.ValueResolver;
import com.github.jknack.handlebars.context.MapValueResolver;
import com.github.jknack.handlebars.helper.StringHelpers;
import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;
import org.apache.drill.common.exceptions.DrillRuntimeException;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

/**
 * @author Oleg Zinoviev
 * @since 21.06.2017.
 */
public final class HandlebarsHelper {

    private HandlebarsHelper() {
    }


    public static String merge(String input, Map<String, Object> parameters) {
        Preconditions.checkNotNull(input, "input");
        Preconditions.checkNotNull(parameters, "parameters");

        Context context = Context.newBuilder(parameters)
                .push(new CustomMapValueResolver())
                .push(ValueResolver.VALUE_RESOLVERS)
                .build();

        Handlebars handlebars = new FixHandlebars()
                .with(new CollectionFormatter())
                .infiniteLoops(false);
        EncodeHelpers.register(handlebars);
        StringHelpers.register(handlebars);

        try {
            return handlebars.compileInline(input).apply(context);
        } catch (IOException e) {
            throw new DrillRuntimeException(e);
        }

    }

    private static class CustomMapValueResolver implements ValueResolver {

        @Override
        public Object resolve(Object context, String name) {
            Object resolve = MapValueResolver.INSTANCE.resolve(context, name);
            if (resolve == UNRESOLVED) {
                resolve = MapValueResolver.INSTANCE.resolve(context, StringUtils.lowerCase(name));
            }
            return resolve;
        }

        @Override
        public Object resolve(Object context) {
            return MapValueResolver.INSTANCE.resolve(context);
        }

        @Override
        public Set<Map.Entry<String, Object>> propertySet(Object context) {
            return MapValueResolver.INSTANCE.propertySet(context);
        }
    }

}
