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

import com.github.jknack.handlebars.Handlebars;
import com.google.common.base.Preconditions;
import org.apache.drill.common.exceptions.DrillRuntimeException;

import java.io.IOException;
import java.util.Map;

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

        Handlebars handlebars = new Handlebars().infiniteLoops(false);
        try {
            return handlebars.compileInline(input).apply(parameters);
        } catch (IOException e) {
            throw new DrillRuntimeException(e);
        }

    }

}
