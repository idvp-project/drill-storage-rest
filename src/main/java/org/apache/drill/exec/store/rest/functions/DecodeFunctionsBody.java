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
package org.apache.drill.exec.store.rest.functions;

import io.netty.buffer.DrillBuf;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.exec.expr.holders.ValueHolder;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;

/**
 * @author Oleg Zinoviev
 * @since 22.06.2017.
 */
@SuppressWarnings("WeakerAccess")
public final class DecodeFunctionsBody {
    private DecodeFunctionsBody() {
    }

    public static class UrlDecodeVarCharFuncBody {
        private UrlDecodeVarCharFuncBody() {
        }

        public static String eval(ValueHolder source) {
            String encoded = FunctionsHelper.asString(source);
            try {
                return URLDecoder.decode(encoded, StandardCharsets.UTF_8.name());
            } catch (UnsupportedEncodingException e) {
                throw new DrillRuntimeException(e);
            }
        }
    }

    public static class XmlDecodeVarCharFuncBody {
        private XmlDecodeVarCharFuncBody() {
        }

        public static String eval(ValueHolder source) {
            String encoded = FunctionsHelper.asString(source);
            return StringEscapeUtils.unescapeHtml4(encoded);
        }
    }
}
