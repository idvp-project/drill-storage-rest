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

import com.google.common.base.Charsets;
import io.netty.buffer.DrillBuf;
import org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers;
import org.apache.drill.exec.expr.holders.*;
import org.apache.drill.exec.vector.complex.writer.BaseWriter;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

/**
 * @author Oleg Zinoviev
 * @since 21.06.2017.
 */
@SuppressWarnings("WeakerAccess")
public final class SelectorFunctionsBody {
    private SelectorFunctionsBody() {
    }

    public static class DOMSelectorFuncBody {
        private DOMSelectorFuncBody() {
        }

        public static void eval(ValueHolder source,
                                ValueHolder selector,
                                BaseWriter.ComplexWriter output,
                                DrillBuf buffer) {
            String html = asString(source);
            String localSelector = asString(selector);
            Document parse = org.jsoup.Jsoup.parse(html);
            Elements elements = parse.select(localSelector);
            BaseWriter.ListWriter listWriter = output.rootAsList();
            listWriter.startList();
            for (Element element : elements) {
                String inner = element.html();
                final byte[] strBytes = inner.getBytes(Charsets.UTF_8);
                buffer = buffer.reallocIfNeeded(strBytes.length);
                buffer.setBytes(0, strBytes);
                listWriter.varChar().writeVarChar(0, strBytes.length, buffer);
            }
            listWriter.endList();
        }

        private static String asString(ValueHolder source) {
            String html;
            if (source instanceof VarCharHolder) {
                VarCharHolder vch = (VarCharHolder) source;
                html = StringFunctionHelpers.toStringFromUTF8(vch.start, vch.end, vch.buffer);
            } else if (source instanceof NullableVarCharHolder) {
                NullableVarCharHolder vch = (NullableVarCharHolder) source;
                html = StringFunctionHelpers.toStringFromUTF8(vch.start, vch.end, vch.buffer);
            } else if (source instanceof Var16CharHolder) {
                Var16CharHolder vch = (Var16CharHolder) source;
                html = StringFunctionHelpers.toStringFromUTF16(vch.start, vch.end, vch.buffer);
            } else if (source instanceof NullableVar16CharHolder) {
                NullableVar16CharHolder vch = (NullableVar16CharHolder) source;
                html = StringFunctionHelpers.toStringFromUTF16(vch.start, vch.end, vch.buffer);
            } else {
                throw new RuntimeException("Unsupported type");
            }
            return html;
        }
    }
}