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
import org.apache.drill.exec.expr.holders.ValueHolder;
import org.apache.drill.exec.vector.complex.fn.JsonReader;
import org.apache.drill.exec.vector.complex.writer.BaseWriter;
import org.json.JSONObject;
import org.json.XML;

import java.io.IOException;

/**
 * @author Oleg Zinoviev
 * @since 23.06.2017.
 */
@SuppressWarnings("WeakerAccess")
public final class ConvertFunctionsBody {
    private ConvertFunctionsBody() {
    }

    public static class ConvertFromXmlFuncBody {
        private ConvertFromXmlFuncBody() {
        }

        public static void eval(ValueHolder source,
                                BaseWriter.ComplexWriter output,
                                DrillBuf buffer) {
            String xml = FunctionsHelper.asString(source);
            String result = FunctionsHelper.removeNamespaces(xml);

            JSONObject xmlJSONObj = XML.toJSONObject(result);
            String json = xmlJSONObj.toString();

            JsonReader jsonReader = new JsonReader(buffer, false, false, false);
            try {
                jsonReader.setSource(json);
                jsonReader.write(output);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

}