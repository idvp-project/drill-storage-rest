/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.store.rest.functions;

import io.netty.buffer.DrillBuf;
import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.annotations.Workspace;
import org.apache.drill.exec.expr.holders.VarCharHolder;
import org.apache.drill.exec.vector.complex.writer.BaseWriter;

import javax.inject.Inject;

/**
 * @author Oleg Zinoviev
 * @since 22.06.2017.
 */
@SuppressWarnings("unused")
public class ConvertFunctions {
    private ConvertFunctions() {
    }

    @FunctionTemplate(name = "convert_fromXML",
            scope = FunctionTemplate.FunctionScope.SIMPLE,
            nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
    public static class ConvertFromXmlFunc implements DrillSimpleFunc {

        @Param
        VarCharHolder source;

        @Output
        BaseWriter.ComplexWriter output;

        @Workspace
        org.apache.drill.exec.vector.complex.fn.JsonReader jsonReader;

        @Inject
        DrillBuf buffer;

        @Override
        public void setup() {
            jsonReader = new org.apache.drill.exec.vector.complex.fn.JsonReader.Builder(buffer)
                    .allTextMode(false)
                    .enableNanInf(false)
                    .readNumbersAsDouble(false)
                    .skipOuterList(false)
                    .build();
        }

        @Override
        public void eval() {
            String result = org.apache.drill.exec.store.rest.functions.ConvertFunctionsBody.ConvertFromXmlFuncBody.eval(source);
            try {
                if (result == null) {
                    result = "";
                }

                jsonReader.setSource(result);
                jsonReader.write(output);
                buffer = jsonReader.getWorkBuf();
            } catch (Exception e) {
                throw org.apache.drill.common.exceptions.UserException.functionError(e).build();
            }
        }
    }

}
