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
import org.apache.drill.exec.expr.holders.VarCharHolder;

import javax.inject.Inject;

/**
 * @author Oleg Zinoviev
 * @since 30.10.17
 **/
@SuppressWarnings("unused")
public class GeoShapeFunctions {
    private GeoShapeFunctions() {
    }

    @FunctionTemplate(name = "geoShape",
            scope = FunctionTemplate.FunctionScope.SIMPLE,
            nulls = FunctionTemplate.NullHandling.NULL_IF_NULL,
            isRandom = true)
    public static class GeoShape1Func implements DrillSimpleFunc {
        @Param
        VarCharHolder source;

        @Output
        org.apache.drill.exec.vector.complex.writer.BaseWriter.ComplexWriter output;

        @Inject
        DrillBuf buffer;

        @Override
        public void setup() {
        }

        @Override
        public void eval() {
            org.apache.drill.exec.vector.complex.writer.BaseWriter.ListWriter listWriter = output.rootAsList();
            listWriter.startList();
            for (byte[] bytes : org.apache.drill.exec.store.rest.functions.GeoShapeFunctionsBody.convert(source, "$..geometry")) {
                buffer = buffer.reallocIfNeeded(bytes.length);
                buffer.setBytes(0, bytes);
                listWriter.varChar().writeVarChar(0, bytes.length, buffer);
            }
            listWriter.endList();
        }
    }

    @FunctionTemplate(name = "geoShape",
            scope = FunctionTemplate.FunctionScope.SIMPLE,
            nulls = FunctionTemplate.NullHandling.NULL_IF_NULL,
            isRandom = true)
    public static class GeoShape2Func implements DrillSimpleFunc {
        @Param
        VarCharHolder source;

        @Param
        VarCharHolder jsonPath;

        @Output
        org.apache.drill.exec.vector.complex.writer.BaseWriter.ComplexWriter output;

        @Inject
        DrillBuf buffer;

        @Override
        public void setup() {
        }

        @Override
        public void eval() {
            String path = org.apache.drill.exec.store.rest.functions.DecodeFunctionsBody.UrlDecodeVarCharFuncBody.eval(jsonPath);

            org.apache.drill.exec.vector.complex.writer.BaseWriter.ListWriter listWriter = output.rootAsList();
            listWriter.startList();
            for (byte[] bytes : org.apache.drill.exec.store.rest.functions.GeoShapeFunctionsBody.convert(source, path)) {
                buffer = buffer.reallocIfNeeded(bytes.length);
                buffer.setBytes(0, bytes);
                listWriter.varChar().writeVarChar(0, bytes.length, buffer);
            }
            listWriter.endList();
        }
    }
}
