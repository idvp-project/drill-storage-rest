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
import org.apache.commons.io.IOUtils;
import org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers;
import org.apache.drill.exec.expr.holders.*;
import org.apache.drill.exec.store.rest.read.XmlRestRecordReader;

import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import java.io.InputStream;
import java.io.StringWriter;

/**
 * @author Oleg Zinoviev
 * @since 22.06.2017.
 */
public final class FunctionsHelper {
    private FunctionsHelper() {
    }

    static String asString(ValueHolder source) {
        String result;
        if (source instanceof VarCharHolder) {
            VarCharHolder vch = (VarCharHolder) source;
            result = StringFunctionHelpers.toStringFromUTF8(vch.start, vch.end, vch.buffer);
        } else if (source instanceof NullableVarCharHolder) {
            NullableVarCharHolder vch = (NullableVarCharHolder) source;
            result = StringFunctionHelpers.toStringFromUTF8(vch.start, vch.end, vch.buffer);
        } else if (source instanceof Var16CharHolder) {
            Var16CharHolder vch = (Var16CharHolder) source;
            result = StringFunctionHelpers.toStringFromUTF16(vch.start, vch.end, vch.buffer);
        } else if (source instanceof NullableVar16CharHolder) {
            NullableVar16CharHolder vch = (NullableVar16CharHolder) source;
            result = StringFunctionHelpers.toStringFromUTF16(vch.start, vch.end, vch.buffer);
        } else if (source instanceof VarBinaryHolder) {
            VarBinaryHolder vch = (VarBinaryHolder) source;
            result = StringFunctionHelpers.toStringFromUTF8(vch.start, vch.end, vch.buffer);
        } else if (source instanceof NullableVarBinaryHolder) {
            NullableVarBinaryHolder vch = (NullableVarBinaryHolder) source;
            result = StringFunctionHelpers.toStringFromUTF8(vch.start, vch.end, vch.buffer);
        } else {
            throw new RuntimeException("Unsupported type");
        }
        return result;
    }

    static void writeString(String value, DrillBuf buffer, ValueHolder output) {
        final byte[] strBytes = value.getBytes(Charsets.UTF_8);
        buffer = buffer.reallocIfNeeded(strBytes.length);
        buffer.setBytes(0, strBytes);

        if (output instanceof VarCharHolder) {
            VarCharHolder vch = (VarCharHolder) output;
            vch.buffer = buffer;
            vch.start = 0;
            vch.end = strBytes.length;
        } else if (output instanceof NullableVarCharHolder) {
            NullableVarCharHolder vch = (NullableVarCharHolder) output;
            vch.buffer = buffer;
            vch.start = 0;
            vch.end = strBytes.length;
        } else if (output instanceof Var16CharHolder) {
            Var16CharHolder vch = (Var16CharHolder) output;
            vch.buffer = buffer;
            vch.start = 0;
            vch.end = strBytes.length;
        } else if (output instanceof NullableVar16CharHolder) {
            NullableVar16CharHolder vch = (NullableVar16CharHolder) output;
            vch.buffer = buffer;
            vch.start = 0;
            vch.end = strBytes.length;
        } else if (output instanceof VarBinaryHolder) {
            VarBinaryHolder vch = (VarBinaryHolder) output;
            vch.buffer = buffer;
            vch.start = 0;
            vch.end = strBytes.length;
        } else if (output instanceof NullableVarBinaryHolder) {
            NullableVarBinaryHolder vch = (NullableVarBinaryHolder) output;
            vch.buffer = buffer;
            vch.start = 0;
            vch.end = strBytes.length;
        } else {
            throw new RuntimeException("Unknown output type");
        }
    }

    public static String removeNamespaces(String source) {
        InputStream stream = IOUtils.toInputStream(source, org.apache.commons.io.Charsets.UTF_8);
        try {

            ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
            if (classLoader == null) {
                classLoader = XmlRestRecordReader.class.getClassLoader();
            }

            TransformerFactory factory = TransformerFactory.newInstance();
            Source xslt = new StreamSource(classLoader.getResourceAsStream("remove-namespaces.xslt"));
            Transformer transformer = factory.newTransformer(xslt);

            Source text = new StreamSource(stream);
            StringWriter result = new StringWriter();
            transformer.transform(text, new StreamResult(result));
            return result.toString();
        } catch (TransformerException e) {
            throw new RuntimeException(e);
        } finally {
            IOUtils.closeQuietly(stream);
        }

    }
}
