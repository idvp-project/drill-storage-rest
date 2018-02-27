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

import org.apache.commons.io.IOUtils;
import org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers;
import org.apache.drill.exec.expr.holders.*;

import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import java.io.InputStream;
import java.io.StringWriter;
import java.util.Objects;

/**
 * @author Oleg Zinoviev
 * @since 22.06.2017.
 */
@SuppressWarnings("WeakerAccess")
public final class FunctionsHelper {
    private FunctionsHelper() {
    }

    @SuppressWarnings("deprecation")
    public static String asString(ValueHolder source) {
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
        } else if (source instanceof ObjectHolder) {
            ObjectHolder oh = (ObjectHolder) source;
            return Objects.toString(oh.obj, null);
        } else {
            throw new RuntimeException("Unsupported type");
        }
        return result;
    }

    public static String removeNamespaces(String source) {
        InputStream stream = IOUtils.toInputStream(source, org.apache.commons.io.Charsets.UTF_8);
        try {

            ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
            if (classLoader == null) {
                classLoader = FunctionsHelper.class.getClassLoader();
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
