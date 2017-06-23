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

import com.google.common.base.Charsets;
import io.netty.buffer.DrillBuf;
import org.apache.commons.io.IOUtils;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.exec.expr.holders.ValueHolder;
import org.apache.drill.exec.vector.complex.writer.BaseWriter;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.xpath.*;
import java.io.InputStream;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;

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
            String html = FunctionsHelper.asString(source);
            String localSelector = FunctionsHelper.asString(selector);
            Document parse = Jsoup.parse(html);
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
    }

    public static class XPathSelectorFuncBody {
        private XPathSelectorFuncBody() {
        }

        public static void eval(ValueHolder source,
                                ValueHolder selector,
                                BaseWriter.ComplexWriter output,
                                DrillBuf buffer) {
            try {
                String xml = FunctionsHelper.asString(source);
                String localSelector = FunctionsHelper.asString(selector);

                InputStream stream = IOUtils.toInputStream(xml, StandardCharsets.UTF_8);

                try {
                    XPathFactory xPathFactory = XPathFactory.newInstance();
                    XPath xPath = xPathFactory.newXPath();
                    XPathExpression expression = xPath.compile(localSelector);
                    NodeList elements = (NodeList) expression.evaluate(new InputSource(stream), XPathConstants.NODESET);

                    Transformer transformer = TransformerFactory.newInstance().newTransformer();
                    transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");

                    BaseWriter.ListWriter listWriter = output.rootAsList();
                    listWriter.startList();
                    for (int i = 0; i < elements.getLength(); i++) {
                        Node element = elements.item(i);
                        StringWriter writer = new StringWriter();
                        transformer.transform(new DOMSource(element), new StreamResult(writer));

                        String inner = writer.toString();
                        final byte[] strBytes = inner.getBytes(Charsets.UTF_8);
                        buffer = buffer.reallocIfNeeded(strBytes.length);
                        buffer.setBytes(0, strBytes);
                        listWriter.varChar().writeVarChar(0, strBytes.length, buffer);
                    }
                    listWriter.endList();
                } finally {
                    IOUtils.closeQuietly(stream);
                }

            } catch (XPathExpressionException | TransformerException e) {
                throw new DrillRuntimeException(e);
            }
        }
    }
}
