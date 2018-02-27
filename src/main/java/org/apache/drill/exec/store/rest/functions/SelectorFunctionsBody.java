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
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.PathNotFoundException;
import org.apache.commons.lang3.StringUtils;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.expr.holders.ValueHolder;
import org.codehaus.jackson.map.ObjectMapper;
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
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathFactory;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;

/**
 * @author Oleg Zinoviev
 * @since 21.06.2017.
 */
@SuppressWarnings("WeakerAccess")
public final class SelectorFunctionsBody {

    static final ObjectMapper mapper = new ObjectMapper();
    static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SelectorFunctionsBody.class);


    private SelectorFunctionsBody() {
    }

    public static Iterable<byte[]> select(ValueHolder typeHolder,
                                          ValueHolder sourceHolder,
                                          ValueHolder selectorHolder) {

        String t = FunctionsHelper.asString(typeHolder);

        if ("css".equalsIgnoreCase(t)) {
            return DOMSelectorFuncBody.eval(sourceHolder, selectorHolder);
        }

        if ("xPath".equalsIgnoreCase(t)) {
            return XPathSelectorFuncBody.eval(sourceHolder, selectorHolder);
        }

        if ("jPath".equalsIgnoreCase(t) || "jsonPath".equalsIgnoreCase(t)) {
            return JsonPathSelectorFuncBody.eval(sourceHolder, selectorHolder);
        }

        throw UserException.functionError()
                .message("Unsupported selector type: %s", t)
                .build(logger);
    }

    public static class DOMSelectorFuncBody {
        private DOMSelectorFuncBody() {
        }

        public static Iterable<byte[]> eval(ValueHolder source,
                                            ValueHolder selector) {
            String html = FunctionsHelper.asString(source);
            String localSelector = FunctionsHelper.asString(selector);
            if (StringUtils.isEmpty(html)) {
                return Collections.emptyList();
            }

            try {
                Document parse = Jsoup.parse(html);
                Elements elements = parse.select(localSelector);
                Iterator<Element> iterator = elements.iterator();
                return () -> new Iterator<byte[]>() {

                    @Override
                    public boolean hasNext() {
                        return iterator.hasNext();
                    }

                    @Override
                    public byte[] next() {
                        String inner = iterator.next().html();
                        return inner.getBytes(Charsets.UTF_8);
                    }
                };
            } catch (Exception e) {
                throw UserException.functionError(e).message("CssSelectorFuncBody").build(logger);
            }
        }
    }

    public static class XPathSelectorFuncBody {
        private XPathSelectorFuncBody() {
        }

        public static Iterable<byte[]> eval(ValueHolder source,
                                            ValueHolder selector) {
            try {
                String xml = FunctionsHelper.asString(source);
                String localSelector = FunctionsHelper.asString(selector);
                if (StringUtils.isEmpty(xml)) {
                    return Collections.emptyList();
                }

                XPathFactory xPathFactory = XPathFactory.newInstance();
                XPath xPath = xPathFactory.newXPath();
                XPathExpression expression = xPath.compile(localSelector);
                NodeList elements = (NodeList) expression.evaluate(new InputSource(new StringReader(xml)), XPathConstants.NODESET);

                Transformer transformer = TransformerFactory.newInstance().newTransformer();
                transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");

                return () -> new Iterator<byte[]>() {

                    private int current = 0;

                    @Override
                    public boolean hasNext() {
                        return current < elements.getLength() ;
                    }

                    @Override
                    public byte[] next() {
                        Node element = elements.item(current);
                        current++;

                        StringWriter writer = new StringWriter();
                        try {
                            transformer.transform(new DOMSource(element), new StreamResult(writer));
                        } catch (TransformerException e) {
                            throw new DrillRuntimeException(e);
                        }

                        String inner = writer.toString();
                        return inner.getBytes(Charsets.UTF_8);

                    }
                };

            } catch (Exception e) {
                throw UserException.functionError(e).message("XPathSelectorFuncBody").build(logger);
            }
        }
    }

    public static class JsonPathSelectorFuncBody {
        private JsonPathSelectorFuncBody() {
        }

        public static Iterable<byte[]> eval(ValueHolder source, ValueHolder selector) {
            String json = FunctionsHelper.asString(source);
            String localSelector = FunctionsHelper.asString(selector);
            return eval(json, localSelector);
        }

        static Iterable<byte[]> eval(String json, String localSelector) {
            if (StringUtils.isEmpty(json)) {
                return Collections.emptyList();
            }

            try {
                Object result;
                try {
                    result = JsonPath.read(json, localSelector);
                } catch (PathNotFoundException e) {
                    return Collections.emptyList();
                }

                if (result == null) {
                    return Collections.emptyList();
                } else if (result instanceof Collection) {
                    Iterator<?> iterator = ((Collection<?>) result).iterator();
                    return () -> new Iterator<byte[]>() {

                        @Override
                        public boolean hasNext() {
                            return iterator.hasNext();
                        }

                        @Override
                        public byte[] next() {
                            Object inner = iterator.next();
                            try {
                                return mapper.writeValueAsBytes(inner);
                            } catch (IOException e) {
                                throw UserException.functionError(e).message("JsonPathSelectorFuncBody").build(logger);
                            }
                        }
                    };
                } else {
                    return Collections.singletonList(mapper.writeValueAsBytes(result));
                }


            } catch (Exception e) {
                throw UserException.functionError(e).message("JsonPathSelectorFuncBody").build(logger);
            }
        }
    }
}
