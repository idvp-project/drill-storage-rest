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
package org.apache.drill.exec.store.rest;

import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;
import org.apache.drill.shaded.guava.com.google.common.base.Stopwatch;
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableMultimap;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.store.rest.config.HttpMethod;
import org.apache.drill.exec.store.rest.config.RuntimeQueryConfig;
import org.apache.drill.exec.store.rest.helpers.HandlebarsHelper;
import org.apache.drill.exec.store.rest.query.ParameterValue;
import org.apache.drill.exec.store.rest.read.RestMetric;
import org.apache.drill.exec.store.rest.read.RestRecordReader;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpRequest;
import org.apache.http.client.HttpResponseException;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.*;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.AbstractResponseHandler;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultHttpRequestRetryHandler;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.protocol.HTTP;
import org.apache.http.util.Args;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.charset.UnsupportedCharsetException;
import java.sql.*;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * @author Oleg Zinoviev
 * @since 26.06.2017.
 */
public final class RequestHandler {

    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(RestRecordReader.class);

    private final RuntimeQueryConfig config;

    private long subqueryTime = 0;

    RequestHandler(RuntimeQueryConfig config) {
        this.config = config;
    }

    public Result execute(RestSubScan scan,
                          OperatorContext context,
                          DrillConfig drillConfig) throws URISyntaxException, IOException, ExecutionSetupException {
        Stopwatch stopwatch = Stopwatch.createStarted();
        try (CloseableHttpClient client = HttpClientBuilder.create()
                .useSystemProperties()
                .setRetryHandler(new DefaultHttpRequestRetryHandler(0, false))
                .build()) {

            HttpUriRequest request = createRequest(config, scan.getSpec(), drillConfig);
            try (CloseableHttpResponse response = client.execute(request)) {

                ResponseHandler<String> responseHandler = new RestResponseHandler();

                String body = responseHandler.handleResponse(response);
                ContentType contentType = ContentType.getOrDefault(response.getEntity());

                Map<String, String> headers = new HashMap<>();
                for (Header header : response.getAllHeaders()) {
                    headers.put(header.getName(), header.getValue());
                }

                return new Result(contentType, body, headers);
            } catch (HttpResponseException e) {
                UserException.Builder builder = UserException.dataReadError(e)
                        .addContext("url", request.getURI().toString())
                        .addContext("method", request.getMethod())
                        .addContext("headers", ArrayUtils.toString(request.getAllHeaders()));
                if (request instanceof HttpEntityEnclosingRequestBase) {
                    HttpEntity entity = ((HttpEntityEnclosingRequestBase) request).getEntity();
                    builder.addContext("body", entity.toString());
                }

                throw builder.build(logger);
            }

        } catch (SQLException e) {
            throw new ExecutionSetupException(e);
        } finally {
            long requestTime = stopwatch.stop().elapsed(TimeUnit.MILLISECONDS);
            context.getStats().addLongStat(RestMetric.TIME_REQUEST, requestTime);

            if (subqueryTime != 0) {
                context.getStats().addLongStat(RestMetric.TIME_SUBQUERIES, subqueryTime);
            }
        }
    }

    private HttpUriRequest createRequest(RuntimeQueryConfig config,
                                         RestScanSpec spec,
                                         DrillConfig drillConfig) throws URISyntaxException, IOException, SQLException {

        Map<String, ParameterValue> parameterValues = Collections.emptyMap();
        if (StringUtils.isNotBlank(spec.getParameters())) {
            parameterValues = RestRecordReader.MAPPER.readValue(spec.getParameters(),
                    new TypeReference<Map<String, ParameterValue>>() {
                    });
        }

        Map<String, Object> parameters = new HashMap<>();
        for (Map.Entry<String, ParameterValue> entry : parameterValues.entrySet()) {
            if (entry.getValue() != null && entry.getValue().getType() == ParameterValue.Type.QUERY) {
                String sql = Objects.toString(entry.getValue().getValue(), null);
                parameters.put(entry.getKey(), executeSingleColumnQuery(sql, drillConfig));
            } else if (entry.getValue() != null && entry.getValue().getType() == ParameterValue.Type.SUBQUERY) {
                String sql = Objects.toString(entry.getValue().getValue(), null);
                parameters.putAll(executeSubQuery(sql, drillConfig).asMap());
            } else {
                parameters.put(entry.getKey(), entry.getValue() == null ? null : entry.getValue().getValue());
            }
        }

        // Так как при передаче параметров с помощью SUBQUERY у нас могут быть названия параметров в неправильном регистре,
        // то копируем значения для key.lowerCase() и модифицируем resolver в handlebars
        parameters = addParametersLowerCase(parameters);

        URI uri = createURI(config, parameters);

        HttpUriRequest request;
        switch (config.getMethod()) {
            case GET: {
                request = new HttpGet(uri);
                break;
            }
            case POST: {
                request = new HttpPost(uri);
                break;
            }
            default: {
                throw new IllegalArgumentException("Unsupported method " + config.getMethod());
            }
        }

        for (Map.Entry<String, String> header : config.getHeaders().entrySet()) {
            if (header.getValue() == null) {
                request.addHeader(header.getKey(), null);
            } else {
                request.addHeader(header.getKey(), HandlebarsHelper.merge(header.getValue(), parameters));
            }
        }

        if (config.getMethod() == HttpMethod.POST && request instanceof HttpPost) {
            HttpPost post = (HttpPost) request;

            String body = null;
            if (StringUtils.isNotBlank(config.getBody())) {
                body = HandlebarsHelper.merge(config.getBody(), parameters);
            }

            if (body != null) {

                ContentType contentType = null;
                try {
                    Header contentTypeHeader = request.getFirstHeader(HttpHeaders.CONTENT_TYPE);
                    if (contentTypeHeader != null) {
                        contentType = ContentType.parse(contentTypeHeader.getValue());
                    }
                } catch (Exception e) {
                    logger.debug("Unknown content type");
                }

                Charset charset = StandardCharsets.UTF_8;
                if (contentType != null && contentType.getCharset() != null) {
                    charset = contentType.getCharset();
                }


                //Не ставим Content-Type здесь, он будет установлен как заголовок
                post.setEntity(new ExtendedByteArrayEntity(body.getBytes(charset), body));
            }
        }

        return request;
    }

    private Map<String, Object> addParametersLowerCase(Map<String, Object> parameters) {
        Map<String, Object> result = new HashMap<>(parameters);

        for (Map.Entry<String, Object> entry : parameters.entrySet()) {
            result.put(entry.getKey(), entry.getValue());
            result.putIfAbsent(StringUtils.lowerCase(entry.getKey()), entry.getValue());
        }

        return result;
    }

    private Object executeSingleColumnQuery(String sql, DrillConfig drillConfig) throws SQLException {
        Preconditions.checkNotNull(sql, "Subquery sql is null");

        List<Object> result = new ArrayList<>();

        Stopwatch stopwatch = Stopwatch.createStarted();

        int port = drillConfig.getInt("drill.exec.rpc.user.server.port");

        try (Connection connection = DriverManager.getConnection("jdbc:drill:drillbit=127.0.0.1:" + port)) {
            try (Statement statement = connection.createStatement()) {
                try (ResultSet resultSet = statement.executeQuery(sql)) {
                    while (resultSet.next()) {
                        result.add(resultSet.getObject(1));
                    }
                }
            }
        } finally {
            subqueryTime += stopwatch.stop().elapsed(TimeUnit.MILLISECONDS);
        }

        return Collections.unmodifiableList(result);
    }

    private ImmutableMultimap<String, Object> executeSubQuery(String sql, DrillConfig drillConfig) throws SQLException {
        Preconditions.checkNotNull(sql, "Subquery sql is null");

        ImmutableMultimap.Builder<String, Object> result = new ImmutableMultimap.Builder<>();

        Stopwatch stopwatch = Stopwatch.createStarted();

        int port = drillConfig.getInt("drill.exec.rpc.user.server.port");

        try (Connection connection = DriverManager.getConnection("jdbc:drill:drillbit=127.0.0.1:" + port)) {
            try (Statement statement = connection.createStatement()) {
                try (ResultSet resultSet = statement.executeQuery(sql)) {
                    ResultSetMetaData metaData = resultSet.getMetaData();
                    List<String> columns = new ArrayList<>(metaData.getColumnCount());
                    for (int i = 1; i <= metaData.getColumnCount(); i++) {
                        columns.add(metaData.getColumnName(i));
                    }

                    while (resultSet.next()) {
                        for (String column : columns) {
                            result.put(column, resultSet.getObject(column));
                        }
                    }
                }
            }
        } finally {
            subqueryTime += stopwatch.stop().elapsed(TimeUnit.MILLISECONDS);
        }

        return result.build();
    }


    private URI createURI(RuntimeQueryConfig config, Map<String, Object> parameters) throws URISyntaxException {

        String localUri = HandlebarsHelper.merge(config.getUrl(), parameters);
        URI scanUri = new URI(localUri);
        if (!scanUri.isAbsolute()) {
            Preconditions.checkNotNull(config.getBaseUrl(), "config.baseUrl");
            String baseUri = HandlebarsHelper.merge(config.getBaseUrl(), parameters);
            scanUri = new URI(baseUri).resolve(scanUri);
        }

        return scanUri;
    }

    @SuppressWarnings("unused")
    public final static class Result {
        private final ContentType contentType;
        private final String content;
        private final Map<String, String> headers;


        Result(ContentType contentType,
               String content,
               Map<String, String> headers) {
            this.contentType = contentType;
            this.content = content;
            this.headers = headers;
        }

        public ContentType getContentType() {
            return contentType;
        }

        public String getContent() {
            return content;
        }

        public Map<String, String> getHeaders() {
            return headers;
        }
    }

    private final static class RestResponseHandler extends AbstractResponseHandler<String> {

        @Override
        public String handleEntity(HttpEntity entity) throws IOException {
            InputStream stream = entity.getContent();
            if (stream == null) {
                return null;
            } else {
                try {
                    Args.check(entity.getContentLength() <= 2147483647L, "HTTP entity too large to be buffered in memory");
                    return bodyAsString(IOUtils.toByteArray(stream), ContentType.getOrDefault(entity));
                } finally {
                    stream.close();
                }
            }
        }

        private String bodyAsString(byte[] content, ContentType contentType) {
            if (content == null) {
                return null;
            }

            Charset charset = null;
            if (contentType != null) {
                charset = contentType.getCharset();
                if (charset == null && Objects.equals(ContentType.TEXT_XML.getMimeType(), contentType.getMimeType())) {
                    // если charset не передан, но у нас xml документ, то пытаемся посмотреть <?xml ..>

                    try {
                        XMLStreamReader xmlStreamReader = XMLInputFactory.newFactory().createXMLStreamReader(new ByteArrayInputStream(content));
                        try {

                            String characterEncodingScheme = xmlStreamReader.getCharacterEncodingScheme();
                            charset = Charset.forName(characterEncodingScheme);

                        } finally {
                            xmlStreamReader.close();
                        }
                    } catch (XMLStreamException | UnsupportedCharsetException e) {
                        logger.error("xml read error", e);
                    }
                }

                if (charset == null) {
                    ContentType byMimeType = ContentType.getByMimeType(contentType.getMimeType());
                    if (byMimeType != null) {
                        charset = byMimeType.getCharset();
                    }
                }
            }

            if (charset == null) {
                charset = HTTP.DEF_CONTENT_CHARSET;
            }

            return new String(content, charset);
        }
    }

    private final static class ExtendedByteArrayEntity extends ByteArrayEntity {

        private final String str;

        public ExtendedByteArrayEntity(byte[] bytes, String str) {
            super(bytes);
            this.str = str;
        }

        @Override
        public String toString() {
            return str;
        }
    }
}
