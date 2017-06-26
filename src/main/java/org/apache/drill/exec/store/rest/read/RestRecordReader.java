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
package org.apache.drill.exec.store.rest.read;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Stopwatch;
import org.apache.commons.io.Charsets;
import org.apache.commons.lang3.StringUtils;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.store.AbstractRecordReader;
import org.apache.drill.exec.store.easy.json.JsonProcessor;
import org.apache.drill.exec.store.easy.json.reader.CountingJsonReader;
import org.apache.drill.exec.store.rest.FilterPushDown;
import org.apache.drill.exec.store.rest.RequestHandler;
import org.apache.drill.exec.store.rest.RestSubScan;
import org.apache.drill.exec.store.rest.functions.FunctionsHelper;
import org.apache.drill.exec.vector.complex.fn.RestJsonReader;
import org.apache.drill.exec.vector.complex.impl.VectorContainerWriter;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.utils.HttpClientUtils;
import org.apache.http.entity.ContentType;
import org.apache.http.util.EntityUtils;
import org.json.JSONObject;
import org.json.XML;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static org.apache.drill.exec.store.easy.json.JSONRecordReader.DEFAULT_ROWS_PER_BATCH;

/**
 * @author Oleg Zinoviev
 * @since 19.06.2017.
 */
public class RestRecordReader extends AbstractRecordReader {
    private static final String APPLICATION_SOAP_XML = "application/soap+xml";
    private static final String CONTENT_COLUMN = "content";
    private static final String CONTENT_TYPE_COLUMN = "content_type";

    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(RestRecordReader.class);

    private final FragmentContext fragmentContext;
    private final RestSubScan scan;
    private final RequestHandler requestHandler;
    private final boolean enableAllTextMode;
    private final boolean readNumbersAsDouble;
    private final boolean unionEnabled;

    private JsonProcessor jsonReader;
    private VectorContainerWriter writer;
    private JsonProcessor.ReadState write = null;
    private long totalScanTime = 0L;
    private long totalScanRecords = 0;
    private OperatorContext operatorContext;
    private CloseableHttpResponse response;


    public RestRecordReader(FragmentContext fragmentContext,
                            RestSubScan scan,
                            RequestHandler requestHandler) {
        this.fragmentContext = fragmentContext;
        this.scan = scan;
        this.requestHandler = requestHandler;

        this.enableAllTextMode = fragmentContext.getOptions().getOption(ExecConstants.JSON_READER_ALL_TEXT_MODE_VALIDATOR);
        this.readNumbersAsDouble = fragmentContext.getOptions().getOption(ExecConstants.JSON_READ_NUMBERS_AS_DOUBLE_VALIDATOR);
        this.unionEnabled = fragmentContext.getOptions().getOption(ExecConstants.ENABLE_UNION_TYPE);
    }


    @Override
    public void setup(OperatorContext operatorContext, OutputMutator output) throws ExecutionSetupException {
        this.operatorContext = operatorContext;
        try {
            this.writer = new VectorContainerWriter(output, unionEnabled);
            if (isSkipQuery()) {
                this.jsonReader = new CountingJsonReader(fragmentContext.getManagedBuffer());
            } else {
                this.jsonReader = new RestJsonReader(fragmentContext.getManagedBuffer(),
                        enableAllTextMode,
                        true,
                        readNumbersAsDouble,
                        scan.getSpec().getFilterPushDown() == FilterPushDown.SOME ? scan.getSpec().getParameters() : Collections.emptyMap());
            }
            setupParser();
        } catch (final Exception e) {
            handleAndRaise(e);
        }
    }

    private void setupParser() throws IOException, URISyntaxException, ExecutionSetupException {
        response = requestHandler.execute(scan, operatorContext);

        ContentType contentType = requestHandler.extractContentType(response);

        if (Objects.equals(contentType.getMimeType(), ContentType.APPLICATION_JSON.getMimeType())) {
            jsonReader.setSource(response.getEntity().getContent());
        } else if (Objects.equals(contentType.getMimeType(), ContentType.APPLICATION_XML.getMimeType())
                || Objects.equals(contentType.getMimeType(), ContentType.TEXT_XML.getMimeType())
                || Objects.equals(contentType.getMimeType(), APPLICATION_SOAP_XML)) {
            Stopwatch stopwatch = Stopwatch.createStarted();
            try {
                String xml = EntityUtils.toString(response.getEntity(), Charsets.UTF_8);
                String result = FunctionsHelper.removeNamespaces(xml);
                JSONObject xmlJSONObj = XML.toJSONObject(result);
                String json = xmlJSONObj.toString();
                jsonReader.setSource(new ByteArrayInputStream(json.getBytes(Charsets.UTF_8)));
            } finally {
                operatorContext.getStats().addLongStat(RestMetric.TIME_XML_TRANSFORM, stopwatch.stop().elapsed(TimeUnit.MILLISECONDS));
            }
        } else {
            Stopwatch stopwatch = Stopwatch.createStarted();
            try {
                String text = EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);
                if (StringUtils.isNotEmpty(text)) {
                    JsonNodeFactory factory = JsonNodeFactory.withExactBigDecimals(false);
                    ObjectNode node = factory.objectNode();
                    node.set(CONTENT_COLUMN, factory.textNode(text));
                    node.set(CONTENT_TYPE_COLUMN, factory.textNode(contentType.getMimeType()));
                    jsonReader.setSource(node);
                }
            } finally {
                operatorContext.getStats().addLongStat(RestMetric.TIME_TEXT_TRANSFORM, stopwatch.stop().elapsed(TimeUnit.MILLISECONDS));
            }
        }



    }

    private void handleAndRaise(Exception e) throws UserException {

        String message = e.getMessage();
        int columnNr = -1;

        if (e instanceof JsonParseException) {
            final JsonParseException ex = (JsonParseException) e;
            message = ex.getOriginalMessage();
            columnNr = ex.getLocation().getColumnNr();
        }

        UserException.Builder exceptionBuilder = UserException.dataReadError(e)
                .message("%s - %s", "Failure reading JSON", message);
        if (columnNr > 0) {
            exceptionBuilder.pushContext("Column ", columnNr);
        }

        throw exceptionBuilder.build(logger);
    }

    @Override
    public int next() {
        Stopwatch stopwatch = Stopwatch.createStarted();
        int recordCount = 0;
        try {
            writer.allocate();
            writer.reset();
            if (write == JsonProcessor.ReadState.JSON_RECORD_PARSE_EOF_ERROR) {
                return recordCount;
            }

            while (recordCount < DEFAULT_ROWS_PER_BATCH) {
                try {
                    writer.setPosition(recordCount);
                    write = jsonReader.write(writer);
                    if (write == JsonProcessor.ReadState.WRITE_SUCCEED) {
                        recordCount++;
                    } else if (write == JsonProcessor.ReadState.JSON_RECORD_PARSE_ERROR || write == JsonProcessor.ReadState.JSON_RECORD_PARSE_EOF_ERROR) {
                        handleAndRaise(new Exception(scan.getSpec().getQuery() + " : line nos :" + (recordCount + 1)));
                    } else {
                        break;
                    }
                } catch (IOException ex) {
                    handleAndRaise(ex);
                }
            }
            jsonReader.ensureAtLeastOneField(writer);
            writer.setValueCount(recordCount);
            return recordCount;
        } finally {
            totalScanTime += stopwatch.stop().elapsed(TimeUnit.MILLISECONDS);
            totalScanRecords += recordCount;
        }
    }

    private void updateStats() {
        operatorContext.getStats().addLongStat(RestMetric.TIME_RESULT_SCAN, totalScanTime);
        operatorContext.getStats().addLongStat(RestMetric.TOTAL_SCAN, totalScanRecords);
    }

    @Override
    public void close() throws Exception {
        updateStats();
        HttpClientUtils.closeQuietly(response);
        writer.close();
    }
}
