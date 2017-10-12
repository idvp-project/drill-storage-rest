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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Stopwatch;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.store.AbstractRecordReader;
import org.apache.drill.exec.store.easy.json.JsonProcessor;
import org.apache.drill.exec.store.easy.json.reader.CountingJsonReader;
import org.apache.drill.exec.store.rest.RequestHandler;
import org.apache.drill.exec.store.rest.RestSubScan;
import org.apache.drill.exec.store.rest.functions.FunctionsHelper;
import org.apache.drill.exec.vector.complex.fn.RestJsonReader;
import org.apache.drill.exec.vector.complex.impl.VectorContainerWriter;
import org.apache.http.entity.ContentType;
import org.json.JSONObject;
import org.json.XML;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static org.apache.drill.exec.store.easy.json.JSONRecordReader.DEFAULT_ROWS_PER_BATCH;

/**
 * @author Oleg Zinoviev
 * @since 19.06.2017.
 */
public class RestRecordReader extends AbstractRecordReader {
    public static final ObjectMapper MAPPER = new ObjectMapper();

    private static final String APPLICATION_SOAP_XML = "application/soap+xml";
    private static final String CONTENT_COLUMN = "content";
    private static final String OBJECT_COLUMN = "object";
    private static final String HEADERS_COLUMN = "headers";

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
                        readNumbersAsDouble);
            }
            setupParser();
        } catch (final Throwable e) {
            handleAndRaise(e);
        }
    }

    private void setupParser() throws IOException, URISyntaxException, ExecutionSetupException {
        RequestHandler.Result result = requestHandler.execute(scan, operatorContext);

        JsonNodeFactory factory = JsonNodeFactory.withExactBigDecimals(false);

        ObjectNode rootNode = factory.objectNode();
        rootNode.set(CONTENT_COLUMN, factory.textNode(result.getContent()));
        rootNode.set(HEADERS_COLUMN, MAPPER.valueToTree(result.getHeaders()));

        if (result.getContent() != null) {

            ContentType contentType = result.getContentType();
            if (contentType != null) {
                if (Objects.equals(result.getContentType().getMimeType(), ContentType.APPLICATION_JSON.getMimeType())) {
                    Stopwatch stopwatch = Stopwatch.createStarted();
                    try {
                        rootNode.set(OBJECT_COLUMN, MAPPER.readTree(result.getContent()));
                    } catch (Exception e) {
                        logger.error("Cannot read json", e);
                    } finally {
                        operatorContext.getStats().addLongStat(RestMetric.TIME_JSON_TRANSFORM, stopwatch.stop().elapsed(TimeUnit.MILLISECONDS));
                    }
                } else if (Objects.equals(contentType.getMimeType(), ContentType.APPLICATION_XML.getMimeType())
                        || Objects.equals(contentType.getMimeType(), ContentType.TEXT_XML.getMimeType())
                        || Objects.equals(contentType.getMimeType(), APPLICATION_SOAP_XML)) {
                    Stopwatch stopwatch = Stopwatch.createStarted();
                    try {
                        String clearXml = FunctionsHelper.removeNamespaces(result.getContent());
                        JSONObject xmlJSONObj = XML.toJSONObject(clearXml);
                        rootNode.set(OBJECT_COLUMN, MAPPER.readTree(xmlJSONObj.toString()));
                    } catch (Exception e) {
                        logger.error("Cannot read xml", e);
                    } finally {
                        operatorContext.getStats().addLongStat(RestMetric.TIME_XML_TRANSFORM, stopwatch.stop().elapsed(TimeUnit.MILLISECONDS));
                    }
                }
            }
        } else {
            rootNode.set(OBJECT_COLUMN, factory.nullNode());
        }

        jsonReader.setSource(rootNode);
    }

    private void handleAndRaise(Throwable e) throws UserException {

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
        writer.close();
    }
}
