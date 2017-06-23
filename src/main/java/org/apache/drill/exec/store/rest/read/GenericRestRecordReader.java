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
package org.apache.drill.exec.store.rest.read;

import com.google.common.base.Stopwatch;
import org.apache.commons.lang3.StringUtils;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.store.AbstractRecordReader;
import org.apache.drill.exec.store.rest.FilterPushDown;
import org.apache.drill.exec.store.rest.RestSubScan;
import org.apache.drill.exec.vector.complex.fn.ReaderHelper;
import org.apache.drill.exec.vector.complex.fn.WorkingBufferProxy;
import org.apache.drill.exec.vector.complex.impl.VectorContainerWriter;
import org.apache.drill.exec.vector.complex.writer.BaseWriter;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.utils.HttpClientUtils;
import org.apache.http.entity.ContentType;
import org.apache.http.util.EntityUtils;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @author Oleg Zinoviev
 * @since 20.06.2017.
 */
public final class GenericRestRecordReader extends AbstractRecordReader {
    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(GenericRestRecordReader.class);
    private static final String CONTENT_COLUMN = "content";
    private static final String CONTENT_TYPE_COLUMN = "content_type";

    private final CloseableHttpResponse response;
    private final WorkingBufferProxy workingBuffer;
    private final Map<String, Object> pushedDownFilters;

    private VectorContainerWriter writer;
    private boolean read = false;
    private OperatorContext operatorContext;

    public GenericRestRecordReader(FragmentContext fragmentContext,
                                   RestSubScan scan,
                                   CloseableHttpResponse response) {
        this.response = response;
        this.workingBuffer = new WorkingBufferProxy(fragmentContext.getManagedBuffer());
        this.pushedDownFilters = scan.getSpec().getFilterPushDown() == FilterPushDown.SOME ? scan.getSpec().getParameters() : Collections.emptyMap();
    }


    @Override
    public void setup(OperatorContext operatorContext, OutputMutator output) throws ExecutionSetupException {
        this.operatorContext = operatorContext;
        try{
            this.writer = new VectorContainerWriter(output, false);
        }catch(final Exception e){
            handleAndRaise(e);
        }
    }

    @Override
    public int next() {
        writer.allocate();
        writer.reset();
        int recordCount = 0;
        if (read) {
            return recordCount;
        }

        Stopwatch stopwatch = Stopwatch.createStarted();
        try {
            String text = EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);
            if (StringUtils.isNotEmpty(text)) {
                writer.setPosition(recordCount);
                BaseWriter.MapWriter mapWriter = writer.rootAsMap();
                mapWriter.start();

                String mimeType = ContentType.getOrDefault(response.getEntity()).getMimeType();

                mapWriter.varChar(CONTENT_COLUMN).writeVarChar(0,
                        workingBuffer.prepareVarCharHolder(text),
                        workingBuffer.getBuf());
                mapWriter.varChar(CONTENT_TYPE_COLUMN).writeVarChar(0,
                        workingBuffer.prepareVarCharHolder(mimeType),
                        workingBuffer.getBuf());

                for (Map.Entry<String, Object> entry : pushedDownFilters.entrySet()) {
                    if (CONTENT_COLUMN.equals(entry.getKey())
                            || CONTENT_TYPE_COLUMN.equals(entry.getKey())
                            || entry.getValue() == null) {
                        continue;
                    }

                    ReaderHelper.write(mapWriter, entry.getKey(), entry.getValue(), workingBuffer);
                }


                mapWriter.end();

                recordCount++;
                writer.setValueCount(recordCount);
            }
        } catch (Exception e) {
            handleAndRaise(e);
        } finally {
            operatorContext.getStats().addLongStat(RestMetric.TIME_RESULT_SCAN, stopwatch.stop().elapsed(TimeUnit.MILLISECONDS));
            operatorContext.getStats().addLongStat(RestMetric.TOTAL_SCAN, 1L);
            read = true;
        }

        return recordCount;
    }

    @Override
    public void close() throws Exception {
        HttpClientUtils.closeQuietly(response);
        writer.close();
    }

    private void handleAndRaise(Exception e) throws UserException {
        String message = e.getMessage();

        UserException.Builder exceptionBuilder = UserException.dataReadError(e)
                .message("%s - %s", "Failure reading TEXT", message);

        throw exceptionBuilder.build(logger);
    }
}
