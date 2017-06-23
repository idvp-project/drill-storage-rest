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
package org.apache.drill.exec.store.rest;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.impl.BatchCreator;
import org.apache.drill.exec.physical.impl.ScanBatch;
import org.apache.drill.exec.record.CloseableRecordBatch;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.store.RecordReader;
import org.apache.drill.exec.store.rest.config.RuntimeQueryConfig;
import org.apache.drill.exec.store.rest.read.GenericRestRecordReader;
import org.apache.drill.exec.store.rest.read.JsonRestRecordReader;
import org.apache.drill.exec.store.rest.read.XmlRestRecordReader;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.utils.HttpClientUtils;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.CloseableHttpClient;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * @author Oleg Zinoviev
 * @since 16.06.2017.
 */
@SuppressWarnings("unused")
public class RestScanCreator implements BatchCreator<RestSubScan> {
    private static final String APPLICATION_SOAP_XML = "application/soap+xml";

    @Override
    public CloseableRecordBatch getBatch(FragmentContext context, RestSubScan scan, List<RecordBatch> children) throws ExecutionSetupException {
        assert children == null || children.isEmpty();
        try {
            return createBatchScan(context, scan);
        } catch (URISyntaxException | IOException e) {
            throw new ExecutionSetupException(e);
        }
    }

    private CloseableRecordBatch createBatchScan(FragmentContext context, RestSubScan scan) throws
            URISyntaxException,
            IOException,
            ExecutionSetupException {

        RuntimeQueryConfig config = scan.getStoragePluginConfig().getRuntimeConfig(scan.getSpec().getQuery());

        CloseableHttpClient client = RestClientProvider.INSTANCE.getClient(scan.getStoragePlugin());
        HttpUriRequest request = RestClientProvider.INSTANCE.createRequest(config, scan.getSpec());
        CloseableHttpResponse response = client.execute(request);
        handleResponseStatus(response);

        ContentType contentType = ContentType.TEXT_PLAIN;
        if (!config.isIgnoreContentType()) {
            contentType = ContentType.getOrDefault(response.getEntity());
        }

        RecordReader reader;

        if (Objects.equals(contentType.getMimeType(), ContentType.APPLICATION_JSON.getMimeType())) {
            reader = new JsonRestRecordReader(context, scan, response);
        } else if (Objects.equals(contentType.getMimeType(), ContentType.APPLICATION_XML.getMimeType())
                || Objects.equals(contentType.getMimeType(), ContentType.TEXT_XML.getMimeType())
                || Objects.equals(contentType.getMimeType(), APPLICATION_SOAP_XML)) {
            reader = new XmlRestRecordReader(context, scan, response);
        } else {
            reader = new GenericRestRecordReader(context, scan, response);
        }

        return new ScanBatch(scan, context, Collections.singleton(reader).iterator());
    }

    private void handleResponseStatus(CloseableHttpResponse response) throws ExecutionSetupException {
        if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
            HttpClientUtils.closeQuietly(response);
            throw new ExecutionSetupException(response.getStatusLine().getReasonPhrase());
        }
    }
}
