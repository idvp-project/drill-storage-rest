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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.drill.common.JSONOptions;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OptimizerRulesContext;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.physical.impl.ScanBatch;
import org.apache.drill.exec.record.CloseableRecordBatch;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.AbstractStoragePlugin;
import org.apache.drill.exec.store.RecordReader;
import org.apache.drill.exec.store.SchemaConfig;
import org.apache.drill.exec.store.rest.config.RuntimeQueryConfig;
import org.apache.drill.exec.store.rest.query.RestPushFilterIntoScan;
import org.apache.drill.exec.store.rest.read.GenericRestRecordReader;
import org.apache.drill.exec.store.rest.read.JsonRestRecordReader;
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
import java.util.Set;

/**
 * @author Oleg Zinoviev
 * @since 15.06.2017.
 */
@SuppressWarnings("FieldCanBeLocal")
public class RestStoragePlugin extends AbstractStoragePlugin {


    @SuppressWarnings("unused")
    private final DrillbitContext context;
    private final RestSchemaFactory schemaFactory;
    private final RestStoragePluginConfig config;
    private final String name;

    public RestStoragePlugin(RestStoragePluginConfig config, DrillbitContext context, String name) {
        this.context = context;
        this.schemaFactory = new RestSchemaFactory(name, this);
        this.config = config;
        this.name = name;
    }

    @Override
    public RestStoragePluginConfig getConfig() {
        return config;
    }

    @Override
    public void registerSchemas(SchemaConfig schemaConfig, SchemaPlus schemaPlus) throws IOException {
        schemaFactory.registerSchemas(schemaConfig, schemaPlus);
    }

    @Override
    public AbstractGroupScan getPhysicalScan(String userName, JSONOptions selection, List<SchemaPath> columns) throws IOException {
        RestScanSpec scanSpec = selection.getListWith(new ObjectMapper(), new TypeReference<RestScanSpec>() {
        });
        return new RestGroupScan(userName, this, scanSpec, columns);
    }

    @SuppressWarnings("deprecation")
    @Override
    public Set<? extends RelOptRule> getPhysicalOptimizerRules(OptimizerRulesContext optimizerRulesContext) {
        return ImmutableSet.of(RestPushFilterIntoScan.FILTER_ON_SCAN);
    }

    String getName() {
        return name;
    }

    CloseableRecordBatch createBatchScan(FragmentContext context, RestSubScan scan) throws
            URISyntaxException,
            IOException,
            ExecutionSetupException {

        RuntimeQueryConfig config = this.config.getRuntimeConfig(scan.getSpec().getQuery());

        CloseableHttpClient client = RestClientProvider.INSTANCE.getClient(this);
        HttpUriRequest request = RestClientProvider.INSTANCE.createRequest(config, scan.getSpec());
        CloseableHttpResponse response = client.execute(request);
        handleResponseStatus(response);

        ContentType contentType = ContentType.getOrDefault(response.getEntity());

        RecordReader reader;

        if (Objects.equals(contentType.getMimeType(), ContentType.APPLICATION_JSON.getMimeType())) {
            reader = new JsonRestRecordReader(context, scan, response);
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