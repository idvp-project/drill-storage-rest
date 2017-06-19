package org.apache.drill.exec.store.rest;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.drill.common.JSONOptions;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OptimizerRulesContext;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.physical.impl.ScanBatch;
import org.apache.drill.exec.record.CloseableRecordBatch;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.AbstractStoragePlugin;
import org.apache.drill.exec.store.RecordReader;
import org.apache.drill.exec.store.SchemaConfig;
import org.apache.drill.exec.store.rest.query.RestPushFilterIntoScan;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultHttpRequestRetryHandler;
import org.apache.http.impl.client.HttpClientBuilder;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.util.stream.Collectors;

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
    @SuppressWarnings("unused")
    private final String name;

    public RestStoragePlugin(RestStoragePluginConfig config, DrillbitContext context, String name) {
        this.context = context;
        this.schemaFactory = new RestSchemaFactory(name, this);
        this.config = config;
        this.name = name;
    }

    @Override
    public StoragePluginConfig getConfig() {
        return config;
    }

    @Override
    public void registerSchemas(SchemaConfig schemaConfig, SchemaPlus schemaPlus) throws IOException {
        schemaFactory.registerSchemas(schemaConfig, schemaPlus);
    }

    @Override
    public AbstractGroupScan getPhysicalScan(String userName, JSONOptions selection, List<SchemaPath> columns) throws IOException {
        RestScanSpec scanSpec = selection.getListWith(new ObjectMapper(), new TypeReference<RestScanSpec>() {});
        return new RestGroupScan(userName, this, scanSpec, columns);
    }

    @Override
    public Set<? extends RelOptRule> getPhysicalOptimizerRules(OptimizerRulesContext optimizerRulesContext) {
        return ImmutableSet.of(RestPushFilterIntoScan.FILTER_ON_SCAN);
    }

    CloseableRecordBatch createBatchScan(FragmentContext context, RestSubScan scan) throws
            URISyntaxException,
            IOException,
            ExecutionSetupException {

        CloseableHttpClient client = HttpClientBuilder.create()
                .useSystemProperties()
                .setRetryHandler(new DefaultHttpRequestRetryHandler(0, false))
                .build();

        HttpGet request = new HttpGet(createURI(scan.getSpec()));
        HttpResponse response = client.execute(request);
        if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
            throw new ExecutionSetupException(response.getStatusLine().getReasonPhrase());
        }

        RecordReader reader = new RestRecordReader(context, scan, client, response);
        return new ScanBatch(scan, context, Collections.singleton(reader).iterator());
    }

    private URI createURI(RestScanSpec scanSpec) throws URISyntaxException {
        StringBuilder query = new StringBuilder(scanSpec.getQuery());
        ImmutableMap.Builder<String, String> replacementBuilder = ImmutableMap.builder();
        for (Map.Entry<String, Object> entry : scanSpec.getParameters().entrySet()) {
            replacementBuilder.put("${" + entry.getKey() + "}", Objects.toString(entry.getValue()));
        }


        Map<String, String> replacement = replacementBuilder.build();
        for (Map.Entry<String, String> entry : replacement.entrySet()) {
            int index;
            while ((index = query.indexOf(entry.getKey())) >= 0){
                query = query.replace(index, index + entry.getKey().length(), entry.getValue());
            }
        }

        URI scanUri = new URI(query.toString());
        if (!scanUri.isAbsolute()) {
            scanUri = new URI(config.getUrl()).resolve(scanUri);
        }

        return scanUri;
    }
}
