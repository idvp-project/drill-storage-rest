package org.apache.drill.exec.store.rest;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.drill.common.JSONOptions;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.ops.OptimizerRulesContext;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.AbstractStoragePlugin;
import org.apache.drill.exec.store.SchemaConfig;
import org.apache.drill.exec.store.rest.calcite.RestPushFilterIntoScan;

import java.io.IOException;
import java.util.List;
import java.util.Set;

/**
 * @author Oleg Zinoviev
 * @since 15.06.2017.
 */
public class RestStoragePlugin extends AbstractStoragePlugin {

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
}
