package org.apache.drill.exec.store.rest;

import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.drill.exec.planner.logical.DynamicDrillTable;
import org.apache.drill.exec.store.AbstractSchema;
import org.apache.drill.exec.store.SchemaConfig;
import org.apache.drill.exec.store.SchemaFactory;
import org.apache.drill.exec.store.StoragePlugin;

import java.io.IOException;
import java.util.Collections;

/**
 * @author Oleg Zinoviev
 * @since 15.06.2017.
 */
public class RestSchemaFactory implements SchemaFactory {

    private final String schemaName;
    private final RestStoragePlugin plugin;

    RestSchemaFactory(String schemaName, RestStoragePlugin plugin) {
        this.schemaName = schemaName;
        this.plugin = plugin;
    }

    @Override
    public void registerSchemas(SchemaConfig schemaConfig, SchemaPlus parent) throws IOException {
        RestSchema schema = new RestSchema(schemaName);
        parent.add(schemaName, schema);
    }

    class RestSchema extends AbstractSchema {

        RestSchema(String name) {
            super(Collections.emptyList(), name);
        }

        @Override
        public String getTypeName() {
            return RestStoragePluginConfig.NAME;
        }

        @Override
        public Table getTable(String name) {
            return new RestTable(plugin, schemaName, new RestScanSpec(name, Collections.emptyMap()));
        }
    }

    static class RestTable extends DynamicDrillTable {

        RestTable(StoragePlugin plugin, String storageEngineName, RestScanSpec selection) {
            super(plugin, storageEngineName, selection);
        }
    }
}
