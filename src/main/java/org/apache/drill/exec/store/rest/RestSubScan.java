package org.apache.drill.exec.store.rest;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.physical.base.AbstractSubScan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.store.StoragePluginRegistry;

import java.io.IOException;
import java.util.List;

/**
 * @author Oleg Zinoviev
 * @since 16.06.2017.
 */
@JsonTypeName("rest-sub-scan")
public class RestSubScan extends AbstractSubScan {

    private final RestScanSpec spec;
    private final RestStoragePlugin storagePlugin;
    private final StoragePluginConfig storagePluginConfig;

    @JsonCreator
    private RestSubScan(@JsonProperty("userName") String userName,
                        @JsonProperty("spec") RestScanSpec restScanSpec,
                        @JsonProperty("storagePluginConfig") RestStoragePluginConfig storagePluginConfig,
                        @JacksonInject StoragePluginRegistry pluginRegistry) throws IOException, ExecutionSetupException {
        this (userName, restScanSpec, (RestStoragePlugin) pluginRegistry.getPlugin(storagePluginConfig));
    }

    RestSubScan(String userName, RestScanSpec spec, RestStoragePlugin storagePlugin) {
        super(userName);
        this.spec = spec;
        this.storagePlugin = storagePlugin;
        this.storagePluginConfig = storagePlugin.getConfig();
        initialAllocation = 100;
    }

    @JsonProperty
    public RestScanSpec getSpec() {
        return spec;
    }

    @JsonProperty
    public StoragePluginConfig getStoragePluginConfig() {
        return storagePluginConfig;
    }

    public int getOperatorType() {
        return -1;
    }

    @Override
    public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) throws ExecutionSetupException {
        return super.getNewWithChildren(children);
    }

}
