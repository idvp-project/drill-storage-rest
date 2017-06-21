package org.apache.drill.exec.store.rest;

import com.fasterxml.jackson.annotation.*;
import com.google.common.base.Preconditions;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.PhysicalOperatorSetupException;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.ScanStats;
import org.apache.drill.exec.physical.base.SubScan;
import org.apache.drill.exec.proto.CoordinationProtos;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.rest.config.RuntimeQueryConfig;
import org.apache.drill.exec.store.rest.helpers.VelocityHelper;

import java.io.IOException;
import java.util.List;
import java.util.Set;

/**
 * @author Oleg Zinoviev
 * @since 16.06.2017.
 */
@JsonTypeName("rest-scan")
@SuppressWarnings("WeakerAccess")
public class RestGroupScan extends AbstractGroupScan {

    private static final ScanStats HUGE_TABLE = new ScanStats(ScanStats.GroupScanProperty.NO_EXACT_ROW_COUNT, 1000L, 10.0f, 10.0f);

    private final RestScanSpec spec;
    private List<SchemaPath> columns;
    private final RestStoragePlugin storagePlugin;
    private final RestStoragePluginConfig storagePluginConfig;
    private Set<String> queryParameters;
    private FilterPushDown filterPushedDown = FilterPushDown.NONE;

    @JsonCreator
    private RestGroupScan(@JsonProperty("userName") String userName,
                         @JsonProperty("spec") RestScanSpec restScanSpec,
                         @JsonProperty("storagePluginConfig") RestStoragePluginConfig storagePluginConfig,
                         @JsonProperty("columns") List<SchemaPath> columns,
                         @JsonProperty("filterPushedDown") FilterPushDown filterPushedDown,
                         @JacksonInject StoragePluginRegistry pluginRegistry) throws IOException, ExecutionSetupException {
        this (userName, (RestStoragePlugin) pluginRegistry.getPlugin(storagePluginConfig), restScanSpec, columns);
        this.filterPushedDown = filterPushedDown;
    }

    @JsonProperty
    public RestScanSpec getSpec() {
        return spec;
    }

    @JsonProperty
    public List<SchemaPath> getColumns() {
        return columns;
    }

    @JsonProperty
    public RestStoragePluginConfig getStoragePluginConfig() {
        return storagePluginConfig;
    }

    @JsonIgnore
    public Set<String> getQueryParameters() {
        if (queryParameters == null) {
            queryParameters = getQueryParameters(spec);
        }
        return queryParameters;
    }

    @JsonIgnore
    public RestStoragePlugin getStoragePlugin() {
        return storagePlugin;
    }

    @JsonProperty
    public FilterPushDown getFilterPushedDown() {
        if (filterPushedDown == null) {
            filterPushedDown = FilterPushDown.NONE;
        }
        return filterPushedDown;
    }

    public void setFilterPushedDown(FilterPushDown filterPushedDown) {
        this.filterPushedDown = filterPushedDown;
    }

    public RestGroupScan(String userName, RestStoragePlugin plugin, RestScanSpec spec, List<SchemaPath> columns) {
        super(userName);
        this.storagePlugin = plugin;
        this.storagePluginConfig = (RestStoragePluginConfig) storagePlugin.getConfig();
        this.spec = spec;
        this.columns = columns == null ? ALL_COLUMNS : columns;
        this.queryParameters = getQueryParameters(spec);
    }

    private Set<String> getQueryParameters(RestScanSpec spec) {
        String query = Preconditions.checkNotNull(spec.getQuery());
        RuntimeQueryConfig config = storagePluginConfig.getRuntimeConfig(query);
        return VelocityHelper.INSTANCE.parameters(config);
    }

    public RestGroupScan(RestGroupScan that) {
        super(that);
        this.storagePlugin = that.storagePlugin;
        this.storagePluginConfig = that.storagePluginConfig;
        this.spec = that.spec;
        this.columns = that.columns;
        this.queryParameters = that.queryParameters;
        this.filterPushedDown = that.filterPushedDown;
    }

    @Override
    public RestGroupScan clone(List<SchemaPath> columns) {
        RestGroupScan scan = new RestGroupScan(this);
        scan.columns = columns == null ? ALL_COLUMNS : columns;
        return scan;
    }

    @Override
    public void applyAssignments(List<CoordinationProtos.DrillbitEndpoint> list) throws PhysicalOperatorSetupException {
        //Ассаймент к нодам дрила не поддерживается
    }

    @Override
    public SubScan getSpecificScan(int i) throws ExecutionSetupException {
        return new RestSubScan(getUserName(), spec, storagePlugin);
    }

    @Override
    public int getMaxParallelizationWidth() {
        return 1;
    }

    @Override
    public String getDigest() {
        return toString();
    }

    @Override
    public String toString() {
        return "RestGroupScan [RestScanSpec="
                + spec
                + ", columns="
                + columns + "]";
    }

    @Override
    public PhysicalOperator getNewWithChildren(List<PhysicalOperator> list) throws ExecutionSetupException {
        Preconditions.checkArgument(list.isEmpty());
        return new RestGroupScan(this);
    }

    @Override
    public boolean supportsPartitionFilterPushdown() {
        return true;
    }

    @Override
    public ScanStats getScanStats() {
        if (getFilterPushedDown() != FilterPushDown.NONE) {
            return ScanStats.TRIVIAL_TABLE;
        }
        return HUGE_TABLE;
    }

}
