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

import java.io.IOException;
import java.util.List;

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
    private final boolean pushedDown;

    @JsonCreator
    private RestGroupScan(@JsonProperty("userName") String userName,
                         @JsonProperty("spec") RestScanSpec restScanSpec,
                         @JsonProperty("storagePluginConfig") RestStoragePluginConfig storagePluginConfig,
                         @JsonProperty("columns") List<SchemaPath> columns,
                         @JsonProperty("pushedDown") boolean pushedDown,
                         @JacksonInject StoragePluginRegistry pluginRegistry) throws IOException, ExecutionSetupException {
        this (userName, (RestStoragePlugin) pluginRegistry.getPlugin(storagePluginConfig), restScanSpec, columns, pushedDown);
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

    @JsonProperty
    public boolean isPushedDown() {
        return pushedDown;
    }

    @JsonIgnore
    public RestStoragePlugin getStoragePlugin() {
        return storagePlugin;
    }

    public RestGroupScan(String userName,
                         RestStoragePlugin plugin,
                         RestScanSpec spec,
                         List<SchemaPath> columns,
                         boolean pushedDown) {
        super(userName);
        this.storagePlugin = plugin;
        this.storagePluginConfig = storagePlugin.getConfig();
        this.spec = spec;
        this.columns = columns == null ? ALL_COLUMNS : columns;
        this.pushedDown = pushedDown;
    }

    public RestGroupScan(RestGroupScan that) {
        super(that);
        this.storagePlugin = that.storagePlugin;
        this.storagePluginConfig = that.storagePluginConfig;
        this.spec = that.spec;
        this.columns = that.columns;
        this.pushedDown = that.pushedDown;
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
        if (pushedDown) {
            return ScanStats.TRIVIAL_TABLE;
        }
        return HUGE_TABLE;
    }

}
