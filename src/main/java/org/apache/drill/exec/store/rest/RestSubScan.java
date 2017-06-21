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
import org.apache.drill.common.exceptions.ExecutionSetupException;
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

    @JsonIgnore
    RestStoragePlugin getStoragePlugin() {
        return storagePlugin;
    }

    @Override
    public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) throws ExecutionSetupException {
        return super.getNewWithChildren(children);
    }

}
