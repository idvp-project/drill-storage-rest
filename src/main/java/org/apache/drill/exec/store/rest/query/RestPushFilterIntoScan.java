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
package org.apache.drill.exec.store.rest.query;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexNode;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.exec.planner.logical.DrillOptiq;
import org.apache.drill.exec.planner.logical.DrillParseContext;
import org.apache.drill.exec.planner.logical.RelOptHelper;
import org.apache.drill.exec.planner.physical.FilterPrel;
import org.apache.drill.exec.planner.physical.PrelUtil;
import org.apache.drill.exec.planner.physical.ScanPrel;
import org.apache.drill.exec.store.StoragePluginOptimizerRule;
import org.apache.drill.exec.store.rest.RestGroupScan;
import org.apache.drill.exec.store.rest.RestScanSpec;

import java.util.Collection;
import java.util.Map;

public class RestPushFilterIntoScan extends StoragePluginOptimizerRule {

    public static final StoragePluginOptimizerRule FILTER_ON_SCAN = new RestPushFilterIntoScan(RelOptHelper.some(FilterPrel.class,
            RelOptHelper.any(ScanPrel.class)), "RestPushFilterIntoScan:Filter_On_Scan");

    private RestPushFilterIntoScan(RelOptRuleOperand operand, String description) {
        super(operand, description);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final ScanPrel scan = call.rel(1);
        final FilterPrel filter = call.rel(0);

        RestGroupScan groupScan = (RestGroupScan) scan.getGroupScan();
        if (groupScan.isPushedDown()) {
            return; //Фильтры уже проброшены в запрос
        }

        // convert the filter to one that references the child of the project
        final RexNode condition =  filter.getCondition();


        final LogicalExpression conditionExp = DrillOptiq.toDrill(new DrillParseContext(PrelUtil.getPlannerSettings(call.getPlanner())), scan, condition);
        final RestFilterBuilder filterBuilder = new RestFilterBuilder(groupScan, conditionExp);
        RestScanSpec newScanSpec = filterBuilder.parseTree();
        if (newScanSpec == null) {
            return;
        }

        postProcessScanSpec(newScanSpec);

        final RestGroupScan newGroupsScan = new RestGroupScan(groupScan.getUserName(),
                groupScan.getStoragePlugin(),
                newScanSpec,
                groupScan.getColumns(),
                true);

        final RelNode childRel = ScanPrel.create(scan, filter.getTraitSet(), newGroupsScan, scan.getRowType());
        call.transformTo(filter.copy(filter.getTraitSet(), childRel, rewriteCondition(scan, filterBuilder.getPushedDownFilters(), condition)));
    }

    private void postProcessScanSpec(RestScanSpec newScanSpec) {
        for (Map.Entry<String, ParameterValue> entry : newScanSpec.getParameters().entrySet()) {
            if (entry.getValue() != null && entry.getValue().getType() == ParameterValue.Type.PUSH_DOWN) {
                if (entry.getValue().getDefaultValue() != null) {
                    entry.setValue(entry.getValue().getDefaultValue());
                } else {
                    entry.setValue(new ParameterValue(ParameterValue.Type.VALUE, null, null, null));
                }
            }
        }
    }

    private RexNode rewriteCondition(ScanPrel scan, Collection<String> pushedDownFilters, RexNode condition) {
        DrillFilterRewriter shuttle = new DrillFilterRewriter(scan, pushedDownFilters);
        return shuttle.apply(condition);
    }


    @Override
    public boolean matches(RelOptRuleCall call) {
        final ScanPrel scan = call.rel(1);
        return scan.getGroupScan() instanceof RestGroupScan && super.matches(call);
    }


}