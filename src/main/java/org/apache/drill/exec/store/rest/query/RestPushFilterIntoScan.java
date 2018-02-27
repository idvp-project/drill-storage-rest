/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.store.rest.query;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexNode;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.exec.planner.logical.DrillOptiq;
import org.apache.drill.exec.planner.logical.DrillParseContext;
import org.apache.drill.exec.planner.logical.RelOptHelper;
import org.apache.drill.exec.planner.physical.FilterPrel;
import org.apache.drill.exec.planner.physical.PrelUtil;
import org.apache.drill.exec.planner.physical.ProjectPrel;
import org.apache.drill.exec.planner.physical.ScanPrel;
import org.apache.drill.exec.store.StoragePluginOptimizerRule;
import org.apache.drill.exec.store.rest.RestGroupScan;
import org.apache.drill.exec.store.rest.RestScanSpec;

public abstract class RestPushFilterIntoScan extends StoragePluginOptimizerRule {

    public static final StoragePluginOptimizerRule FILTER_ON_SCAN = new RestPushFilterOnScan(
            RelOptHelper.some(FilterPrel.class, RelOptHelper.any(ScanPrel.class)),
            "RestPushFilterIntoScan:Filter_On_Scan");
    public static final StoragePluginOptimizerRule FILTER_ON_PROJECT = new RestPushFilterOnProject(
            RelOptHelper.some(FilterPrel.class, RelOptHelper.some(ProjectPrel.class, RelOptHelper.any(ScanPrel.class))),
            "RestPushFilterIntoScan:Filter_On_Project");

    private RestPushFilterIntoScan(RelOptRuleOperand operand,
                                   String description) {
        super(operand, description);
    }

    void doOnMatch(final RelOptRuleCall call,
                   final FilterPrel filter,
                   final ProjectPrel project,
                   final ScanPrel scan,
                   final RestGroupScan groupScan,
                   final RexNode condition) {
        final LogicalExpression conditionExp = DrillOptiq.toDrill(new DrillParseContext(PrelUtil.getPlannerSettings(call.getPlanner())), scan, condition);
        final RestFilterBuilder filterBuilder = new RestFilterBuilder(groupScan, conditionExp);
        RestScanSpec newScanSpec = filterBuilder.parseTree();
        if (newScanSpec == null) {
            return;
        }

        final RestGroupScan newGroupsScan =
                new RestGroupScan(groupScan.getUserName(), groupScan.getStoragePlugin(), newScanSpec, groupScan.getColumns(), true);

        final RelNode newScanPrel = ScanPrel.create(scan, filter.getTraitSet(), newGroupsScan, scan.getRowType());
        // Depending on whether is a project in the middle, assign either scan or copy of project to childRel.
        final RelNode childRel = project == null ? newScanPrel : project.copy(project.getTraitSet(), ImmutableList.of(newScanPrel));
        call.transformTo(filter.copy(filter.getTraitSet(), childRel, rewriteCondition(scan, condition)));
    }

    private RexNode rewriteCondition(ScanPrel scan, RexNode condition) {
        DrillFilterRewriter shuttle = new DrillFilterRewriter(scan);
        return shuttle.apply(condition);
    }


    private static class RestPushFilterOnScan extends RestPushFilterIntoScan {
        private RestPushFilterOnScan(RelOptRuleOperand operand, String description) {
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

            final RexNode condition = filter.getCondition();

            doOnMatch(call, filter, null, scan, groupScan, condition);
        }

        @Override
        public boolean matches(RelOptRuleCall call) {
            final ScanPrel scan = call.rel(1);
            return scan.getGroupScan() instanceof RestGroupScan && super.matches(call);
        }
    }

    private static class RestPushFilterOnProject extends RestPushFilterIntoScan {
        private RestPushFilterOnProject(RelOptRuleOperand operand, String description) {
            super(operand, description);
        }

        @Override
        public void onMatch(RelOptRuleCall call) {
            final ScanPrel scan = call.rel(2);
            final ProjectPrel project = call.rel(1);
            final FilterPrel filter = call.rel(0);

            RestGroupScan groupScan = (RestGroupScan) scan.getGroupScan();
            if (groupScan.isPushedDown()) {
                return; //Фильтры уже проброшены в запрос
            }

            // convert the filter to one that references the child of the project
            final RexNode condition =  RelOptUtil.pushPastProject(filter.getCondition(), project);

            doOnMatch(call, filter, project, scan, groupScan, condition);
        }

        @Override
        public boolean matches(RelOptRuleCall call) {
            final ScanPrel scan = call.rel(2);
            return scan.getGroupScan() instanceof RestGroupScan && super.matches(call);
        }

    }


}