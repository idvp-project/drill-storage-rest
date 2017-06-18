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

package org.apache.drill.exec.store.rest.calcite;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rex.RexNode;
import org.apache.commons.collections.SetUtils;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.exec.planner.logical.DrillOptiq;
import org.apache.drill.exec.planner.logical.DrillParseContext;
import org.apache.drill.exec.planner.logical.RelOptHelper;
import org.apache.drill.exec.planner.physical.FilterPrel;
import org.apache.drill.exec.planner.physical.PrelUtil;
import org.apache.drill.exec.planner.physical.ScanPrel;
import org.apache.drill.exec.store.StoragePluginOptimizerRule;
import org.apache.drill.exec.store.rest.RestGroupScan;

import java.util.HashSet;
import java.util.Set;

public class RestPushFilterIntoScan extends StoragePluginOptimizerRule {

    private RestPushFilterIntoScan(RelOptRuleOperand operand, String description) {
        super(operand, description);
    }

    public static final StoragePluginOptimizerRule FILTER_ON_SCAN = new RestPushFilterIntoScan(RelOptHelper.some(FilterPrel.class,
            RelOptHelper.any(ScanPrel.class)), "RestPushFilterIntoScan:Filter_On_Scan");

    @Override
    public void onMatch(RelOptRuleCall call) {
        final ScanPrel scan = call.rel(1);
        final FilterPrel filter = call.rel(0);
        final RexNode condition = filter.getCondition();

        RestGroupScan groupScan = (RestGroupScan) scan.getGroupScan();
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        final ScanPrel scan = call.rel(1);
        if (scan.getGroupScan() instanceof RestGroupScan) {
            if (super.matches(call)) {
                final FilterPrel filter = call.rel(0);
                final RexNode condition = filter.getCondition();

                LogicalExpression expression = toLogicalExpression(call, scan, condition);
                Set<String> parameterNames = getPossibleParameterNames(expression);
                RestGroupScan restGroupScan = (RestGroupScan) scan.getGroupScan();
                Set<String> intersection = new HashSet<>(restGroupScan.getQueryParameters());
                intersection.retainAll(parameterNames);
                return !intersection.isEmpty();
            }
        }
        return false;
    }



    private LogicalExpression toLogicalExpression(final RelOptRuleCall call, final ScanPrel scan, final RexNode condition) {
        return DrillOptiq.toDrill(new DrillParseContext(PrelUtil.getPlannerSettings(call.getPlanner())), scan, condition);
    }

    private Set<String> getPossibleParameterNames(LogicalExpression expression) {
        RestFilterNameParser parser = new RestFilterNameParser();
        return expression.accept(parser, null);
    }


}
