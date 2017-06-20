package org.apache.drill.exec.store.rest.query;

import com.google.common.collect.ImmutableList;
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
import org.apache.drill.exec.store.rest.FilterPushDown;
import org.apache.drill.exec.store.rest.RestGroupScan;
import org.apache.drill.exec.store.rest.RestScanSpec;

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

        RestGroupScan groupScan = (RestGroupScan) scan.getGroupScan();
        if (groupScan.getFilterPushedDown() != FilterPushDown.NONE) {
            return; //already pushed down
        }

        // convert the filter to one that references the child of the project
        final RexNode condition =  filter.getCondition();

        final LogicalExpression conditionExp = DrillOptiq.toDrill(new DrillParseContext(PrelUtil.getPlannerSettings(call.getPlanner())), scan, condition);
        final RestFilterBuilder filterBuilder = new RestFilterBuilder(groupScan, conditionExp);
        RestScanSpec newScanSpec = filterBuilder.parseTree();
        if (newScanSpec == null) {
            return;
        }

        if (filterBuilder.isAllNodesConverted()) {
            newScanSpec = new RestScanSpec(newScanSpec.getQuery(), newScanSpec.getParameters(), FilterPushDown.ALL);
        }

        final RestGroupScan newGroupsScan = new RestGroupScan(groupScan.getUserName(), groupScan.getStoragePlugin(),
                newScanSpec, groupScan.getColumns());
        newGroupsScan.setFilterPushedDown(filterBuilder.isAllNodesConverted() ? FilterPushDown.ALL : FilterPushDown.SOME);

        final RelNode childRel = ScanPrel.create(scan, filter.getTraitSet(), newGroupsScan, scan.getRowType());
        if (filterBuilder.isAllNodesConverted()) {
            call.transformTo(childRel);
        } else {
            call.transformTo(filter.copy(filter.getTraitSet(), ImmutableList.of(childRel)));
        }


    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        final ScanPrel scan = call.rel(1);
        return scan.getGroupScan() instanceof RestGroupScan && super.matches(call);
    }


}
