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

import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.drill.exec.planner.physical.ProjectPrel;

import java.util.List;

/**
 * @author Oleg Zinoviev
 * @since 12.10.2017
 **/
public class DrillProjectFilter extends RexShuttle {
    private final ProjectPrel project;
    private final RexBuilder builder;
    private final RexLiteral dummyNode;

    DrillProjectFilter(ProjectPrel project) {
        this.project = project;
        this.builder = new RexBuilder(new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT));
        this.dummyNode = builder.makeLiteral("dummy");
    }

    @Override
    public RexNode visitCall(RexCall call) {

        boolean[] update = {false};
        List<RexNode> clonedOperands = visitList(call.operands, update);
        if (update[0]) {
            boolean shouldSkipCall = false;
            for (RexNode operand : clonedOperands) {
                if (operand == dummyNode) {
                    shouldSkipCall = true;
                    break;
                }
            }

            if (shouldSkipCall) {
                RexLiteral left = builder.makeLiteral("1");
                RexLiteral right = builder.makeLiteral("1");
                return builder.makeCall(SqlStdOperatorTable.EQUALS, left, right);
            } else {

                return builder.makeCall(
                        call.getType(),
                        call.getOperator(),
                        clonedOperands);
            }
        } else {
            return call;
        }
    }

    @Override
    public RexNode visitInputRef(RexInputRef ref) {
        RexNode rexNode = project.getProjects().get(ref.getIndex());
        if (rexNode instanceof RexInputRef) {
            return rexNode;
        }

        return dummyNode;
    }
}
