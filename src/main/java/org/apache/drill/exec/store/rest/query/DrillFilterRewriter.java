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

import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.drill.exec.planner.physical.ScanPrel;
import org.apache.drill.exec.store.rest.RestGroupScan;

import java.util.Collection;
import java.util.TreeSet;

/**
 * @author Oleg Zinoviev
 * @since 12.10.2017
 **/
public class DrillFilterRewriter extends RexShuttle {
    private final ScanPrel scan;
    private final RestGroupScan groupScan;
    private final TreeSet<String> pushedDownFilters;

    DrillFilterRewriter(ScanPrel scan, Collection<String> pushedDownFilters) {
        this.scan = scan;
        this.groupScan = (RestGroupScan) scan.getGroupScan();
        this.pushedDownFilters = new TreeSet<>(String::compareToIgnoreCase);
        this.pushedDownFilters.addAll(pushedDownFilters);
    }

    @Override
    public RexNode visitCall(RexCall call) {
        if (call.getOperator().getKind() == SqlKind.EQUALS) {

            for (RexNode node : call.getOperands()) {
                if (node instanceof RexInputRef) {
                    final int index = ((RexInputRef) node).getIndex();
                    final RelDataTypeField field = scan.getRowType().getFieldList().get(index);
                    if (field == null) {
                        return super.visitCall(call);
                    }

                    if (groupScan.getStoragePlugin().getRequestParameters().equalsIgnoreCase(field.getName())) {
                        RexBuilder builder = new RexBuilder(new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT));
                        RexLiteral left = builder.makeLiteral("1");
                        RexLiteral right = builder.makeLiteral("1");
                        return builder.makeCall(call.getOperator(), left, right);
                    }

                    if (pushedDownFilters.contains(field.getName())) {
                        RexBuilder builder = new RexBuilder(new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT));
                        RexLiteral left = builder.makeLiteral("1");
                        RexLiteral right = builder.makeLiteral("1");
                        return builder.makeCall(call.getOperator(), left, right);
                    }
                }
            }

        }

        return super.visitCall(call);
    }
}
