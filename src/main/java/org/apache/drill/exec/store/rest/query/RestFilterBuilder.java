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

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.StringUtils;
import org.apache.drill.common.expression.BooleanOperator;
import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.visitors.AbstractExprVisitor;
import org.apache.drill.exec.store.rest.RestGroupScan;
import org.apache.drill.exec.store.rest.RestScanSpec;
import org.apache.drill.exec.store.rest.read.RestRecordReader;

import java.io.IOException;
import java.util.*;

/**
 * @author Oleg Zinoviev
 * @since 16.06.2017.
 */
public class RestFilterBuilder extends AbstractExprVisitor<RestScanSpec, Void, RuntimeException> {

    private static final String BOOLEAN_AND = "booleanAnd";
    private static final String BOOLEAN_OR = "booleanOr";

    private final RestGroupScan scan;
    private final LogicalExpression expression;

    private RestScanSpec nodeScanSpec = null;
    private final List<String> pushedDownFilters = new ArrayList<>();

    RestFilterBuilder(RestGroupScan scan, LogicalExpression expression) {
        this.scan = scan;
        this.expression = expression;
    }

    RestScanSpec parseTree() {
        return expression.accept(this, null);
    }

    List<String> getPushedDownFilters() {
        return Collections.unmodifiableList(pushedDownFilters);
    }

    @Override
    public RestScanSpec visitFunctionCall(FunctionCall call, Void value) throws RuntimeException {
        String functionName = call.getName();
        ImmutableList<LogicalExpression> args = call.args;

        if (nodeScanSpec == null && TableParamProcessor.isCompareFunction(functionName)) {
            TableParamProcessor processor = TableParamProcessor.process(call, scan);
            if (processor.isSuccess()) {
                nodeScanSpec = createScanSpec(processor);
            }
        } else if (nodeScanSpec != null && PushDownFirstProcessor.isCompareFunction(functionName)) {

            Map<String, String> filterToParameterMap = new HashMap<>();
            for (Map.Entry<String, ParameterValue> entry : nodeScanSpec.getParameters().entrySet()) {
                if (entry.getValue() != null & entry.getValue().getType() == ParameterValue.Type.PUSH_DOWN) {
                    filterToParameterMap.put(entry.getValue().getFilterCode(), entry.getKey());
                }
            }

            PushDownFirstProcessor processor = PushDownFirstProcessor.process(call, filterToParameterMap);
            if (processor.isSuccess()) {
                nodeScanSpec = createScanSpec(processor);
            }
        } else {
            switch (functionName) {
                case BOOLEAN_AND:
                case BOOLEAN_OR:
                    for (int i = 0; i < args.size(); ++i) {
                        nodeScanSpec = args.get(i).accept(this, null);
                        if (nodeScanSpec != null) {
                            break;
                        }
                    }
                    break;
            }
        }

        return nodeScanSpec;
    }

    private RestScanSpec createScanSpec(PushDownFirstProcessor processor) {
        if (nodeScanSpec == null) {
            throw new IllegalStateException("nodeScanSpec is null");
        }

        String filter = processor.getFilter();
        String parameter = processor.getParameter();
        Object value = processor.getValue();

        nodeScanSpec.getParameters().put(parameter, new ParameterValue(ParameterValue.Type.VALUE, value, null, null));
        pushedDownFilters.add(filter);
        return nodeScanSpec;
    }

    private RestScanSpec createScanSpec(TableParamProcessor processor) {
        String value = processor.getValue();

        Map<String, ParameterValue> parameterValues = Collections.emptyMap();
        if (StringUtils.isNotBlank(value)) {
            try {
                parameterValues = RestRecordReader.MAPPER.readValue(value, new TypeReference<Map<String, ParameterValue>>() {});
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        return new RestScanSpec(scan.getSpec().getQuery(), parameterValues);
    }

    @Override
    public RestScanSpec visitBooleanOperator(BooleanOperator op, Void value) throws RuntimeException {
        return visitFunctionCall(op, value);
    }

    @Override
    public RestScanSpec visitUnknown(LogicalExpression e, Void value) throws RuntimeException {
        return null;
    }

}
