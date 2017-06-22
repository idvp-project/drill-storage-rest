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

import com.google.common.collect.ImmutableList;
import org.apache.drill.common.expression.*;
import org.apache.drill.common.expression.visitors.AbstractExprVisitor;
import org.apache.drill.exec.store.rest.FilterPushDown;
import org.apache.drill.exec.store.rest.RestGroupScan;
import org.apache.drill.exec.store.rest.RestScanSpec;

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
    private boolean allNodesConverted = true;

    RestFilterBuilder(RestGroupScan scan, LogicalExpression expression) {
        this.scan = scan;
        this.expression = expression;
    }

    RestScanSpec parseTree() {
        RestScanSpec parsedSpec = expression.accept(this, null);
        if (parsedSpec != null) {
            parsedSpec = mergeScanSpecs("booleanAnd", this.scan.getSpec(), parsedSpec);
        }
        return parsedSpec;
    }

    @Override
    public RestScanSpec visitFunctionCall(FunctionCall call, Void value) throws RuntimeException {
        RestScanSpec nodeScanSpec = null;
        String functionName = call.getName();
        ImmutableList<LogicalExpression> args = call.args;

        if (CompareProcessor.isCompareFunction(functionName)) {
            CompareProcessor processor = CompareProcessor.process(call, scan);
            if (processor.isSuccess()) {
                nodeScanSpec = createScanSpec(processor);
            }
        } else {
            switch (functionName) {
                case BOOLEAN_AND:
                case BOOLEAN_OR:
                    RestScanSpec firstScanSpec = args.get(0).accept(this, null);
                    for (int i = 1; i < args.size(); ++i) {
                        RestScanSpec nextScanSpec = args.get(i).accept(this, null);
                        if (firstScanSpec != null && nextScanSpec != null) {
                            nodeScanSpec = mergeScanSpecs(functionName, firstScanSpec, nextScanSpec);
                        } else {
                            allNodesConverted = false;
                            if (BOOLEAN_AND.equals(functionName)) {
                                nodeScanSpec = firstScanSpec == null ? nextScanSpec : firstScanSpec;
                            }
                        }
                        firstScanSpec = nodeScanSpec;
                    }
                    break;
            }
        }

        if (nodeScanSpec == null) {
            allNodesConverted = false;
        }

        return nodeScanSpec;
    }

    private RestScanSpec mergeScanSpecs(String functionName, RestScanSpec firstScanSpec, RestScanSpec nextScanSpec) {
        Map<String, Object> newFilter = new LinkedHashMap<>(firstScanSpec.getParameters());

        switch (functionName) {
            case BOOLEAN_AND:
                newFilter.putAll(nextScanSpec.getParameters());
                break;
            case BOOLEAN_OR:
                for (Map.Entry<String, Object> entry : nextScanSpec.getParameters().entrySet()) {
                    if (newFilter.containsKey(entry.getKey())) {
                        Object value = newFilter.get(entry.getKey());
                        if (value == null) {
                            newFilter.put(entry.getKey(), entry.getValue());
                        } else {
                            ImmutableList.Builder<Object> aggValue = ImmutableList.builder();
                            if (value instanceof Iterable<?>) {
                                aggValue.addAll((Iterable<?>) value);
                            } else if (value.getClass().isArray()) {
                                aggValue.addAll(Arrays.asList((Object[]) value));
                            } else {
                                aggValue.add(value);
                            }

                            if (entry.getValue() != null) {
                                if (entry.getValue() instanceof Iterable<?>) {
                                    aggValue.addAll((Iterable<?>) entry.getValue());
                                } else if (entry.getValue().getClass().isArray()) {
                                    aggValue.addAll(Arrays.asList((Object[]) entry.getValue()));
                                } else {
                                    aggValue.add(entry.getValue());
                                }
                            }

                            newFilter.put(entry.getKey(), aggValue.build());
                        }

                    } else {
                        newFilter.put(entry.getKey(), entry.getValue());
                    }
                }
                break;
        }
        return new RestScanSpec(scan.getSpec().getQuery(), newFilter, FilterPushDown.SOME);

    }

    private RestScanSpec createScanSpec(CompareProcessor processor) {
        SchemaPath field = processor.getPath();
        String name = field.getAsUnescapedPath();
        Object value = processor.getValue();
        return new RestScanSpec(scan.getSpec().getQuery(), Collections.singletonMap(name, value), FilterPushDown.SOME);
    }

    @Override
    public RestScanSpec visitBooleanOperator(BooleanOperator op, Void value) throws RuntimeException {
        return visitFunctionCall(op, value);
    }

    @Override
    public RestScanSpec visitUnknown(LogicalExpression e, Void value) throws RuntimeException {
        this.allNodesConverted = false;
        return null;
    }

    boolean isAllNodesConverted() {
        return allNodesConverted;
    }
}
