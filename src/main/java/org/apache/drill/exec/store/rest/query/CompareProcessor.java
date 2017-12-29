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

import com.google.common.collect.ImmutableSet;
import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.expression.ValueExpressions;
import org.apache.drill.common.expression.visitors.AbstractExprVisitor;
import org.apache.drill.exec.store.rest.RestGroupScan;

import java.util.Set;

/**
 * @author Oleg Zinoviev
 * @since 19.06.2017.
 */
class CompareProcessor extends AbstractExprVisitor<Boolean, LogicalExpression, RuntimeException> {
    private final static Set<String> COMPARE_FUNCTIONS = ImmutableSet.<String>builder()
            .add("equal")
            .build();

    private static final ImmutableSet<Class<? extends LogicalExpression>> VALUE_EXPRESSION_CLASSES = ImmutableSet.<Class<? extends LogicalExpression>>builder()
            .add(ValueExpressions.QuotedString.class)
            .build();

    static boolean isCompareFunction(String function) {
        return COMPARE_FUNCTIONS.contains(function);
    }

    static CompareProcessor process(final FunctionCall call, RestGroupScan scan) {
        LogicalExpression nameArg = call.args.get(0);
        LogicalExpression valueArg = call.args.size() >= 2 ? call.args.get(1) : null;
        CompareProcessor evaluator = new CompareProcessor(scan);

        if (valueArg != null) { // binary function
            if (VALUE_EXPRESSION_CLASSES.contains(nameArg.getClass())) {
                LogicalExpression swapArg = valueArg;
                valueArg = nameArg;
                nameArg = swapArg;
            }
            evaluator.success = nameArg.accept(evaluator, valueArg);
        } else if (call.args.get(0) instanceof SchemaPath) {
            evaluator.success = true;
        }

        return evaluator;
    }

    private final RestGroupScan scan;
    private boolean success = false;
    private String value;

    private CompareProcessor(RestGroupScan scan) {
        this.scan = scan;
    }

    boolean isSuccess() {
        return success;
    }

    String getValue() {
        return value;
    }

    @Override
    public Boolean visitQuotedStringConstant(ValueExpressions.QuotedString e, LogicalExpression value) throws RuntimeException {
        this.value = e.getString();
        return true;
    }

    @Override
    public Boolean visitSchemaPath(SchemaPath path, LogicalExpression valueArg) throws RuntimeException {
        if (!VALUE_EXPRESSION_CLASSES.contains(valueArg.getClass())) {
            return false;
        }

        boolean success = scan.getStoragePlugin().getRequestParameters().equalsIgnoreCase(path.getRootSegmentPath());
        return success && valueArg.accept(this, valueArg);
    }

}
