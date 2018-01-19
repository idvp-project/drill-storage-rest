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
import org.apache.drill.common.expression.*;
import org.apache.drill.common.expression.visitors.AbstractExprVisitor;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.*;

/**
 * @author Oleg Zinoviev
 * @since 19.06.2017.
 */
class PushDownFirstProcessor extends AbstractExprVisitor<Boolean, LogicalExpression, RuntimeException> {
    private final static Set<String> COMPARE_FUNCTIONS = ImmutableSet.<String>builder()
            .add("equal")
            .build();

    private static final ImmutableSet<Class<? extends LogicalExpression>> VALUE_EXPRESSION_CLASSES = ImmutableSet.<Class<? extends LogicalExpression>>builder()
            .add(ValueExpressions.BooleanExpression.class)
            .add(ValueExpressions.QuotedString.class)
            .add(ValueExpressions.DateExpression.class)
            .add(ValueExpressions.Decimal9Expression.class)
            .add(ValueExpressions.Decimal18Expression.class)
            .add(ValueExpressions.DoubleExpression.class)
            .add(ValueExpressions.FloatExpression.class)
            .add(ValueExpressions.IntExpression.class)
            .add(ValueExpressions.LongExpression.class)
            .add(ValueExpressions.TimeExpression.class)
            .add(ValueExpressions.TimeStampExpression.class)
            .add(NullExpression.class)
            .build();

    private final Map<String, String> filterNames;

    static boolean isCompareFunction(String function) {
        return COMPARE_FUNCTIONS.contains(function);
    }

    private PushDownFirstProcessor(Map<String, String> filterToParameterMap) {
        this.filterNames = new TreeMap<>(String::compareToIgnoreCase);
        this.filterNames.putAll(filterToParameterMap);
    }

    static PushDownFirstProcessor process(final FunctionCall call, Map<String, String> filterToParameterMap) {
        LogicalExpression nameArg = call.args.get(0);
        LogicalExpression valueArg = call.args.size() >= 2 ? call.args.get(1) : null;
        PushDownFirstProcessor evaluator = new PushDownFirstProcessor(filterToParameterMap);

        if (valueArg != null) { // binary function
            if (VALUE_EXPRESSION_CLASSES.contains(nameArg.getClass())) {
                LogicalExpression swapArg = valueArg;
                valueArg = nameArg;
                nameArg = swapArg;
            }
            evaluator.success = nameArg.accept(evaluator, valueArg);
        }

        return evaluator;
    }

    private boolean success = false;
    private String parameter;
    private String filter;
    private Object value;

    boolean isSuccess() {
        return success;
    }

    String getParameter() {
        return parameter;
    }

    String getFilter() {
        return filter;
    }

    Object getValue() {
        return value;
    }

    @Override
    public Boolean visitBooleanConstant(ValueExpressions.BooleanExpression e, LogicalExpression value) throws RuntimeException {
        this.value = e.getBoolean();
        return true;
    }

    @Override
    public Boolean visitQuotedStringConstant(ValueExpressions.QuotedString e, LogicalExpression value) throws RuntimeException {
        this.value = e.getString();
        return true;
    }

    @Override
    public Boolean visitDateConstant(ValueExpressions.DateExpression e, LogicalExpression value) throws RuntimeException {
        this.value = new Date(e.getDate());
        return true;
    }

    @Override
    public Boolean visitDecimal9Constant(ValueExpressions.Decimal9Expression e, LogicalExpression value) throws RuntimeException {
        this.value = BigDecimal.valueOf(e.getIntFromDecimal(), e.getScale()).doubleValue();
        return true;
    }

    @Override
    public Boolean visitDecimal18Constant(ValueExpressions.Decimal18Expression e, LogicalExpression value) throws RuntimeException {
        this.value = BigDecimal.valueOf(e.getLongFromDecimal(), e.getScale()).doubleValue();
        return true;
    }

    @Override
    public Boolean visitDoubleConstant(ValueExpressions.DoubleExpression e, LogicalExpression value) throws RuntimeException {
        this.value = e.getDouble();
        return true;
    }

    @Override
    public Boolean visitFloatConstant(ValueExpressions.FloatExpression e, LogicalExpression value) throws RuntimeException {
        this.value = e.getFloat();
        return true;
    }

    @Override
    public Boolean visitIntConstant(ValueExpressions.IntExpression e, LogicalExpression value) throws RuntimeException {
        this.value = e.getInt();
        return true;
    }

    @Override
    public Boolean visitLongConstant(ValueExpressions.LongExpression e, LogicalExpression value) throws RuntimeException {
        this.value = e.getLong();
        return true;
    }

    @Override
    public Boolean visitTimeConstant(ValueExpressions.TimeExpression e, LogicalExpression value) throws RuntimeException {
        this.value = e.getTime();
        return true;
    }

    @Override
    public Boolean visitTimeStampConstant(ValueExpressions.TimeStampExpression e, LogicalExpression value) throws RuntimeException {
        this.value = new Timestamp(e.getTimeStamp());
        return true;
    }

    @Override
    public Boolean visitNullExpression(NullExpression e, LogicalExpression value) throws RuntimeException {
        this.value = null;
        return true;
    }



    @Override
    public Boolean visitSchemaPath(SchemaPath path, LogicalExpression valueArg) throws RuntimeException {
        if (!VALUE_EXPRESSION_CLASSES.contains(valueArg.getClass())) {
            return false;
        }

        boolean success = filterNames.keySet().contains(path.getRootSegmentPath());

        if (success && valueArg.accept(this, valueArg)) {
            filter = path.getRootSegmentPath();
            parameter = filterNames.get(path.getRootSegmentPath());
            return true;
        }

        return false;
    }


}
