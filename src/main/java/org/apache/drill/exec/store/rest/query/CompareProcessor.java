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
import org.apache.drill.exec.store.rest.RestGroupScan;
import org.joda.time.DateTime;
import org.joda.time.Duration;

import java.math.BigDecimal;
import java.math.MathContext;
import java.util.Objects;
import java.util.Set;

/**
 * @author Oleg Zinoviev
 * @since 19.06.2017.
 */
class CompareProcessor extends AbstractExprVisitor<Boolean, LogicalExpression, RuntimeException> {
    private final static Set<String> COMPARE_FUNCTIONS = ImmutableSet.<String>builder()
            .add("equal")
            .add("isnull")
            .add("isNull")
            .add("is null")
            .build();

    private static final ImmutableSet<Class<? extends LogicalExpression>> VALUE_EXPRESSION_CLASSES = ImmutableSet.<Class<? extends LogicalExpression>>builder()
            .add(ValueExpressions.Decimal9Expression.class)
            .add(ValueExpressions.Decimal18Expression.class)
            .add(ValueExpressions.Decimal28Expression.class)
            .add(ValueExpressions.Decimal38Expression.class)
            .add(ValueExpressions.BooleanExpression.class)
            .add(ValueExpressions.DateExpression.class)
            .add(ValueExpressions.DoubleExpression.class)
            .add(ValueExpressions.FloatExpression.class)
            .add(ValueExpressions.IntExpression.class)
            .add(ValueExpressions.LongExpression.class)
            .add(ValueExpressions.QuotedString.class)
            .add(ValueExpressions.TimeExpression.class)
            .add(ValueExpressions.IntervalDayExpression.class)
            .add(ValueExpressions.IntervalYearExpression.class)
            .add(NullExpression.class)
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
            evaluator.path = (SchemaPath) nameArg;
        }

        return evaluator;
    }

    private final RestGroupScan scan;
    private boolean success = false;
    private SchemaPath path;
    private Object value;

    private CompareProcessor(RestGroupScan scan) {
        this.scan = scan;
    }

    boolean isSuccess() {
        return success;
    }

    SchemaPath getPath() {
        return path;
    }

    Object getValue() {
        return value;
    }

    @Override
    public Boolean visitCastExpression(CastExpression e, LogicalExpression valueArg) throws RuntimeException {
        if (e.getInput() instanceof CastExpression || e.getInput() instanceof SchemaPath) {
            return e.getInput().accept(this, valueArg);
        }
        return false;
    }

    @Override
    public Boolean visitConvertExpression(ConvertExpression e, LogicalExpression valueArg) throws RuntimeException {
        if (Objects.equals(e.getConvertFunction(), ConvertExpression.CONVERT_FROM)) {

            String encodingType = e.getEncodingType();
            Object value = null;

            if (e.getInput() instanceof SchemaPath) {
                switch (encodingType) {
                    case "INT_BE":
                    case "INT":
                    case "UINT_BE":
                    case "UINT":
                    case "UINT4_BE":
                    case "UINT4":
                        if (valueArg instanceof ValueExpressions.IntExpression) {
                            value = ((ValueExpressions.IntExpression)valueArg).getInt();
                        }
                        break;
                    case "BIGINT_BE":
                    case "BIGINT":
                    case "UINT8_BE":
                    case "UINT8":
                        if (valueArg instanceof ValueExpressions.LongExpression) {
                            value = ((ValueExpressions.LongExpression)valueArg).getLong();
                        }
                        break;
                    case "FLOAT":
                        if (valueArg instanceof ValueExpressions.FloatExpression) {
                            value = ((ValueExpressions.FloatExpression)valueArg).getFloat();
                        }
                        break;
                    case "DOUBLE":
                        if (valueArg instanceof ValueExpressions.DoubleExpression) {
                            value = ((ValueExpressions.DoubleExpression)valueArg).getDouble();
                        }
                        break;
                    case "TIME_EPOCH":
                    case "TIME_EPOCH_BE":
                        if (valueArg instanceof ValueExpressions.TimeExpression) {
                            value = ((ValueExpressions.TimeExpression)valueArg).getTime();
                        }
                        break;
                    case "DATE_EPOCH":
                    case "DATE_EPOCH_BE":
                        if (valueArg instanceof ValueExpressions.DateExpression) {
                            value = ((ValueExpressions.DateExpression)valueArg).getDate();
                        }
                        break;
                    case "BOOLEAN_BYTE":
                        if (valueArg instanceof ValueExpressions.BooleanExpression) {
                            value = ((ValueExpressions.BooleanExpression)valueArg).getBoolean();
                        }
                        break;
                    case "DOUBLE_OB":
                    case "DOUBLE_OBD":
                        if (valueArg instanceof ValueExpressions.DoubleExpression) {
                            value = ((ValueExpressions.DoubleExpression)valueArg).getDouble();
                        }
                        break;
                    case "FLOAT_OB":
                    case "FLOAT_OBD":
                        if (valueArg instanceof ValueExpressions.FloatExpression) {
                            value = ((ValueExpressions.FloatExpression)valueArg).getFloat();
                        }
                        break;
                    case "BIGINT_OB":
                    case "BIGINT_OBD":
                        if (valueArg instanceof ValueExpressions.LongExpression) {
                            value = ((ValueExpressions.LongExpression)valueArg).getLong();
                        }
                        break;
                    case "INT_OB":
                    case "INT_OBD":
                        if (valueArg instanceof ValueExpressions.IntExpression) {
                            value = ((ValueExpressions.IntExpression)valueArg).getInt();
                        }
                        break;
                    case "UTF8":
                        // let visitSchemaPath() handle this.
                        return e.getInput().accept(this, valueArg);
                }

                if (value != null) {
                    this.value = value;
                    this.path = (SchemaPath)e.getInput();
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public Boolean visitUnknown(LogicalExpression e, LogicalExpression valueArg) throws RuntimeException {
        return false;
    }

    @Override
    public Boolean visitBooleanConstant(ValueExpressions.BooleanExpression e, LogicalExpression value) throws RuntimeException {
        this.value = e.getBoolean();
        return true;
    }

    @Override
    public Boolean visitDateConstant(ValueExpressions.DateExpression e, LogicalExpression value) throws RuntimeException {
        this.value = new DateTime(e.getDate());
        return true;
    }

    @Override
    public Boolean visitDecimal9Constant(ValueExpressions.Decimal9Expression e, LogicalExpression value) throws RuntimeException {
        this.value = BigDecimal.valueOf(e.getIntFromDecimal(), e.getScale()).round(new MathContext(e.getPrecision()));
        return true;
    }

    @Override
    public Boolean visitDecimal18Constant(ValueExpressions.Decimal18Expression e, LogicalExpression value) throws RuntimeException {
        this.value = BigDecimal.valueOf(e.getLongFromDecimal(), e.getScale()).round(new MathContext(e.getPrecision()));
        return true;
    }

    @Override
    public Boolean visitDecimal28Constant(ValueExpressions.Decimal28Expression e, LogicalExpression value) throws RuntimeException {
        this.value = e.getBigDecimal();
        return true;
    }

    @Override
    public Boolean visitDecimal38Constant(ValueExpressions.Decimal38Expression e, LogicalExpression value) throws RuntimeException {
        this.value = e.getBigDecimal();
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
    public Boolean visitNullConstant(TypedNullConstant e, LogicalExpression value) throws RuntimeException {
        this.value = null;
        return true;
    }

    @Override
    public Boolean visitTimeConstant(ValueExpressions.TimeExpression e, LogicalExpression value) throws RuntimeException {
        this.value = Duration.millis(e.getTime());
        return true;
    }

    @Override
    public Boolean visitIntervalDayConstant(ValueExpressions.IntervalDayExpression e, LogicalExpression value) throws RuntimeException {
        this.value = Duration.millis(e.getIntervalMillis());
        return true;
    }

    @Override
    public Boolean visitIntervalYearConstant(ValueExpressions.IntervalYearExpression e, LogicalExpression value) throws RuntimeException {
        this.value = Duration.standardDays(e.getIntervalYear() * 365); //TODO високосный год
        return true;
    }

    @Override
    public Boolean visitTimeStampConstant(ValueExpressions.TimeStampExpression e, LogicalExpression value) throws RuntimeException {
        this.value = Duration.millis(e.getTimeStamp());
        return true;
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

        this.path = path;
        boolean success = scan.getQueryParameters().contains(path.getAsUnescapedPath());
        return success && valueArg.accept(this, valueArg);
    }

}
