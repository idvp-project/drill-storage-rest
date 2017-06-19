package org.apache.drill.exec.store.rest.query;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableSet;
import org.apache.drill.common.expression.*;
import org.apache.drill.common.expression.visitors.AbstractExprVisitor;
import org.apache.drill.exec.store.rest.RestGroupScan;

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
            .add(ValueExpressions.BooleanExpression.class)
            .add(ValueExpressions.DateExpression.class)
            .add(ValueExpressions.DoubleExpression.class)
            .add(ValueExpressions.FloatExpression.class)
            .add(ValueExpressions.IntExpression.class)
            .add(ValueExpressions.LongExpression.class)
            .add(ValueExpressions.QuotedString.class)
            .add(ValueExpressions.TimeExpression.class)
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
    public Boolean visitSchemaPath(SchemaPath path, LogicalExpression valueArg) throws RuntimeException {
        this.path = path;
        boolean success = scan.getQueryParameters().contains(path.getAsUnescapedPath());
        if (valueArg instanceof ValueExpressions.QuotedString) {
            this.value = ((ValueExpressions.QuotedString) valueArg).getString();
        } else {
            return success && value(valueArg);
        }
        return success;
    }

    private boolean value(LogicalExpression valueArg) {
        Object value = null;
        if (valueArg instanceof ValueExpressions.IntExpression) {
            value = ((ValueExpressions.IntExpression)valueArg).getInt();
        }

        if (valueArg instanceof ValueExpressions.LongExpression) {
            value = ((ValueExpressions.LongExpression)valueArg).getLong();
        }

        if (valueArg instanceof ValueExpressions.FloatExpression) {
            value = ((ValueExpressions.FloatExpression)valueArg).getFloat();
        }

        if (valueArg instanceof ValueExpressions.DoubleExpression) {
            value = ((ValueExpressions.DoubleExpression)valueArg).getDouble();
        }

        if (valueArg instanceof ValueExpressions.TimeExpression) {
            value = ((ValueExpressions.TimeExpression)valueArg).getTime();
        }

        if (valueArg instanceof ValueExpressions.DateExpression) {
            value = ((ValueExpressions.DateExpression)valueArg).getDate();
        }

        if (valueArg instanceof ValueExpressions.BooleanExpression) {
            value = ((ValueExpressions.BooleanExpression)valueArg).getBoolean();
        }

        if (value == null) {
            return false;
        }

        this.value = value;
        return true;
    }

}
