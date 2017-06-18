package org.apache.drill.exec.store.rest.calcite;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.drill.common.expression.*;
import org.apache.drill.common.expression.visitors.AbstractExprVisitor;

import java.util.Collections;
import java.util.Set;

/**
 * @author Oleg Zinoviev
 * @since 16.06.2017.
 */
public class RestFilterNameParser extends AbstractExprVisitor<Set<String>, Void, RuntimeException> {


    @Override
    public Set<String> visitFunctionCall(FunctionCall call, Void value) throws RuntimeException {

        ImmutableSet.Builder<String> fieldNames = ImmutableSet.builder();

        ImmutableList<LogicalExpression> args = call.args;
        for (LogicalExpression le : args) {

            if (le instanceof FieldReference) {
                fieldNames.add(((FieldReference) le).getAsUnescapedPath());
            }

            fieldNames.addAll(le.accept(this, value));
        }

        return fieldNames.build();
    }

    @Override
    public Set<String> visitBooleanOperator(BooleanOperator op, Void value) throws RuntimeException {
        return visitFunctionCall(op, value);
    }

    @Override
    public Set<String> visitUnknown(LogicalExpression e, Void value) throws RuntimeException {
        return Collections.emptySet();
    }
}
