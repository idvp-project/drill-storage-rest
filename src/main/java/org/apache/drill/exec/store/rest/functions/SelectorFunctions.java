package org.apache.drill.exec.store.rest.functions;

import io.netty.buffer.DrillBuf;
import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.holders.Var16CharHolder;
import org.apache.drill.exec.expr.holders.VarCharHolder;

import javax.inject.Inject;

/**
 * @author Oleg Zinoviev
 * @since 21.06.2017.
 */
@SuppressWarnings("unused")
public class SelectorFunctions {
    private SelectorFunctions() {
    }

    @FunctionTemplate(name = "selector_CSS",
            scope = FunctionTemplate.FunctionScope.SIMPLE,
            nulls = FunctionTemplate.NullHandling.NULL_IF_NULL,
            isRandom = true)
    public static class DOMSelectorVarCharFunc implements DrillSimpleFunc {

        @Param
        VarCharHolder source;

        @Param
        VarCharHolder selector;

        @Output
        org.apache.drill.exec.vector.complex.writer.BaseWriter.ComplexWriter output;

        @Inject
        DrillBuf buffer;

        @Override
        public void setup() {
        }

        @Override
        public void eval() {
            org.apache.drill.exec.store.rest.functions.SelectorFunctionsBody.DOMSelectorFuncBody.eval(source, selector, output, buffer);
        }
    }

    @FunctionTemplate(name = "selector_CSS",
            scope = FunctionTemplate.FunctionScope.SIMPLE,
            nulls = FunctionTemplate.NullHandling.NULL_IF_NULL,
            isRandom = true)
    public static class DOMSelectorVar16CharFunc implements DrillSimpleFunc {

        @Param
        Var16CharHolder source;

        @Param
        Var16CharHolder selector;

        @Output
        org.apache.drill.exec.vector.complex.writer.BaseWriter.ComplexWriter output;

        @Inject
        DrillBuf buffer;

        @Override
        public void setup() {
        }

        @Override
        public void eval() {
            org.apache.drill.exec.store.rest.functions.SelectorFunctionsBody.DOMSelectorFuncBody.eval(source, selector, output, buffer);
        }
    }
}
