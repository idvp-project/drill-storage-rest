package org.apache.drill.exec.vector.complex.impl;

import com.google.common.collect.Sets;
import org.apache.drill.exec.vector.complex.MapVector;
import org.apache.drill.exec.vector.complex.writer.*;
import org.joda.time.DateTime;
import org.joda.time.Duration;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * @author Oleg Zinoviev
 * @since 20.06.2017.
 */
public class EnhancedSingleMapWriter extends SingleMapWriter {
    private final Map<String, Object> pushedDownFilters;
    private final Set<String> currentObjectFields = Sets.newHashSet();

    EnhancedSingleMapWriter(MapVector container,
                            FieldWriter parent,
                            boolean unionEnabled,
                            Map<String, Object> pushedDownFilters) {
        super(container, parent, unionEnabled);
        this.pushedDownFilters = pushedDownFilters == null ? Collections.emptyMap() : pushedDownFilters;
    }

    @Override
    public MapWriter map(String name) {
        currentObjectFields.add(name);
        return super.map(name);
    }

    @Override
    public ListWriter list(String name) {
        currentObjectFields.add(name);
        return super.list(name);
    }

    @Override
    public TinyIntWriter tinyInt(String name) {
        currentObjectFields.add(name);
        return super.tinyInt(name);
    }

    @Override
    public UInt1Writer uInt1(String name) {
        currentObjectFields.add(name);
        return super.uInt1(name);
    }

    @Override
    public UInt2Writer uInt2(String name) {
        currentObjectFields.add(name);
        return super.uInt2(name);
    }

    @Override
    public SmallIntWriter smallInt(String name) {
        currentObjectFields.add(name);
        return super.smallInt(name);
    }

    @Override
    public IntWriter integer(String name) {
        currentObjectFields.add(name);
        return super.integer(name);
    }

    @Override
    public UInt4Writer uInt4(String name) {
        currentObjectFields.add(name);
        return super.uInt4(name);
    }

    @Override
    public Float4Writer float4(String name) {
        currentObjectFields.add(name);
        return super.float4(name);
    }

    @Override
    public TimeWriter time(String name) {
        currentObjectFields.add(name);
        return super.time(name);
    }

    @Override
    public IntervalYearWriter intervalYear(String name) {
        currentObjectFields.add(name);
        return super.intervalYear(name);
    }

    @Override
    public Decimal9Writer decimal9(String name) {
        currentObjectFields.add(name);
        return super.decimal9(name);
    }

    @Override
    public Decimal9Writer decimal9(String name, int scale, int precision) {
        currentObjectFields.add(name);
        return super.decimal9(name, scale, precision);
    }

    @Override
    public BigIntWriter bigInt(String name) {
        currentObjectFields.add(name);
        return super.bigInt(name);
    }

    @Override
    public UInt8Writer uInt8(String name) {
        currentObjectFields.add(name);
        return super.uInt8(name);
    }

    @Override
    public Float8Writer float8(String name) {
        currentObjectFields.add(name);
        return super.float8(name);
    }

    @Override
    public DateWriter date(String name) {
        currentObjectFields.add(name);
        return super.date(name);
    }

    @Override
    public TimeStampWriter timeStamp(String name) {
        currentObjectFields.add(name);
        return super.timeStamp(name);
    }

    @Override
    public Decimal18Writer decimal18(String name) {
        currentObjectFields.add(name);
        return super.decimal18(name);
    }

    @Override
    public Decimal18Writer decimal18(String name, int scale, int precision) {
        currentObjectFields.add(name);
        return super.decimal18(name, scale, precision);
    }

    @Override
    public IntervalDayWriter intervalDay(String name) {
        currentObjectFields.add(name);
        return super.intervalDay(name);
    }

    @Override
    public IntervalWriter interval(String name) {
        currentObjectFields.add(name);
        return super.interval(name);
    }

    @Override
    public Decimal28DenseWriter decimal28Dense(String name) {
        currentObjectFields.add(name);
        return super.decimal28Dense(name);
    }

    @Override
    public Decimal28DenseWriter decimal28Dense(String name, int scale, int precision) {
        currentObjectFields.add(name);
        return super.decimal28Dense(name, scale, precision);
    }

    @Override
    public Decimal38DenseWriter decimal38Dense(String name) {
        currentObjectFields.add(name);
        return super.decimal38Dense(name);
    }

    @Override
    public Decimal38DenseWriter decimal38Dense(String name, int scale, int precision) {
        currentObjectFields.add(name);
        return super.decimal38Dense(name, scale, precision);
    }

    @Override
    public Decimal28SparseWriter decimal28Sparse(String name) {
        currentObjectFields.add(name);
        return super.decimal28Sparse(name);
    }

    @Override
    public Decimal28SparseWriter decimal28Sparse(String name, int scale, int precision) {
        currentObjectFields.add(name);
        return super.decimal28Sparse(name, scale, precision);
    }

    @Override
    public Decimal38SparseWriter decimal38Sparse(String name) {
        currentObjectFields.add(name);
        return super.decimal38Sparse(name);
    }

    @Override
    public Decimal38SparseWriter decimal38Sparse(String name, int scale, int precision) {
        currentObjectFields.add(name);
        return super.decimal38Sparse(name, scale, precision);
    }

    @Override
    public VarBinaryWriter varBinary(String name) {
        currentObjectFields.add(name);
        return super.varBinary(name);
    }

    @Override
    public VarCharWriter varChar(String name) {
        currentObjectFields.add(name);
        return super.varChar(name);
    }

    @Override
    public Var16CharWriter var16Char(String name) {
        currentObjectFields.add(name);
        return super.var16Char(name);
    }

    @Override
    public BitWriter bit(String name) {
        currentObjectFields.add(name);
        return super.bit(name);
    }

    @Override
    public void start() {
        currentObjectFields.clear();
        super.start();
    }

    @Override
    public void end() {
        for (Map.Entry<String, Object> entry : pushedDownFilters.entrySet()) {
            if (currentObjectFields.contains(entry.getKey()) || entry.getValue() == null) {
                continue;
            }

            if (entry.getValue() instanceof Integer) {
                integer(entry.getKey()).writeInt((int) entry.getValue());
            } else if (entry.getValue() instanceof Long) {
                bigInt(entry.getKey()).writeBigInt((long) entry.getValue());
            } else if (entry.getValue() instanceof Float) {
                float4(entry.getKey()).writeFloat4((float) entry.getValue());
            } else if (entry.getValue() instanceof Double) {
                float8(entry.getKey()).writeFloat8((double) entry.getValue());
            } else if (entry.getValue() instanceof Duration) {
                time(entry.getKey()).writeTime((int) ((Duration) entry.getValue()).getMillis());
            } else if (entry.getValue() instanceof DateTime) {
                date(entry.getKey()).writeDate(((DateTime) entry.getValue()).getMillis());
            }

        }
        super.end();
    }

    @Override
    public void clear() {
        currentObjectFields.clear();
        super.clear();
    }
}
