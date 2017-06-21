package org.apache.drill.exec.store.rest.read;

import org.apache.drill.exec.vector.complex.fn.WorkingBufferProxy;
import org.apache.drill.exec.vector.complex.writer.BaseWriter;
import org.joda.time.DateTime;
import org.joda.time.Duration;

import java.io.IOException;

/**
 * @author Oleg Zinoviev
 * @since 21.06.2017.
 */
public final class ReaderHelper {
    private ReaderHelper() {
    }

    public static void write(BaseWriter.MapWriter mapWriter, String field, Object value, WorkingBufferProxy bufferProxy) throws IOException {
        if (value == null) {
            return;
        }

        if (value instanceof Integer) {
            mapWriter.integer(field).writeInt((int) value);
        } else if (value instanceof Long) {
            mapWriter.bigInt(field).writeBigInt((long) value);
        } else if (value instanceof Float) {
            mapWriter.float4(field).writeFloat4((float) value);
        } else if (value instanceof Double) {
            mapWriter.float8(field).writeFloat8((double) value);
        } else if (value instanceof Duration) {
            mapWriter.time(field).writeTime((int) ((Duration) value).getMillis());
        } else if (value instanceof DateTime) {
            mapWriter.date(field).writeDate(((DateTime) value).getMillis());
        } else {
            mapWriter.varChar(field).writeVarChar(0,
                    bufferProxy.prepareVarCharHolder(value.toString()),
                    bufferProxy.getBuf());
        }
    }
}
