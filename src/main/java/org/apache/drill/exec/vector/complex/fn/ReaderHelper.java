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
package org.apache.drill.exec.vector.complex.fn;

import org.apache.drill.exec.vector.complex.writer.BaseWriter;
import org.joda.time.DateTime;
import org.joda.time.Duration;

import java.io.IOException;

/**
 * @author Oleg Zinoviev
 * @since 21.06.2017.
 */
final class ReaderHelper {
    private ReaderHelper() {
    }

    static void write(BaseWriter.MapWriter mapWriter, String field, Object value, WorkingBuffer buffer) throws IOException {
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
                    buffer.prepareVarCharHolder(value.toString()),
                    buffer.getBuf());
        }
    }
}
