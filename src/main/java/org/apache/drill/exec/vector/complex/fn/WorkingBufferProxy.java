package org.apache.drill.exec.vector.complex.fn;

import io.netty.buffer.DrillBuf;

/**
 * @author Oleg Zinoviev
 * @since 20.06.2017.
 */
public class WorkingBufferProxy extends WorkingBuffer {
    public WorkingBufferProxy(DrillBuf workBuf) {
        super(workBuf);
    }
}
