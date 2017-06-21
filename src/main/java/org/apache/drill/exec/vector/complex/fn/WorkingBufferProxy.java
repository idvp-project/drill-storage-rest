package org.apache.drill.exec.vector.complex.fn;

import io.netty.buffer.DrillBuf;

/**
 * Паблик Мороз, открывающий доступ к WorkingBuffer из других пакетов
 * @author Oleg Zinoviev
 * @since 20.06.2017.
 */
public class WorkingBufferProxy extends WorkingBuffer {
    public WorkingBufferProxy(DrillBuf workBuf) {
        super(workBuf);
    }
}
