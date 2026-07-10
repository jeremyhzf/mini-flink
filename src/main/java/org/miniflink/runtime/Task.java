package org.miniflink.runtime;

import java.util.List;

/** 多线程执行单元。broadcastEob 按分区器语义向所有出边的下游发送 EOB（关闭语义）。 */
public interface Task extends Runnable {

    default void broadcastEob(List<Output> outputs, int upstreamIndex) {
        for (Output o : outputs) {
            o.sendEob(upstreamIndex);
        }
    }
}
