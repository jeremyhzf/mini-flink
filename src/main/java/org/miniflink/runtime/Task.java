package org.miniflink.runtime;

import java.util.List;

/** 多线程执行单元。broadcastEob 向所有出边的下游 Channel 广播 EOB（关闭语义）。 */
public interface Task extends Runnable {

    default void broadcastEob(List<Output> outputs) {
        for (Output o : outputs) {
            o.sendEob();
        }
    }
}
