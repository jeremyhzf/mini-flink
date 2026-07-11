package org.miniflink.runtime;

import java.util.List;

/** 多线程执行单元。broadcastEob 按分区器语义向所有出边的下游发送 EOB（关闭语义）。 */
public interface Task extends Runnable {

    default void broadcastEob(List<Output> outputs, int upstreamIndex) {
        for (Output o : outputs) {
            o.sendEob(upstreamIndex);
        }
    }

    /** 向所有出边广播 watermark（source 结束 / 算子转发用）。 */
    default void broadcastWatermark(List<Output> outputs, Watermark wm) {
        for (Output o : outputs) {
            o.sendWatermark(wm);
        }
    }

    /** 向所有出边广播 barrier（对齐后转发用）。 */
    default void broadcastBarrier(List<Output> outputs, Barrier barrier) {
        for (Output o : outputs) {
            o.sendBarrier(barrier);
        }
    }
}
