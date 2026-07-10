package org.miniflink.execution;

import java.util.concurrent.atomic.AtomicInteger;

/** 轮询：多下游间循环分发（source→多下游并行用）。线程安全计数器。 */
public class RebalancePartitioner implements Partitioner {
    private final AtomicInteger counter = new AtomicInteger(0);

    @Override
    public int selectChannel(int numDownstream, Object key, int upstreamIndex) {
        return Math.floorMod(counter.getAndIncrement(), numDownstream);
    }
}
