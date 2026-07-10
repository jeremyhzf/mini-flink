package org.miniflink.execution;

/** 一对一：上游 i → 下游 i（要求上下游并行度相同）。 */
public class ForwardPartitioner implements Partitioner {
    @Override
    public int selectChannel(int numDownstream, Object key, int upstreamIndex) {
        if (upstreamIndex >= numDownstream || upstreamIndex < 0) {
            throw new IllegalStateException(
                    "forward 要求上下游并行度相同：upstreamIndex=" + upstreamIndex
                            + ", numDownstream=" + numDownstream);
        }
        return upstreamIndex;
    }
}
