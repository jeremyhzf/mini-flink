package org.miniflink.execution;

/** 按 key 哈希取模：同 key 恒落同一下游 subtask（keyBy 用）。 */
public class HashPartitioner implements Partitioner {
    @Override
    public int selectChannel(int numDownstream, Object key, int upstreamIndex) {
        int h = (key == null) ? 0 : key.hashCode();
        return Math.floorMod(h, numDownstream);
    }
}
