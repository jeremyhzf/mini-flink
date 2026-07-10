package org.miniflink.execution;

/** 分区器：决定一条数据发往哪个下游 subtask（0..numDownstream-1）。 */
public interface Partitioner {
    int selectChannel(int numDownstream, Object key, int upstreamIndex);
}
