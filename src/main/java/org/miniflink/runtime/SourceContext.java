package org.miniflink.runtime;

/** source 发数据用的上下文，并暴露该 subtask 的并行位置（分片用）。 */
public interface SourceContext<T> {
    void collect(T record);
    int getSubtaskIndex();
    int getParallelism();
}
