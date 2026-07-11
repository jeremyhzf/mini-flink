package org.miniflink.runtime;

/** source 发数据用的上下文，并暴露该 subtask 的并行位置（分片用）；snapshotOffset/restoreOffset 支持断点重放。 */
public interface SourceContext<T> {
    void collect(T record);
    int getSubtaskIndex();
    int getParallelism();

    /** 当前已转发条数（checkpoint 快照用）。 */
    long snapshotOffset();

    /** 恢复时跳过前 offset 条已发记录（exactly-once 重放）。 */
    void restoreOffset(long offset);
}
