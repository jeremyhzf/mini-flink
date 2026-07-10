package org.miniflink.runtime;

/** source 算子接口：open 注入输出与并行位置，run 产生数据，close 释放资源。 */
public interface SourceOperator<OUT> {
    void open(Collector<OUT> out, RuntimeContext ctx);
    void run() throws Exception;
    void close();

    /**
     * 复制出独立的 source 算子实例（共享无状态的用户 SourceFunction）。
     * 多并行度下每个 subtask 必须持有独立 source——open 写入的 per-subtask 状态
     * （如 SourceContext）若被多 subtask 共享会相互踩踏导致丢数据/重复。
     */
    SourceOperator<OUT> copy();
}
