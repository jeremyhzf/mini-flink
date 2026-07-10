package org.miniflink.runtime;

/** 处理算子接口：接收一条输入，处理后向下游输出。 */
public interface Operator<IN, OUT> {
    void open(Collector<OUT> out, RuntimeContext ctx);
    void processElement(IN record) throws Exception;
    void close();

    /**
     * 复制出独立的算子实例（共享无状态的用户函数）。
     * 多并行度下每个 subtask 必须持有独立算子——open 写入的 per-subtask 状态
     * （如 out 收集器）若被多 subtask 共享会相互踩踏导致丢数据/重复。
     */
    Operator<IN, OUT> copy();
}
