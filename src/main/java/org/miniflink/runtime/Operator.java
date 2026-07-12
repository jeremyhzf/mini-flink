package org.miniflink.runtime;

import org.miniflink.state.OperatorState;

/** 处理算子接口：接收一条输入，处理后向下游输出。 */
public interface Operator<IN, OUT> {
    void open(Collector<OUT> out, RuntimeContext ctx);
    void processElement(IN record) throws Exception;
    void close();

    /** 收到 watermark（事件时间推进）。普通算子默认不处理（由 OperatorTask 统一转发）。 */
    default void onWatermark(Watermark watermark) {
        // 默认空：map/filter 等透传算子不消费 watermark
    }

    /**
     * 复制出独立的算子实例（共享无状态的用户函数）。
     * 多并行度下每个 subtask 必须持有独立算子——open 写入的 per-subtask 状态
     * （如 out 收集器）若被多 subtask 共享会相互踩踏导致丢数据/重复。
     */
    Operator<IN, OUT> copy();

    /** 算子级快照（无额外状态的算子返回 empty）。 */
    default java.util.Optional<OperatorState> snapshotState() {
        return java.util.Optional.empty();
    }

    /** 从快照恢复算子级状态（无额外状态的算子空实现）。 */
    default void restoreState(OperatorState state) { }
}
