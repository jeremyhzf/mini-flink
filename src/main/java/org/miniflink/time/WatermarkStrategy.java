package org.miniflink.time;

import java.time.Duration;

/**
 * 事件时间策略：提取 timestamp + 生成 watermark。
 * 仿 Flink WatermarkStrategy。
 */
public interface WatermarkStrategy<T> extends TimestampAssigner<T> {
    /** 当前 watermark（单调递增，只增不减）。 */
    long currentWatermark();

    /** 固定乱序容忍策略：watermark = maxObservedTimestamp - maxOutOfOrderness。 */
    static <T> WatermarkStrategy<T> forBoundedOutOfOrderness(Duration maxOutOfOrderness, TimestampAssigner<T> assigner) {
        return new BoundedOutOfOrdernessWatermarks<>(maxOutOfOrderness.toMillis(), assigner);
    }

    /**
     * 复制本策略，供每个 subtask 持有独立实例（消除有状态策略在 parallelism>1 时的竞态）。
     * 默认返回 this（适用于无状态策略）；有状态实现须覆写。
     */
    default WatermarkStrategy<T> copy() {
        return this;
    }
}
