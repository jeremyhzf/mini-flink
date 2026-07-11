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
}
