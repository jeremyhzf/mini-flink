package org.miniflink.runtime.operator;

import org.miniflink.runtime.Collector;
import org.miniflink.runtime.Operator;
import org.miniflink.runtime.RuntimeContext;
import org.miniflink.runtime.Watermark;
import org.miniflink.time.WatermarkStrategy;

/**
 * 打事件时间戳 + 发 watermark 的算子。
 * processElement：extractTimestamp → 设 ctx ts（OutputCollector 读它包 Record(value, ts) 流向下游）
 *               → 输出 value → 发当前 watermark（单调）。
 */
public class TimestampsAndWatermarksOperator<T> implements Operator<T, T> {
    private final WatermarkStrategy<T> strategy;
    private Collector<T> out;
    private RuntimeContext ctx;

    public TimestampsAndWatermarksOperator(WatermarkStrategy<T> strategy) {
        this.strategy = strategy;
    }

    @Override
    public void open(Collector<T> out, RuntimeContext ctx) {
        this.out = out;
        this.ctx = ctx;
    }

    @Override
    public void processElement(T record) {
        long ts = strategy.extractTimestamp(record);
        ctx.setCurrentTimestamp(ts);                                   // 设 ts，OutputCollector 读它包 Record
        out.collect(record);                                           // 输出 value（带 ts 流向下游）
        ctx.emitWatermark(new Watermark(strategy.currentWatermark())); // 每条记录后发当前 watermark
    }

    @Override
    public void close() { /* 无操作 */ }

    @Override
    public TimestampsAndWatermarksOperator<T> copy() {
        return new TimestampsAndWatermarksOperator<>(strategy.copy()); // per-subtask 独立 strategy（BoundedOutOfOrdernessWatermarks 有状态）
    }
}
