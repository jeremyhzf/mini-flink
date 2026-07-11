package org.miniflink.runtime;

import java.util.List;

/**
 * Collector 实现：collect 时把记录按分区器路由到下游 Channel（fan-out）。
 * 持有 RuntimeContext 以读取当前记录的事件时间戳，包装进 Record 出链。
 */
public class OutputCollector<T> implements Collector<T> {
    private final List<Output> outputs;
    private final int upstreamIndex;
    private final RuntimeContext ctx;

    public OutputCollector(List<Output> outputs, RuntimeContext ctx) {
        this.outputs = outputs;
        this.ctx = ctx;
        this.upstreamIndex = ctx.getSubtaskIndex();
    }

    @Override
    public void collect(T record) {
        for (Output o : outputs) {
            try {
                o.route(record, ctx.getCurrentTimestamp(), upstreamIndex);
            } catch (Exception e) {
                throw new RuntimeException("输出路由异常", e);
            }
        }
    }

    @Override
    public void close() {
        // EOB 由 Task 统一发送
    }
}
