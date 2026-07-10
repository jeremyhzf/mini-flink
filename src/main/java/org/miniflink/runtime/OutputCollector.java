package org.miniflink.runtime;

import java.util.List;

/**
 * Collector 实现：collect 时把记录按分区器路由到下游 Channel（fan-out）。
 * 一个算子可能有多条出边（阶段②线性链通常 1 条），遍历每个 Output 路由。
 */
public class OutputCollector<T> implements Collector<T> {
    private final List<Output> outputs;
    private final int upstreamIndex;

    public OutputCollector(List<Output> outputs, int upstreamIndex) {
        this.outputs = outputs;
        this.upstreamIndex = upstreamIndex;
    }

    @Override
    public void collect(T record) {
        for (Output o : outputs) {
            try {
                o.route(record, upstreamIndex);
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
