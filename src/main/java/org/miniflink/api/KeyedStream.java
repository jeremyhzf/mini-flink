package org.miniflink.api;

import org.miniflink.api.function.KeySelector;
import org.miniflink.api.function.ReduceFunction;
import org.miniflink.execution.HashPartitioner;
import org.miniflink.runtime.operator.ReduceOperator;

/**
 * keyBy 返回的流：携带 keySelector，提供 keyed 聚合操作。
 * reduce/sum 建一个 hash 分区的 OneInputTransformation（reduce 算子）。
 */
public class KeyedStream<T, K> {
    private final DataStream<T> dataStream;
    private final KeySelector<T, K> keySelector;
    private final HashPartitioner hashPartitioner = new HashPartitioner();

    public KeyedStream(DataStream<T> dataStream, KeySelector<T, K> keySelector) {
        this.dataStream = dataStream;
        this.keySelector = keySelector;
    }

    public DataStream<T> reduce(ReduceFunction<T> reduceFn) {
        ReduceOperator<T> op = new ReduceOperator<>(reduceFn);
        return dataStream.keyedTransform("reduce", op, hashPartitioner, keySelector);
    }

    /** 按 windowAssigner 开窗，返回 WindowedStream。 */
    public <W extends org.miniflink.window.Window> WindowedStream<T, W> window(
            org.miniflink.window.WindowAssigner<T, W> windowAssigner) {
        return new WindowedStream<>(this, windowAssigner);
    }

    /** 供 WindowedStream 使用：建一个 hash 分区（沿用本 KeyedStream 的 keySelector）的 transformation。 */
    <O> DataStream<O> keyedTransformFor(String name, org.miniflink.runtime.Operator<T, O> operator) {
        return dataStream.keyedTransform(name, operator, hashPartitioner, keySelector);
    }

    /** 便捷：直接委托 reduce（如 sum(Integer::sum)）。 */
    public DataStream<T> sum(ReduceFunction<T> sumFn) {
        return reduce(sumFn);
    }

    public KeySelector<T, K> getKeySelector() {
        return keySelector;
    }
}
