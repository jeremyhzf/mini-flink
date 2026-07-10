package org.miniflink.api;

import org.miniflink.api.function.KeySelector;
import org.miniflink.api.function.ReduceFunction;
import org.miniflink.execution.HashPartitioner;
import org.miniflink.runtime.Operator;
import org.miniflink.runtime.operator.ReduceOperator;

/**
 * keyBy 返回的流：携带 keySelector，提供 keyed 聚合操作。
 * reduce/sum 建一个 hash 分区的 OneInputTransformation（reduce 算子）。
 */
public class KeyedStream<T, K> {
    private final DataStream<T> dataStream;
    private final KeySelector<T, K> keySelector;

    public KeyedStream(DataStream<T> dataStream, KeySelector<T, K> keySelector) {
        this.dataStream = dataStream;
        this.keySelector = keySelector;
    }

    public DataStream<T> reduce(ReduceFunction<T> reduceFn) {
        ReduceOperator<T> op = new ReduceOperator<>(reduceFn);
        return dataStream.keyedTransform("reduce", op, new HashPartitioner(), keySelector);
    }

    /** 便捷：直接委托 reduce（如 sum(Integer::sum)）。 */
    public DataStream<T> sum(ReduceFunction<T> sumFn) {
        return reduce(sumFn);
    }

    public KeySelector<T, K> getKeySelector() {
        return keySelector;
    }
}
