package org.miniflink.runtime.operator;

import org.miniflink.api.function.FlatMapFunction;
import org.miniflink.runtime.Collector;
import org.miniflink.runtime.Operator;

/** 包装 FlatMapFunction 的算子：把下游 Collector 交给用户函数发出多条结果。 */
public class FlatMapOperator<IN, OUT> implements Operator<IN, OUT> {
    private final FlatMapFunction<IN, OUT> flatMapFunction;
    private Collector<OUT> out;

    public FlatMapOperator(FlatMapFunction<IN, OUT> flatMapFunction) {
        this.flatMapFunction = flatMapFunction;
    }

    @Override
    public void open(Collector<OUT> out) {
        this.out = out;
    }

    @Override
    public void processElement(IN record) throws Exception {
        flatMapFunction.flatMap(record, out);
    }

    @Override
    public void close() {
        // 阶段①无需操作
    }
}
