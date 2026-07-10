package org.miniflink.runtime.operator;

import org.miniflink.api.function.SinkFunction;
import org.miniflink.runtime.Collector;
import org.miniflink.runtime.Operator;
import org.miniflink.runtime.RuntimeContext;

/** 包装 SinkFunction 的算子：到达元素交给 sink 消费，无下游输出（OUT = Void）。 */
public class SinkOperator<IN> implements Operator<IN, Void> {
    private final SinkFunction<IN> sinkFunction;

    public SinkOperator(SinkFunction<IN> sinkFunction) {
        this.sinkFunction = sinkFunction;
    }

    @Override
    public void open(Collector<Void> out, RuntimeContext ctx) {
        // sink 无下游输出，ctx 暂不使用
    }

    @Override
    public void processElement(IN record) throws Exception {
        sinkFunction.invoke(record);
    }

    @Override
    public void close() {
        // 阶段①无需操作
    }

    @Override
    public SinkOperator<IN> copy() {
        return new SinkOperator<>(sinkFunction);
    }
}
