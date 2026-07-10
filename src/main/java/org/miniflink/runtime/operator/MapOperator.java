package org.miniflink.runtime.operator;

import org.miniflink.api.function.MapFunction;
import org.miniflink.runtime.Collector;
import org.miniflink.runtime.Operator;
import org.miniflink.runtime.RuntimeContext;

/** 包装 MapFunction 的算子。 */
public class MapOperator<IN, OUT> implements Operator<IN, OUT> {
    private final MapFunction<IN, OUT> mapFunction;
    private Collector<OUT> out;

    public MapOperator(MapFunction<IN, OUT> mapFunction) {
        this.mapFunction = mapFunction;
    }

    @Override
    public void open(Collector<OUT> out, RuntimeContext ctx) {
        this.out = out;
    }

    @Override
    public void processElement(IN record) throws Exception {
        OUT result = mapFunction.map(record);
        out.collect(result);
    }

    @Override
    public void close() {
        // 阶段①无需操作
    }

    @Override
    public MapOperator<IN, OUT> copy() {
        return new MapOperator<>(mapFunction);
    }
}
