package org.miniflink.api;

import org.miniflink.api.function.FilterFunction;
import org.miniflink.api.function.FlatMapFunction;
import org.miniflink.api.function.MapFunction;
import org.miniflink.api.function.SinkFunction;
import org.miniflink.graph.OneInputTransformation;
import org.miniflink.graph.Transformation;
import org.miniflink.runtime.Operator;
import org.miniflink.runtime.operator.FilterOperator;
import org.miniflink.runtime.operator.FlatMapOperator;
import org.miniflink.runtime.operator.MapOperator;
import org.miniflink.runtime.operator.SinkOperator;

/** 流抽象：链式调用算子方法，内部累积逻辑 transformation。 */
public class DataStream<T> {
    private final StreamExecutionEnvironment env;
    private final Transformation<T> transformation;

    public DataStream(StreamExecutionEnvironment env, Transformation<T> transformation) {
        this.env = env;
        this.transformation = transformation;
    }

    public Transformation<T> getTransformation() {
        return transformation;
    }

    public <O> DataStream<O> map(MapFunction<T, O> mapper) {
        return transform("map", new MapOperator<>(mapper));
    }

    public <O> DataStream<O> flatMap(FlatMapFunction<T, O> flatMapper) {
        return transform("flatMap", new FlatMapOperator<>(flatMapper));
    }

    public DataStream<T> filter(FilterFunction<T> filter) {
        return transform("filter", new FilterOperator<>(filter));
    }

    public void addSink(SinkFunction<T> sinkFunction) {
        OneInputTransformation<T, Void> sink = new OneInputTransformation<>(
                env.getNewNodeId(), "sink", transformation, new SinkOperator<>(sinkFunction));
        env.addSink(sink);
    }

    /** 通用单输入变换：创建 transformation、注册到 env、返回新的 DataStream。 */
    private <O> DataStream<O> transform(String name, Operator<T, O> operator) {
        OneInputTransformation<T, O> tx = new OneInputTransformation<>(
                env.getNewNodeId(), name, transformation, operator);
        env.addTransformation(tx);
        return new DataStream<>(env, tx);
    }
}
