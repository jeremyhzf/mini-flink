package org.miniflink.api;

import org.miniflink.api.function.FilterFunction;
import org.miniflink.api.function.FlatMapFunction;
import org.miniflink.api.function.KeySelector;
import org.miniflink.api.function.MapFunction;
import org.miniflink.api.function.SinkFunction;
import org.miniflink.execution.ForwardPartitioner;
import org.miniflink.execution.HashPartitioner;
import org.miniflink.execution.Partitioner;
import org.miniflink.graph.OneInputTransformation;
import org.miniflink.graph.Transformation;
import org.miniflink.runtime.Operator;
import org.miniflink.runtime.operator.FilterOperator;
import org.miniflink.runtime.operator.FlatMapOperator;
import org.miniflink.runtime.operator.MapOperator;
import org.miniflink.runtime.operator.SinkOperator;

/** 流抽象：链式调用算子方法。keyBy 设置下一条边的分区器；setParallelism 设并行度。 */
public class DataStream<T> {
    private final StreamExecutionEnvironment env;
    private final Transformation<T> transformation;
    private Partitioner nextPartitioner = null;   // keyBy 设置，供下一个 transformation 使用
    private KeySelector<T, ?> nextKeySelector = null;

    public DataStream(StreamExecutionEnvironment env, Transformation<T> transformation) {
        this.env = env;
        this.transformation = transformation;
    }

    public Transformation<T> getTransformation() {
        return transformation;
    }

    /** 设置当前 transformation 的并行度。 */
    public DataStream<T> setParallelism(int parallelism) {
        transformation.setParallelism(parallelism);
        return this;
    }

    /** 按key分区：使下一个算子的入边用 hash 分区。返回的流与原流共享同一 transformation。 */
    public DataStream<T> keyBy(KeySelector<T, ?> keySelector) {
        DataStream<T> keyed = new DataStream<>(env, transformation);
        keyed.nextPartitioner = new HashPartitioner();
        keyed.nextKeySelector = keySelector;
        return keyed;
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
        Partitioner part = (nextPartitioner != null) ? nextPartitioner : new ForwardPartitioner();
        OneInputTransformation<T, Void> sink = new OneInputTransformation<>(
                env.getNewNodeId(), "sink", transformation, new SinkOperator<>(sinkFunction), part, nextKeySelector);
        env.addSink(sink);
    }

    private <O> DataStream<O> transform(String name, Operator<T, O> operator) {
        Partitioner part = (nextPartitioner != null) ? nextPartitioner : new ForwardPartitioner();
        OneInputTransformation<T, O> tx = new OneInputTransformation<>(
                env.getNewNodeId(), name, transformation, operator, part, nextKeySelector);
        env.addTransformation(tx);
        return new DataStream<>(env, tx); // 新流重置为 forward（不继承 keyBy）
    }
}
