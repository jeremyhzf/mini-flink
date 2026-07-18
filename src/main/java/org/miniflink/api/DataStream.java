package org.miniflink.api;

import org.miniflink.api.function.*;
import org.miniflink.execution.ForwardPartitioner;
import org.miniflink.execution.Partitioner;
import org.miniflink.graph.OneInputTransformation;
import org.miniflink.graph.Transformation;
import org.miniflink.runtime.Operator;
import org.miniflink.runtime.operator.*;
import org.miniflink.time.WatermarkStrategy;

/** 流抽象：链式调用算子方法。keyBy 返回 KeyedStream 提供 keyed 聚合；setParallelism 设并行度。 */
public class DataStream<T> {
    private final StreamExecutionEnvironment env;
    private final Transformation<T> transformation;
    private final Partitioner nextPartitioner = null;   // keyBy 设置，供下一个 transformation 使用
    private final KeySelector<T, ?> nextKeySelector = null;

    public DataStream(StreamExecutionEnvironment env, Transformation<T> transformation) {
        this.env = env;
        this.transformation = transformation;
    }

    public Transformation<T> getTransformation() {
        return transformation;
    }

    /** 设置当前 transformation 的并行度。 */
    public DataStream<T> setParallelism(int parallelism) {
        if (parallelism < 1) {
            throw new IllegalArgumentException("parallelism 必须 >= 1: " + parallelism);
        }
        transformation.setParallelism(parallelism);
        return this;
    }

    /** 按 key 分区：返回 KeyedStream，提供 keyed 聚合操作（reduce/sum）。 */
    public <K> KeyedStream<T, K> keyBy(KeySelector<T, K> keySelector) {
        return new KeyedStream<>(this, keySelector);
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

    /** 打事件时间戳并生成 watermark（独立算子）。 */
    public DataStream<T> assignTimestampsAndWatermarks(WatermarkStrategy<T> strategy) {
        return transform("timestamps-and-watermarks", new TimestampsAndWatermarksOperator<>(strategy));
    }

    public void addSink(SinkFunction<T> sinkFunction) {
        Partitioner part = new ForwardPartitioner();
        OneInputTransformation<T, Void> sink = new OneInputTransformation<>(
                env.getNewNodeId(), "sink", transformation, new SinkOperator<>(sinkFunction), part, nextKeySelector);
        env.addSink(sink);
    }

    private <O> DataStream<O> transform(String name, Operator<T, O> operator) {
        Partitioner part = new ForwardPartitioner();
        OneInputTransformation<T, O> tx = new OneInputTransformation<>(
                env.getNewNodeId(), name, transformation, operator, part, nextKeySelector);
        env.addTransformation(tx);
        return new DataStream<>(env, tx); // 新流重置为 forward（不继承 keyBy）
    }

    /** 供 KeyedStream 使用：建一个带指定分区器与 keySelector 的 transformation。 */
    <O> DataStream<O> keyedTransform(String name, Operator<T, O> operator,
                                     Partitioner partitioner,
                                     KeySelector<T, ?> keySelector) {
        OneInputTransformation<T, O> tx = new OneInputTransformation<>(
                env.getNewNodeId(), name, transformation, operator, partitioner, keySelector);
        env.addTransformation(tx);
        return new DataStream<>(env, tx);
    }
}
