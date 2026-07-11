package org.miniflink.api;

import org.miniflink.api.function.SourceFunction;
import org.miniflink.connector.CollectionSource;
import org.miniflink.execution.ExecutionGraph;
import org.miniflink.graph.SourceTransformation;
import org.miniflink.graph.StreamGraph;
import org.miniflink.graph.Transformation;
import org.miniflink.runtime.StreamExecutor;
import org.miniflink.runtime.operator.SourceOperatorImpl;

import java.util.concurrent.atomic.AtomicInteger;

/** 作业入口：构建 DAG 并触发执行。 */
public class StreamExecutionEnvironment {
    private final AtomicInteger idCounter = new AtomicInteger(0);
    private final StreamGraph streamGraph = new StreamGraph();
    private long checkpointInterval = Long.MAX_VALUE;   // 默认不启用 checkpoint
    private int maxRestarts = 3;                         // 默认最多重启 3 次

    public int getNewNodeId() {
        return idCounter.incrementAndGet();
    }

    public <T> DataStream<T> fromCollection(Iterable<T> data) {
        java.util.List<T> list = new java.util.ArrayList<>();
        data.forEach(list::add);   // 转可重复遍历的 List（支持 checkpoint 重放）
        return addSource(new CollectionSource<>(list), "source");
    }

    /** 用自定义 SourceFunction 作为 source（支持带延迟/有状态的源）。 */
    public <T> DataStream<T> addSource(SourceFunction<T> function) {
        return addSource(function, "source");
    }

    /** 用自定义 SourceFunction 作为 source（指定名称）。 */
    public <T> DataStream<T> addSource(SourceFunction<T> function, String name) {
        SourceTransformation<T> source = new SourceTransformation<>(
                getNewNodeId(), name, new SourceOperatorImpl<>(function));
        streamGraph.addTransformation(source);
        return new DataStream<>(this, source);
    }

    public void addTransformation(Transformation<?> t) {
        streamGraph.addTransformation(t);
    }

    public void addSink(Transformation<?> sink) {
        streamGraph.addSink(sink);
    }

    StreamGraph getStreamGraph() {
        return streamGraph;
    }

    /** 启用周期 checkpoint（毫秒间隔）。 */
    public void enableCheckpointing(long intervalMillis) {
        this.checkpointInterval = intervalMillis;
    }

    /** 设置 failover 最大重启次数。 */
    public void setMaxRestarts(int n) {
        this.maxRestarts = n;
    }

    /** 编译逻辑图（StreamGraph → ExecutionGraph）并同步执行（含 checkpoint + 自动 failover）。 */
    public void execute(String jobName) throws Exception {
        ExecutionGraph execGraph = ExecutionGraph.from(streamGraph);
        new StreamExecutor().execute(execGraph, checkpointInterval, maxRestarts);
    }
}
