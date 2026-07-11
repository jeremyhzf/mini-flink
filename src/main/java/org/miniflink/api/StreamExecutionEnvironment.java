package org.miniflink.api;

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

    public int getNewNodeId() {
        return idCounter.incrementAndGet();
    }

    public <T> DataStream<T> fromCollection(Iterable<T> data) {
        java.util.List<T> list = new java.util.ArrayList<>();
        data.forEach(list::add);   // 转可重复遍历的 List（支持 checkpoint 重放）
        SourceTransformation<T> source = new SourceTransformation<>(
                getNewNodeId(), "source", new SourceOperatorImpl<>(new CollectionSource<>(list)));
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

    /** 编译逻辑图（StreamGraph → ExecutionGraph）并同步执行。 */
    public void execute(String jobName) throws Exception {
        ExecutionGraph execGraph = ExecutionGraph.from(streamGraph);
        new StreamExecutor().execute(execGraph);
    }
}
