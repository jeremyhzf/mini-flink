package org.miniflink.api;

import org.miniflink.connector.CollectionSource;
import org.miniflink.graph.SourceTransformation;
import org.miniflink.graph.StreamGraph;
import org.miniflink.graph.Transformation;
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
        SourceTransformation<T> source = new SourceTransformation<>(
                getNewNodeId(), "source", new SourceOperatorImpl<>(new CollectionSource<>(data)));
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

    /** 编译逻辑图并执行。运行逻辑在 Task 7（ExecutionGraph + StreamExecutor）补全。 */
    public void execute(String jobName) throws Exception {
        throw new UnsupportedOperationException("execute 将在 Task 7 实现");
    }
}
