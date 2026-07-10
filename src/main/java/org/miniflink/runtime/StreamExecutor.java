package org.miniflink.runtime;

import org.miniflink.execution.ExecutionEdge;
import org.miniflink.execution.ExecutionGraph;
import org.miniflink.execution.ExecutionVertex;
import org.miniflink.execution.ForwardPartitioner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 多线程执行器：为每个 ExecutionVertex 建 Task，Task 间用 Channel 连接，启动线程并 join 等待。
 * 任一 Task 未捕获异常 → 记录并在 join 后抛出。
 */
public class StreamExecutor {

    public void execute(ExecutionGraph graph) throws Exception {
        // 1. 为每个 target vertex 建输入 Channel（fan-in 汇聚）
        Map<ExecutionVertex, Channel> inputChannelOf = new HashMap<>();
        for (ExecutionEdge edge : graph.getEdges()) {
            for (ExecutionVertex t : edge.getTargets()) {
                inputChannelOf.computeIfAbsent(t, k -> new Channel());
            }
        }

        // 2. 为每个 vertex 建 RuntimeContext + Task
        List<Task> tasks = new ArrayList<>();
        for (ExecutionVertex v : graph.getVertices()) {
            RuntimeContext ctx = new RuntimeContextImpl(
                    v.getSubtaskIndex(), v.getParallelism(), findInputKeySelector(v, graph.getEdges()));
            List<Output> outputs = buildOutputs(v, graph.getEdges(), inputChannelOf);
            if (v.isSource()) {
                tasks.add(new SourceTask(v.getSourceOperator(), outputs, ctx));
            } else {
                Channel input = inputChannelOf.get(v);
                int pending = countUpstreams(v, graph.getEdges());
                tasks.add(new OperatorTask(new OperatorChain<>(v.getOperators()), input, pending, outputs, ctx));
            }
        }

        // 3. 启动所有线程
        List<Thread> threads = new ArrayList<>();
        AtomicReference<Throwable> error = new AtomicReference<>();
        for (Task t : tasks) {
            Thread th = new Thread(t, "miniflink-task-" + threads.size());
            th.setUncaughtExceptionHandler((tr, e) -> error.compareAndSet(null, e));
            threads.add(th);
            th.start();
        }

        // 4. join 等待全部
        for (Thread th : threads) {
            th.join();
        }

        // 5. 异常传播
        if (error.get() != null) {
            throw new RuntimeException("作业执行失败", error.get());
        }
    }

    private List<Output> buildOutputs(ExecutionVertex v, List<ExecutionEdge> edges,
                                      Map<ExecutionVertex, Channel> inputChannelOf) {
        List<Output> outputs = new ArrayList<>();
        for (ExecutionEdge edge : edges) {
            if (edge.getSources().contains(v)) {
                List<Channel> targetChannels = new ArrayList<>();
                for (ExecutionVertex t : edge.getTargets()) {
                    targetChannels.add(inputChannelOf.get(t));
                }
                outputs.add(new Output(targetChannels, edge.getPartitioner(), edge.getKeySelector()));
            }
        }
        return outputs;
    }

    private int countUpstreams(ExecutionVertex v, List<ExecutionEdge> edges) {
        int pending = 0;
        for (ExecutionEdge edge : edges) {
            if (edge.getTargets().contains(v)) {
                if (edge.getPartitioner() instanceof ForwardPartitioner) {
                    pending += 1; // forward 一对一：下游.i 只有一个上游
                } else {
                    pending += edge.getSources().size(); // fan-in：所有上游都会发
                }
            }
        }
        return pending;
    }

    /** 取 vertex 入边的 keySelector（keyed 算子非 null；单线性链每 vertex 最多一条入边）。 */
    private org.miniflink.api.function.KeySelector<?, ?> findInputKeySelector(
            ExecutionVertex v, List<ExecutionEdge> edges) {
        for (ExecutionEdge edge : edges) {
            if (edge.getTargets().contains(v)) {
                return edge.getKeySelector();
            }
        }
        return null;
    }
}
