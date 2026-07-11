package org.miniflink.runtime;

import org.miniflink.execution.ExecutionEdge;
import org.miniflink.execution.ExecutionGraph;
import org.miniflink.execution.ExecutionVertex;
import org.miniflink.execution.ForwardPartitioner;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 多线程执行器：为每条边按分区器建 per-(上游,下游) Channel（支持 barrier 对齐的 InputGate），
 * 为每个 vertex 建 Task，启动线程并 join。任一 Task 未捕获异常 → 中断其余 Task（解锁阻塞的
 * receive/send）→ join 带超时兜底 → 干净失败抛 RuntimeException(cause)。
 */
public class StreamExecutor {

    public void execute(ExecutionGraph graph) throws Exception {
        // 1. per-pair channel：forward 同索引对；fan-out 全连接对。target 收集其各上游 InputChannel。
        Map<String, Channel> pairChannels = new HashMap<>();
        Map<ExecutionVertex, List<InputChannel>> incomingOf = new HashMap<>();
        for (ExecutionEdge edge : graph.getEdges()) {
            List<ExecutionVertex> srcs = edge.getSources();
            List<ExecutionVertex> tgts = edge.getTargets();
            boolean forward = edge.getPartitioner() instanceof ForwardPartitioner;
            if (forward) {
                for (int i = 0; i < srcs.size(); i++) {
                    ExecutionVertex s = srcs.get(i);
                    ExecutionVertex t = tgts.get(i);
                    Channel ch = new Channel();
                    pairChannels.put(pairKey(s, t), ch);
                    incomingOf.computeIfAbsent(t, k -> new ArrayList<>()).add(new InputChannel(ch));
                }
            } else {
                for (ExecutionVertex s : srcs) {
                    for (ExecutionVertex t : tgts) {
                        Channel ch = new Channel();
                        pairChannels.put(pairKey(s, t), ch);
                        incomingOf.computeIfAbsent(t, k -> new ArrayList<>()).add(new InputChannel(ch));
                    }
                }
            }
        }

        // 2. 为每个 vertex 建 RuntimeContext + Task
        List<Task> tasks = new ArrayList<>();
        for (ExecutionVertex v : graph.getVertices()) {
            RuntimeContext ctx = new RuntimeContextImpl(
                    v.getSubtaskIndex(), v.getParallelism(), findInputKeySelector(v, graph.getEdges()));
            List<Output> outputs = buildOutputs(v, graph.getEdges(), pairChannels);
            if (v.isSource()) {
                tasks.add(new SourceTask(v.getSourceOperator(), outputs, ctx));
            } else {
                List<InputChannel> inputs = incomingOf.getOrDefault(v, List.of());
                int pending = inputs.size();    // 每个上游 InputChannel 最终发 1 个 EOB
                tasks.add(new OperatorTask(new OperatorChain<>(v.getOperators()), inputs, pending, outputs, ctx));
            }
        }

        // 3. 启动所有线程：任一 Task 未捕获异常 → 记录 cause 并中断其余（失败关闭），
        //    解除其他 Task 在 Channel.receive()/send() 上的阻塞，避免 join 永等。
        List<Thread> threads = new ArrayList<>();
        AtomicReference<Throwable> error = new AtomicReference<>();
        for (Task t : tasks) {
            Thread th = new Thread(t, "miniflink-task-" + threads.size());
            th.setUncaughtExceptionHandler((tr, e) -> {
                error.compareAndSet(null, e);
                interruptOthers(threads, tr);   // 任一异常 → 中断其余
            });
            threads.add(th);
            th.start();
        }
        // 启动期已有 Task 失败的 race 补救：早启动的 Task 可能在后续 Task 尚未加入 threads 列表时
        // 就抛异常，此时上面的 interruptOthers 会漏掉那些尚未入列的线程（isAlive 守卫不到）。
        // 循环结束后列表已完整——若此时 error 已置位，补中断所有存活线程。
        if (error.get() != null) {
            for (Thread th : threads) {
                if (th.isAlive()) {
                    th.interrupt();
                }
            }
        }

        // 4. join 等待全部（带超时兜底，避免极端情况——如线程忽略中断——下永久挂起）
        for (Thread th : threads) {
            th.join(30_000);
            if (th.isAlive()) {
                // 兜底：仍存活则中断并再等
                th.interrupt();
                th.join(5_000);
            }
        }

        // 5. 异常传播
        if (error.get() != null) {
            throw new RuntimeException("作业执行失败", error.get());
        }
    }

    /** 中断除当前线程外的所有 task 线程（失败关闭：解锁阻塞的 receive/send）。
     *  只中断已 start 且存活的线程（th.isAlive()），避免漏中断未启动的线程或对未启动线程 interrupt。 */
    private void interruptOthers(List<Thread> threads, Thread current) {
        for (Thread th : threads) {
            if (th != current && th.isAlive()) {
                th.interrupt();
            }
        }
    }

    private List<Output> buildOutputs(ExecutionVertex v, List<ExecutionEdge> edges,
                                      Map<String, Channel> pairChannels) {
        List<Output> outputs = new ArrayList<>();
        for (ExecutionEdge edge : edges) {
            if (!edge.getSources().contains(v)) {
                continue;
            }
            List<ExecutionVertex> tgts = edge.getTargets();
            boolean forward = edge.getPartitioner() instanceof ForwardPartitioner;
            Channel[] chans = new Channel[tgts.size()];
            for (int k = 0; k < tgts.size(); k++) {
                ExecutionVertex t = tgts.get(k);
                boolean connected = forward ? (k == v.getSubtaskIndex()) : true;
                chans[k] = connected ? pairChannels.get(pairKey(v, t)) : null;
            }
            outputs.add(new Output(Arrays.asList(chans), edge.getPartitioner(), edge.getKeySelector()));
        }
        return outputs;
    }

    private static String pairKey(ExecutionVertex s, ExecutionVertex t) {
        return s.getId() + "->" + t.getId();
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
