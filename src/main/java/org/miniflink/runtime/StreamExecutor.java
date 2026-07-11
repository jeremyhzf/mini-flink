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
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 多线程执行器：为每条边按分区器建 per-(上游,下游) Channel（支持 barrier 对齐的 InputGate），
 * 为每个 vertex 建 Task，启动线程并 join。
 *
 * 支持自动 failover 重试循环（Task 11）：
 * 每轮（attempt）= buildTasks（冷启 / 从 checkpoint 恢复重建）→ runOnce（启动 + 周期 checkpoint
 * + 失败关闭）→ coord.stop → 正常结束 return / 任一失败取 lastCompletedCheckpoint →
 * 有 checkpoint 且未超 maxRestarts 则从 checkpoint 恢复重建重跑，否则抛失败。
 */
public class StreamExecutor {

    /** 兼容旧调用：无 checkpoint、不重启。 */
    public void execute(ExecutionGraph graph) throws Exception {
        execute(graph, Long.MAX_VALUE, 0);
    }

    /**
     * 执行作业（含周期 checkpoint 与自动 failover）。
     * @param graph 执行图
     * @param checkpointInterval checkpoint 周期（毫秒）；Long.MAX_VALUE=不启用
     * @param maxRestarts 最大重启次数（故障后从 checkpoint 恢复重跑的次数上限）
     */
    public void execute(ExecutionGraph graph, long checkpointInterval, int maxRestarts) throws Exception {
        List<String> snapshotKeys = collectSnapshotKeys(graph);   // 每个 subtask 一个 key（vertex.id）
        Checkpoint lastCp = null;
        Throwable lastFailure = null;   // 末轮失败 cause（达 maxRestarts 时随异常抛出，避免丢失）
        for (int attempt = 0; attempt <= maxRestarts; attempt++) {
            // 1. coordinator 先建（持有可变 source 列表引用），buildTasks 往里填 source task
            List<SourceTask> sourceTasks = new ArrayList<>();
            CheckpointCoordinator coordinator = new CheckpointCoordinator(
                    checkpointInterval, sourceTasks, snapshotKeys, 2);
            // 2. 构建 tasks（冷启 lastCp=null；恢复 lastCp=上轮 checkpoint）
            List<Task> tasks = buildTasks(graph, lastCp, coordinator, sourceTasks);
            // 3. 启动一轮 + 周期 checkpoint；返回首个失败（null=正常结束）
            Throwable failure = runOnce(tasks, coordinator);
            coordinator.stop();
            if (failure == null) {
                return;   // 正常结束
            }
            lastFailure = failure;   // 记录末轮失败 cause（达 maxRestarts 时随异常抛出）
            // 4. 失败路径：取最近完成的 checkpoint
            lastCp = coordinator.lastCompletedCheckpoint();
            if (lastCp == null) {
                // 无可用 checkpoint：直接抛（保持 FailureCloseTest 语义——无 checkpoint 时故障即失败）
                throw new RuntimeException("作业执行失败（无可用 checkpoint）", failure);
            }
            // 否则下一轮从 lastCp 恢复重建重跑
        }
        throw new RuntimeException("作业执行失败（已达 maxRestarts=" + maxRestarts + "）", lastFailure);
    }

    /** 收集全部 subtask 的 snapshotKey（每个 vertex 的 id）。 */
    private List<String> collectSnapshotKeys(ExecutionGraph graph) {
        List<String> keys = new ArrayList<>();
        for (ExecutionVertex v : graph.getVertices()) {
            keys.add(String.valueOf(v.getId()));
        }
        return keys;
    }

    /**
     * 构建 tasks：复用 per-pair channel + InputGate 逻辑；算子/source 用 copy() 取新实例；
     * 若 checkpoint != null，按 snapshotKey 取 SubtaskSnapshot 注入恢复参数。
     * @param checkpoint null=冷启；非 null=从该 checkpoint 恢复
     * @param coordinator 注入各 task（ack 用）；同时 source 列表填入 sourceTasksOut（coord 引用同一可变列表）
     */
    private List<Task> buildTasks(ExecutionGraph graph, Checkpoint checkpoint,
                                  CheckpointCoordinator coordinator, List<SourceTask> sourceTasksOut) {
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
            String snapshotKey = String.valueOf(v.getId());
            RuntimeContext ctx = new RuntimeContextImpl(
                    v.getSubtaskIndex(), v.getParallelism(), findInputKeySelector(v, graph.getEdges()));
            List<Output> outputs = buildOutputs(v, graph.getEdges(), pairChannels);
            SubtaskSnapshot sub = (checkpoint != null) ? checkpoint.getSnapshots().get(snapshotKey) : null;
            if (v.isSource()) {
                SourceOperator<?> srcOp = v.getSourceOperator().copy();
                long restoreOffset = (sub != null && sub.getSourceOffset() >= 0) ? sub.getSourceOffset() : -1L;
                SourceTask st = new SourceTask(srcOp, outputs, ctx, coordinator, snapshotKey, restoreOffset);
                sourceTasksOut.add(st);
                tasks.add(st);
            } else {
                List<InputChannel> inputs = incomingOf.getOrDefault(v, List.of());
                int pending = inputs.size();    // 每个上游 InputChannel 最终发 1 个 EOB
                List<Operator<?, ?>> ops = new ArrayList<>();
                for (Operator<?, ?> op : v.getOperators()) {
                    ops.add(op.copy());   // 每轮取新实例（open 写 per-subtask 状态，恢复需干净算子）
                }
                OperatorChain<?, ?> chain = new OperatorChain<>(ops);
                tasks.add(new OperatorTask(chain, inputs, pending, outputs, ctx,
                        coordinator, snapshotKey, sub));   // sub=null 冷启，非 null 恢复
            }
        }
        return tasks;
    }

    /**
     * 启动一轮：返回首个失败（null=正常结束）。任一 Task 未捕获异常 → 记录 cause 并中断其余
     * （失败关闭：解除其他 Task 在 Channel.receive()/send() 上的阻塞，避免 join 永等）。
     */
    private Throwable runOnce(List<Task> tasks, CheckpointCoordinator coordinator) throws InterruptedException {
        List<Thread> threads = new CopyOnWriteArrayList<>();
        AtomicReference<Throwable> error = new AtomicReference<>();
        coordinator.start();
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
        // join 等待全部（带超时兜底，避免极端情况下永久挂起）
        for (Thread th : threads) {
            th.join(30_000);
            if (th.isAlive()) {
                th.interrupt();
                th.join(5_000);
            }
        }
        return error.get();
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
