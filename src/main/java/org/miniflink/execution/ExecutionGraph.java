package org.miniflink.execution;

import org.miniflink.api.function.KeySelector;
import org.miniflink.graph.OneInputTransformation;
import org.miniflink.graph.SourceTransformation;
import org.miniflink.graph.StreamGraph;
import org.miniflink.graph.Transformation;
import org.miniflink.runtime.Operator;
import org.miniflink.runtime.SourceOperator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 物理执行计划：vertices（subtask）+ edges（组间边）。
 * from：回溯单线性链 → 链化分组（forward 同并行度合并）→ 按 parallelism 展开 → 组间边。
 * 单 sink 约束（与阶段①一致）。
 */
public class ExecutionGraph {
    private final List<ExecutionVertex> vertices;
    private final List<ExecutionEdge> edges;

    public ExecutionGraph(List<ExecutionVertex> vertices, List<ExecutionEdge> edges) {
        this.vertices = vertices;
        this.edges = edges;
    }

    public List<ExecutionVertex> getVertices() {
        return vertices;
    }

    public List<ExecutionEdge> getEdges() {
        return edges;
    }

    public static ExecutionGraph from(StreamGraph streamGraph) {
        List<Transformation<?>> sinks = streamGraph.getSinks();
        if (sinks.size() != 1) {
            throw new IllegalStateException("阶段②仅支持单个 sink，当前 sinks=" + sinks.size());
        }

        // 回溯得 sequence [sink.., source]，反转后 [source, op1, ..., sink]
        List<Transformation<?>> seq = new ArrayList<>();
        Transformation<?> cur = sinks.get(0);
        while (cur instanceof OneInputTransformation<?, ?> one) {
            seq.add(one);
            cur = one.getInput();
        }
        if (!(cur instanceof SourceTransformation<?> srcTx)) {
            throw new IllegalStateException("链回溯未终止于 source 节点");
        }
        seq.add(srcTx);
        Collections.reverse(seq);

        // 链化分组：source 单独一组；处理算子连续 forward 同并行度合并
        List<Group> groups = new ArrayList<>();
        groups.add(new Group(true, srcTx.getOperator(), new ArrayList<>(), srcTx.getParallelism(), null, null));

        List<Operator<?, ?>> curOps = new ArrayList<>();
        int curParallelism = -1;
        Partitioner curPart = null;
        KeySelector<?, ?> curKey = null;
        for (int i = 1; i < seq.size(); i++) {
            OneInputTransformation<?, ?> tx = (OneInputTransformation<?, ?>) seq.get(i);
            Partitioner part = tx.getPartitioner();
            KeySelector<?, ?> key = tx.getKeySelector();
            int p = tx.getParallelism();
            boolean canChain = !curOps.isEmpty() && (part instanceof ForwardPartitioner) && p == curParallelism;
            if (canChain) {
                curOps.add(tx.getOperator());
            } else {
                if (!curOps.isEmpty()) {
                    groups.add(new Group(false, null, new ArrayList<>(curOps), curParallelism, curPart, curKey));
                }
                curOps = new ArrayList<>();
                curOps.add(tx.getOperator());
                curParallelism = p;
                curPart = part;
                curKey = key;
            }
        }
        if (!curOps.isEmpty()) {
            groups.add(new Group(false, null, new ArrayList<>(curOps), curParallelism, curPart, curKey));
        }

        // 按 parallelism 展开
        int id = 0;
        List<List<ExecutionVertex>> groupVerts = new ArrayList<>();
        List<ExecutionVertex> allVerts = new ArrayList<>();
        for (Group g : groups) {
            List<ExecutionVertex> verts = new ArrayList<>();
            for (int i = 0; i < g.parallelism; i++) {
                ExecutionVertex v = g.isSource
                        ? new ExecutionVertex(id++, i, g.parallelism, List.of(), g.source)
                        : new ExecutionVertex(id++, i, g.parallelism, g.operators, null);
                verts.add(v);
                allVerts.add(v);
            }
            groupVerts.add(verts);
        }

        // 组间边；forward 但并行度不同时自动改 rebalance
        List<ExecutionEdge> edges = new ArrayList<>();
        for (int g = 1; g < groups.size(); g++) {
            List<ExecutionVertex> srcs = groupVerts.get(g - 1);
            List<ExecutionVertex> tgts = groupVerts.get(g);
            Partitioner part = groups.get(g).inputPartitioner;
            if (part instanceof ForwardPartitioner && srcs.size() != tgts.size()) {
                part = new RebalancePartitioner();
            }
            edges.add(new ExecutionEdge(srcs, tgts, part, groups.get(g).inputKeySelector));
        }

        return new ExecutionGraph(allVerts, edges);
    }

    /** 链化分组中间结构。 */
    private static final class Group {
        final boolean isSource;
        final SourceOperator<?> source;
        final List<Operator<?, ?>> operators;
        final int parallelism;
        final Partitioner inputPartitioner; // 该组入边（与上一组）的分区器
        final KeySelector<?, ?> inputKeySelector;

        Group(boolean isSource, SourceOperator<?> source, List<Operator<?, ?>> operators,
              int parallelism, Partitioner inputPartitioner, KeySelector<?, ?> inputKeySelector) {
            this.isSource = isSource;
            this.source = source;
            this.operators = operators;
            this.parallelism = parallelism;
            this.inputPartitioner = inputPartitioner;
            this.inputKeySelector = inputKeySelector;
        }
    }
}
