package org.miniflink.execution;

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
 * 物理执行计划。阶段①：单线性链（一个 sink），从 sink 回溯到 source，
 * 产出 source + 正向算子序列（source 之后：op1, op2, ..., sink）。
 */
public class ExecutionGraph {
    private final SourceOperator<?> source;
    private final List<Operator<?, ?>> operators;

    public ExecutionGraph(SourceOperator<?> source, List<Operator<?, ?>> operators) {
        this.source = source;
        this.operators = operators;
    }

    public SourceOperator<?> getSource() {
        return source;
    }

    public List<Operator<?, ?>> getOperators() {
        return operators;
    }

    /** 从 StreamGraph 构建单线性链。 */
    public static ExecutionGraph from(StreamGraph streamGraph) {
        List<Transformation<?>> sinks = streamGraph.getSinks();
        if (sinks.size() != 1) {
            throw new IllegalStateException("阶段①仅支持单个 sink，当前 sinks=" + sinks.size());
        }

        List<Operator<?, ?>> chain = new ArrayList<>();
        Transformation<?> current = sinks.get(0);
        while (current instanceof OneInputTransformation<?, ?> oneInput) {
            chain.add(oneInput.getOperator());
            current = oneInput.getInput();
        }
        if (!(current instanceof SourceTransformation<?> sourceTx)) {
            throw new IllegalStateException("链回溯未终止于 source 节点");
        }

        // chain 当前顺序：[sink 算子, ..., 第一个算子]，反转后正向
        Collections.reverse(chain);
        return new ExecutionGraph(sourceTx.getOperator(), chain);
    }
}
