package org.miniflink.execution;

import org.miniflink.runtime.Operator;
import org.miniflink.runtime.SourceOperator;

import java.util.List;
import java.util.Objects;

/**
 * 物理执行计划的 subtask。
 * source vertex：operators 为空、sourceOperator 非 null。
 * 处理 vertex：operators 为链化算子序列、sourceOperator 为 null。
 */
public class ExecutionVertex {
    private final int id;
    private final int subtaskIndex;
    private final int parallelism;
    private final List<Operator<?, ?>> operators;
    private final SourceOperator<?> sourceOperator;

    public ExecutionVertex(int id, int subtaskIndex, int parallelism,
                           List<Operator<?, ?>> operators, SourceOperator<?> sourceOperator) {
        this.id = id;
        this.subtaskIndex = subtaskIndex;
        this.parallelism = parallelism;
        this.operators = operators;
        this.sourceOperator = sourceOperator;
    }

    public boolean isSource() {
        return sourceOperator != null;
    }

    public int getId() {
        return id;
    }

    public int getSubtaskIndex() {
        return subtaskIndex;
    }

    public int getParallelism() {
        return parallelism;
    }

    public List<Operator<?, ?>> getOperators() {
        return operators;
    }

    public SourceOperator<?> getSourceOperator() {
        return sourceOperator;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ExecutionVertex that = (ExecutionVertex) o;
        return id == that.id;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }
}
