package org.miniflink.graph;

import org.miniflink.api.function.KeySelector;
import org.miniflink.execution.ForwardPartitioner;
import org.miniflink.execution.Partitioner;
import org.miniflink.runtime.Operator;

/** 单输入节点：持有一个处理算子及其上游 input，并记录其入边的分区策略。 */
public class OneInputTransformation<IN, OUT> extends Transformation<OUT> {
    private final Transformation<IN> input;
    private final Operator<IN, OUT> operator;
    private final Partitioner partitioner;
    private final KeySelector<?, ?> keySelector;

    /** 默认 forward 分区（向后兼容阶段①调用）。 */
    public OneInputTransformation(int id, String name, Transformation<IN> input, Operator<IN, OUT> operator) {
        this(id, name, input, operator, new ForwardPartitioner(), null);
    }

    public OneInputTransformation(int id, String name, Transformation<IN> input, Operator<IN, OUT> operator,
                                  Partitioner partitioner, KeySelector<?, ?> keySelector) {
        super(id, name);
        this.input = input;
        this.operator = operator;
        this.partitioner = partitioner;
        this.keySelector = keySelector;
    }

    public Transformation<IN> getInput() {
        return input;
    }

    public Operator<IN, OUT> getOperator() {
        return operator;
    }

    public Partitioner getPartitioner() {
        return partitioner;
    }

    public KeySelector<?, ?> getKeySelector() {
        return keySelector;
    }
}
