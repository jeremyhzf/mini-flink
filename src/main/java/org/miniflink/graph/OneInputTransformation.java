package org.miniflink.graph;

import org.miniflink.runtime.Operator;

/** 单输入节点：持有一个处理算子及其上游 input。 */
public class OneInputTransformation<IN, OUT> extends Transformation<OUT> {
    private final Transformation<IN> input;
    private final Operator<IN, OUT> operator;

    public OneInputTransformation(int id, String name, Transformation<IN> input, Operator<IN, OUT> operator) {
        super(id, name);
        this.input = input;
        this.operator = operator;
    }

    public Transformation<IN> getInput() {
        return input;
    }

    public Operator<IN, OUT> getOperator() {
        return operator;
    }
}
