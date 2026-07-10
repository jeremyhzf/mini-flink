package org.miniflink.graph;

import org.miniflink.runtime.SourceOperator;

/** source 节点：持有 SourceOperator。 */
public class SourceTransformation<T> extends Transformation<T> {
    private final SourceOperator<T> operator;

    public SourceTransformation(int id, String name, SourceOperator<T> operator) {
        super(id, name);
        this.operator = operator;
    }

    public SourceOperator<T> getOperator() {
        return operator;
    }
}
