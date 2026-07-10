package org.miniflink.runtime;

/** 把 SourceContext.collect 转发到下游 Collector；持有 subtask 位置供分片。 */
public class SourceContextImpl<T> implements SourceContext<T> {
    private final Collector<T> out;
    private final int subtaskIndex;
    private final int parallelism;

    public SourceContextImpl(Collector<T> out, int subtaskIndex, int parallelism) {
        this.out = out;
        this.subtaskIndex = subtaskIndex;
        this.parallelism = parallelism;
    }

    @Override
    public void collect(T record) {
        out.collect(record);
    }

    @Override
    public int getSubtaskIndex() {
        return subtaskIndex;
    }

    @Override
    public int getParallelism() {
        return parallelism;
    }
}
