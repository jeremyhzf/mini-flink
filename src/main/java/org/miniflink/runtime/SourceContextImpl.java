package org.miniflink.runtime;

/** 把 SourceContext.collect 转发到下游 Collector；持有 subtask 位置供分片；emitted/skipUntil 实现断点重放。 */
public class SourceContextImpl<T> implements SourceContext<T> {
    private final Collector<T> out;
    private final int subtaskIndex;
    private final int parallelism;
    private long emitted = 0;       // 已转发数（snapshot 用）
    private long skipUntil = 0;     // 恢复时跳过前 N 条（重放用）

    public SourceContextImpl(Collector<T> out, int subtaskIndex, int parallelism) {
        this.out = out;
        this.subtaskIndex = subtaskIndex;
        this.parallelism = parallelism;
    }

    @Override
    public void collect(T record) {
        if (emitted < skipUntil) {
            emitted++;          // 恢复重放：丢弃已发条数
            return;
        }
        emitted++;
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

    @Override
    public long snapshotOffset() {
        return emitted;
    }

    @Override
    public void restoreOffset(long offset) {
        this.skipUntil = offset;
        this.emitted = 0;
    }
}
