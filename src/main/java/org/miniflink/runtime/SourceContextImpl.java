package org.miniflink.runtime;

/** 把 SourceContext.collect 转发到下游 Collector；持有 subtask 位置供分片；emitted/skipUntil 实现断点重放。 */
public class SourceContextImpl<T> implements SourceContext<T> {
    private final Collector<T> out;
    private final int subtaskIndex;
    private final int parallelism;
    private long emitted = 0;       // 已转发数（snapshot 用）
    private long skipUntil = 0;     // 恢复时跳过前 N 条（重放用）

    private volatile long requestedBarrierId = -1;
    /** 源线程 checkpoint 钩子：emit(id, offset) 在源线程于 record 边界被调用（snapshot+ack+发 barrier）。 */
    private CheckpointEmitter checkpointEmitter;

    public interface CheckpointEmitter {
        void emit(long checkpointId, long offset) throws Exception;
    }

    public SourceContextImpl(Collector<T> out, int subtaskIndex, int parallelism) {
        this.out = out;
        this.subtaskIndex = subtaskIndex;
        this.parallelism = parallelism;
    }

    public void setCheckpointEmitter(CheckpointEmitter emitter) {
        this.checkpointEmitter = emitter;
    }

    /** coordinator 置标志（跨线程，volatile）；真正处理在源线程 collect()。 */
    @Override
    public void requestCheckpoint(long checkpointId) {
        this.requestedBarrierId = checkpointId;
    }

    @Override
    public void collect(T record) {
        try {
            if (requestedBarrierId >= 0 && checkpointEmitter != null) {
                long id = requestedBarrierId;
                requestedBarrierId = -1;
                // skip 期（emitted<skipUntil）报 skipUntil（绝对坐标）与下游 reduce.acc 同坐标系，
                // 避免恢复 skip 窗口二次故障导致 records 0..skipUntil-1 双重累加；
                // 冷启（skipUntil=0）/正常后（emitted>=skipUntil）max 取 emitted，行为不变。
                checkpointEmitter.emit(id, Math.max(emitted, skipUntil));
            }
        } catch (Exception e) {
            throw new RuntimeException("source checkpoint 处理异常", e);
        }
        if (emitted < skipUntil) {
            emitted++;          // 恢复重放：丢弃已发条数
            return;
        }
        emitted++;
        out.collect(record);
    }

    /** source.run() 返回后、+∞ watermark 前，若仍有 pending（最后一条之后的触发）补一次（offset=emitted 已全部）。 */
    public void drainPending() {
        try {
            if (requestedBarrierId >= 0 && checkpointEmitter != null) {
                long id = requestedBarrierId;
                requestedBarrierId = -1;
                checkpointEmitter.emit(id, Math.max(emitted, skipUntil));   // 同 collect：skip 期报绝对 offset
            }
        } catch (Exception e) {
            throw new RuntimeException("source checkpoint drain 异常", e);
        }
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
