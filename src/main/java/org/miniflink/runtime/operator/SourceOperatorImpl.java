package org.miniflink.runtime.operator;

import org.miniflink.api.function.SourceFunction;
import org.miniflink.runtime.Collector;
import org.miniflink.runtime.RuntimeContext;
import org.miniflink.runtime.SourceContext;
import org.miniflink.runtime.SourceContextImpl;
import org.miniflink.runtime.SourceOperator;

/** 包装 SourceFunction 的 source 算子：open 建 SourceContextImpl（带并行位置），run 调用用户函数。 */
public class SourceOperatorImpl<OUT> implements SourceOperator<OUT> {
    private final SourceFunction<OUT> sourceFunction;
    private volatile SourceContext<OUT> ctx;   // daemon 协调器线程读 / source task 线程写 → volatile 保可见性

    public SourceOperatorImpl(SourceFunction<OUT> sourceFunction) {
        this.sourceFunction = sourceFunction;
    }

    @Override
    public void open(Collector<OUT> out, RuntimeContext ctx) {
        this.ctx = new SourceContextImpl<>(out, ctx.getSubtaskIndex(), ctx.getParallelism());
    }

    @Override
    public void run() throws Exception {
        sourceFunction.run(ctx);
    }

    @Override
    public void close() {
        // 阶段②无需操作
    }

    @Override
    public SourceOperatorImpl<OUT> copy() {
        return new SourceOperatorImpl<>(sourceFunction);
    }

    @Override
    public long snapshotOffset() {
        return ctx.snapshotOffset();
    }

    @Override
    public void restoreOffset(long offset) {
        ctx.restoreOffset(offset);
    }

    @Override
    public void requestCheckpoint(long checkpointId) {
        // coordinator daemon 可能在 source.open() 之前就触发（线程启动竞态）；此时 ctx 尚未建立，
        // 忽略本轮（下一个周期 source 就绪后会被处理），避免 NPE 杀死协调器线程导致永无 checkpoint。
        if (ctx != null) {
            ctx.requestCheckpoint(checkpointId);
        }
    }

    @Override
    public void setCheckpointEmitter(SourceContextImpl.CheckpointEmitter emitter) {
        ((SourceContextImpl<OUT>) ctx).setCheckpointEmitter(emitter);
    }

    /** 暴露内部 ctx（SourceTask 配置 emitter / drainPending 用）。 */
    public SourceContextImpl<OUT> getSourceContext() { return (SourceContextImpl<OUT>) ctx; }
}
