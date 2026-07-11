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
    private SourceContext<OUT> ctx;

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
}
