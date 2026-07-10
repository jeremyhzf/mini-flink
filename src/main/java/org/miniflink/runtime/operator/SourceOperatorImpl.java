package org.miniflink.runtime.operator;

import org.miniflink.api.function.SourceFunction;
import org.miniflink.runtime.Collector;
import org.miniflink.runtime.SourceContext;
import org.miniflink.runtime.SourceContextImpl;
import org.miniflink.runtime.SourceOperator;

/** 包装 SourceFunction 的 source 算子：run 时把 SourceContext 交给用户函数。 */
public class SourceOperatorImpl<OUT> implements SourceOperator<OUT> {
    private final SourceFunction<OUT> sourceFunction;
    private SourceContext<OUT> ctx;

    public SourceOperatorImpl(SourceFunction<OUT> sourceFunction) {
        this.sourceFunction = sourceFunction;
    }

    @Override
    public void open(Collector<OUT> out) {
        this.ctx = new SourceContextImpl<>(out);
    }

    @Override
    public void run() throws Exception {
        sourceFunction.run(ctx);
    }

    @Override
    public void close() {
        // 阶段①无需操作
    }
}
