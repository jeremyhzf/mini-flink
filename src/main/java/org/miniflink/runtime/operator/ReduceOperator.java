package org.miniflink.runtime.operator;

import org.miniflink.api.function.KeySelector;
import org.miniflink.api.function.ReduceFunction;
import org.miniflink.runtime.Collector;
import org.miniflink.runtime.Operator;
import org.miniflink.runtime.RuntimeContext;
import org.miniflink.runtime.ValueState;

/**
 * keyed 聚合算子：open 时从 RuntimeContext 取 ValueState（累加器）；
 * processElement 设 currentKey，reduce(当前累加, 输入) 更新 state 并输出 running 结果。
 */
public class ReduceOperator<IN> implements Operator<IN, IN> {
    private final ReduceFunction<IN> reduceFn;
    private Collector<IN> out;
    private RuntimeContext ctx;
    private ValueState<IN> acc;
    private KeySelector<IN, ?> keySelector;

    public ReduceOperator(ReduceFunction<IN> reduceFn) {
        this.reduceFn = reduceFn;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Override
    public void open(Collector<IN> out, RuntimeContext ctx) {
        this.out = out;
        this.ctx = ctx;
        this.acc = ctx.getStateBackend().getValueState("reduce-acc");
        this.keySelector = (KeySelector<IN, ?>) ctx.getKeySelector();
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Override
    public void processElement(IN record) throws Exception {
        Object key = keySelector.getKey(record);
        ctx.setCurrentKey(key);
        IN current = acc.value();
        IN reduced = (current == null) ? record : reduceFn.reduce(current, record);
        acc.update(reduced);
        out.collect(reduced);
    }

    @Override
    public void close() {
        // 阶段③无需操作
    }

    @Override
    public ReduceOperator<IN> copy() {
        // 共享无状态的 ReduceFunction；acc 在 open 时从 per-subtask RuntimeContext 获取
        return new ReduceOperator<>(reduceFn);
    }
}
