package org.miniflink.runtime;

import java.util.ArrayList;
import java.util.List;

/**
 * 一个 Task 内链化的算子序列 [op1(IN→?), op2, ..., opN(?→OUT)]。
 * 链内算子经 ChainCollector 直接函数调用（不经 Channel）；链尾算子输出到传入的 Collector（通常是 ChannelWriter）。
 * open 从尾到头接线：opN.open(output)；opN-1.open(ChainCollector(opN))；...。
 */
public class OperatorChain<IN, OUT> {
    private final List<Operator<?, ?>> operators;

    public OperatorChain(List<Operator<?, ?>> operators) {
        if (operators == null || operators.isEmpty()) {
            throw new IllegalArgumentException("OperatorChain 至少含一个算子");
        }
        this.operators = new ArrayList<>(operators);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public void open(Collector<OUT> output, RuntimeContext ctx) {
        Collector current = output;
        // 从尾到头：每个算子 open(它的输出)，然后它的输出 = ChainCollector(它)，供上游 open
        for (int i = operators.size() - 1; i >= 0; i--) {
            Operator op = operators.get(i);
            op.open(current, ctx);
            current = new ChainCollector(op);
        }
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public void processElement(IN record) throws Exception {
        // 链头算子处理，经 ChainCollector 级联到链尾 → output
        ((Operator) operators.get(0)).processElement(record);
    }

    public void close() {
        for (Operator<?, ?> op : operators) {
            op.close();
        }
    }
}
