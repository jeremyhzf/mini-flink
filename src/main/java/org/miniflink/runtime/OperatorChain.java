package org.miniflink.runtime;

import java.util.ArrayList;
import java.util.List;
import org.miniflink.state.OperatorState;

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

    @SuppressWarnings({"rawtypes", "unchecked"})
    public void onWatermark(Watermark watermark) {
        // 链内每个算子都收到 watermark（链化算子共享线程，watermark 流过链）
        for (Operator<?, ?> op : operators) {
            ((Operator) op).onWatermark(watermark);
        }
    }

    /** 收集链内各算子的快照（按算子索引）。 */
    @SuppressWarnings({"rawtypes", "unchecked"})
    public java.util.Map<Integer, OperatorState> snapshotState() {
        java.util.Map<Integer, OperatorState> states = new java.util.LinkedHashMap<>();
        for (int i = 0; i < operators.size(); i++) {
            int idx = i;   // lambda 需 effectively final
            java.util.Optional<OperatorState> s = ((Operator) operators.get(i)).snapshotState();
            s.ifPresent(st -> states.put(idx, st));
        }
        return states;
    }

    /** 按算子索引恢复链内算子状态。 */
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void restoreState(java.util.Map<Integer, OperatorState> states) {
        for (var e : states.entrySet()) {
            ((Operator) operators.get(e.getKey())).restoreState(e.getValue());
        }
    }
}
