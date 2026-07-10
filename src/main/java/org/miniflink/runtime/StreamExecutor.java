package org.miniflink.runtime;

import org.miniflink.execution.ExecutionGraph;

import java.util.List;

/**
 * 阶段①同步执行器：把 source 与算子链用 OperatorOutput 串成同步链，
 * open 全部算子后调用 source.run() 触发数据流动，最后统一 close。
 *
 * 注：为避免泛型噪声，链组装使用 raw type + 受检转换；阶段②引入多线程时会重构为 Task。
 */
public class StreamExecutor {

    public void execute(ExecutionGraph graph) throws Exception {
        @SuppressWarnings("rawtypes")
        SourceOperator source = graph.getSource();
        List<Operator<?, ?>> operators = graph.getOperators();
        int n = operators.size();

        // 为每个算子准备它的输出 Collector：
        // 最后一个算子（sink）→ NoopCollector；其余 → OperatorOutput(下一个算子)
        @SuppressWarnings("rawtypes")
        Collector[] outputs = new Collector[n];
        for (int i = n - 1; i >= 0; i--) {
            if (i == n - 1) {
                outputs[i] = new NoopCollector<>();
            } else {
                outputs[i] = new OperatorOutput<>(operators.get(i + 1));
            }
        }

        // open 所有算子（按正序）
        for (int i = 0; i < n; i++) {
            @SuppressWarnings({"rawtypes", "unchecked"})
            Operator op = operators.get(i);
            op.open(outputs[i]);
        }

        // source 的输出连到第一个算子（链为空时丢弃）
        @SuppressWarnings("rawtypes")
        Collector sourceOut = (n == 0) ? new NoopCollector<>() : new OperatorOutput(operators.get(0));
        @SuppressWarnings("unchecked")
        Collector<Object> typedSourceOut = (Collector<Object>) sourceOut;
        source.open(typedSourceOut, 0, 1);

        try {
            source.run();
        } finally {
            // 关闭：source、所有算子、所有输出
            source.close();
            for (Operator<?, ?> op : operators) {
                op.close();
            }
            for (int i = 0; i < n; i++) {
                outputs[i].close();
            }
            typedSourceOut.close();
        }
    }
}
