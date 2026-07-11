package org.miniflink.runtime.operator;

import org.junit.jupiter.api.Test;
import org.miniflink.api.function.FlatMapFunction;
import org.miniflink.runtime.ListCollector;
import org.miniflink.runtime.RuntimeContextImpl;

import static org.junit.jupiter.api.Assertions.assertEquals;

class FlatMapOperatorTest {

    /** 验证 flatMap 算子一条输入可产生零或多条输出。 */
    @Test
    void singleInputMayProduceZeroOrMoreOutputs() throws Exception {
        // 把句子拆成单词
        FlatMapOperator<String, String> op = new FlatMapOperator<>(
                (FlatMapFunction<String, String>) (line, out) -> {
                    for (String word : line.split(" ")) {
                        out.collect(word);
                    }
                });
        ListCollector<String> downstream = new ListCollector<>();
        op.open(downstream, new RuntimeContextImpl(0, 1, null));

        op.processElement("hello world");
        op.processElement("mini flink");

        assertEquals(java.util.List.of("hello", "world", "mini", "flink"), downstream.getResult());
    }
}
