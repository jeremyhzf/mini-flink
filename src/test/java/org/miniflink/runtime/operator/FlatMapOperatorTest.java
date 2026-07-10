package org.miniflink.runtime.operator;

import org.junit.jupiter.api.Test;
import org.miniflink.api.function.FlatMapFunction;
import org.miniflink.runtime.ListCollector;

import static org.junit.jupiter.api.Assertions.assertEquals;

class FlatMapOperatorTest {

    @Test
    void 一条输入可产生零或多条输出() throws Exception {
        // 把句子拆成单词
        FlatMapOperator<String, String> op = new FlatMapOperator<>(
                (FlatMapFunction<String, String>) (line, out) -> {
                    for (String word : line.split(" ")) {
                        out.collect(word);
                    }
                });
        ListCollector<String> downstream = new ListCollector<>();
        op.open(downstream);

        op.processElement("hello world");
        op.processElement("mini flink");

        assertEquals(java.util.List.of("hello", "world", "mini", "flink"), downstream.getResult());
    }
}
