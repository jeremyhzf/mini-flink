package org.miniflink.runtime.operator;

import org.junit.jupiter.api.Test;
import org.miniflink.api.function.SinkFunction;
import org.miniflink.runtime.ListCollector;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SinkOperatorTest {

    @Test
    void 应把每个元素交给SinkFunction() throws Exception {
        List<String> sinked = new ArrayList<>();
        SinkOperator<String> op = new SinkOperator<>((SinkFunction<String>) sinked::add);
        op.open(new ListCollector<>()); // sink 不输出，给一个占位 Collector

        op.processElement("a");
        op.processElement("b");

        assertEquals(List.of("a", "b"), sinked);
    }
}
