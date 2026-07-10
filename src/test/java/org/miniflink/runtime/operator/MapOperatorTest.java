package org.miniflink.runtime.operator;

import org.junit.jupiter.api.Test;
import org.miniflink.api.function.MapFunction;
import org.miniflink.runtime.ListCollector;
import org.miniflink.runtime.RuntimeContextImpl;

import static org.junit.jupiter.api.Assertions.assertEquals;

class MapOperatorTest {

    @Test
    void 应将输入映射为输出并收集到下游() throws Exception {
        // 输入 1,2,3 → 输出 2,4,6（乘 2）
        MapOperator<Integer, Integer> op = new MapOperator<>((MapFunction<Integer, Integer>) x -> x * 2);
        ListCollector<Integer> downstream = new ListCollector<>();
        op.open(downstream, new RuntimeContextImpl(0, 1, null));

        op.processElement(1);
        op.processElement(2);
        op.processElement(3);

        assertEquals(java.util.List.of(2, 4, 6), downstream.getResult());
    }
}
