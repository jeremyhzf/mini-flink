package org.miniflink.runtime.operator;

import org.junit.jupiter.api.Test;
import org.miniflink.api.function.KeySelector;
import org.miniflink.api.function.ReduceFunction;
import org.miniflink.runtime.ListCollector;
import org.miniflink.runtime.RuntimeContextImpl;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ReduceOperatorTest {

    /** 验证 reduce 算子按 key 分组累加并输出 running 结果。 */
    @Test
    void accumulatesByKeyAndEmitsRunningResult() throws Exception {
        // keySelector: x -> x % 2（按奇偶分组）；reduce: 求和
        ReduceOperator<Integer> op = new ReduceOperator<>((ReduceFunction<Integer>) (a, b) -> a + b);
        KeySelector<Integer, Integer> ks = x -> x % 2;
        RuntimeContextImpl ctx = new RuntimeContextImpl(0, 1, ks);
        ListCollector<Integer> out = new ListCollector<>();
        op.open(out, ctx);

        op.processElement(1); // key=1, acc=1, 输出 1
        op.processElement(3); // key=1, acc=1+3=4, 输出 4
        op.processElement(2); // key=0, acc=2, 输出 2
        op.processElement(4); // key=0, acc=2+4=6, 输出 6

        assertEquals(List.of(1, 4, 2, 6), out.getResult());
    }
}
