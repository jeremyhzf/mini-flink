package org.miniflink.runtime.operator;

import org.junit.jupiter.api.Test;
import org.miniflink.api.function.FilterFunction;
import org.miniflink.runtime.ListCollector;
import org.miniflink.runtime.RuntimeContextImpl;

import static org.junit.jupiter.api.Assertions.assertEquals;

class FilterOperatorTest {

    @Test
    void 仅转发满足过滤条件的元素() throws Exception {
        FilterOperator<Integer> op = new FilterOperator<>((FilterFunction<Integer>) x -> x % 2 == 0);
        ListCollector<Integer> downstream = new ListCollector<>();
        op.open(downstream, new RuntimeContextImpl(0, 1, null));

        op.processElement(1);
        op.processElement(2);
        op.processElement(3);
        op.processElement(4);

        assertEquals(java.util.List.of(2, 4), downstream.getResult());
    }
}
