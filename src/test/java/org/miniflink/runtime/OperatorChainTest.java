package org.miniflink.runtime;

import org.junit.jupiter.api.Test;
import org.miniflink.api.function.FilterFunction;
import org.miniflink.api.function.MapFunction;
import org.miniflink.runtime.operator.FilterOperator;
import org.miniflink.runtime.operator.MapOperator;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class OperatorChainTest {

    /** 链化算子应在链内 forward 传递并仅在链尾输出。 */
    @Test
    void chainedOperatorsForwardWithinChainAndEmitAtTail() throws Exception {
        // map(x -> x+1) -> filter(x > 2)，链尾输出到 ListCollector
        OperatorChain<Integer, Integer> chain = new OperatorChain<>(List.of(
                new MapOperator<>((MapFunction<Integer, Integer>) x -> x + 1),
                new FilterOperator<>((FilterFunction<Integer>) x -> x > 2)
        ));
        ListCollector<Integer> out = new ListCollector<>();
        chain.open(out, new RuntimeContextImpl(0, 1, null));

        chain.processElement(1); // 1->2，2>2 false，丢弃
        chain.processElement(2); // 2->3，3>2 true，输出 3
        chain.processElement(3); // 3->4，4>2 true，输出 4

        assertEquals(List.of(3, 4), out.getResult());
    }

    /** 空算子列表应在构造时被拒绝。 */
    @Test
    void emptyOperatorListRejectedAtConstruction() {
        assertThrows(IllegalArgumentException.class, () -> new OperatorChain<Object, Object>(List.of()));
    }
}
