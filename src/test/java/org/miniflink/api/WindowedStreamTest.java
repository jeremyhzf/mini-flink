package org.miniflink.api;

import org.junit.jupiter.api.Test;
import org.miniflink.api.function.KeySelector;
import org.miniflink.api.function.ReduceFunction;
import org.miniflink.execution.HashPartitioner;
import org.miniflink.graph.OneInputTransformation;
import org.miniflink.runtime.operator.WindowOperator;
import org.miniflink.window.TumblingEventTimeWindows;

import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

class WindowedStreamTest {

    @Test
    void window后reduce建WindowOperator的hash分区transformation() {
        StreamExecutionEnvironment env = new StreamExecutionEnvironment();
        DataStream<Integer> reduced = env.fromCollection(List.of(1, 2, 3))
                .keyBy((KeySelector<Integer, Integer>) x -> x)
                .window(TumblingEventTimeWindows.of(java.time.Duration.ofSeconds(1)))
                .reduce((ReduceFunction<Integer>) (a, b) -> a + b);
        OneInputTransformation<?, ?> tx = (OneInputTransformation<?, ?>) reduced.getTransformation();
        assertEquals("window-reduce", tx.getName());
        assertInstanceOf(HashPartitioner.class, tx.getPartitioner());
        assertNotNull(tx.getKeySelector());
        assertInstanceOf(WindowOperator.class, tx.getOperator());
    }
}
