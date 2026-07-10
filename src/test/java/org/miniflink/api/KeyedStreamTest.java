package org.miniflink.api;

import org.junit.jupiter.api.Test;
import org.miniflink.api.function.KeySelector;
import org.miniflink.api.function.ReduceFunction;
import org.miniflink.execution.HashPartitioner;
import org.miniflink.graph.OneInputTransformation;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class KeyedStreamTest {

    @Test
    void keyBy返回KeyedStream且reduce建hash分区transformation() {
        StreamExecutionEnvironment env = new StreamExecutionEnvironment();
        KeyedStream<Integer, Integer> keyed = env.fromCollection(List.of(1, 2, 3))
                .keyBy((KeySelector<Integer, Integer>) x -> x);
        DataStream<Integer> reduced = keyed.reduce((ReduceFunction<Integer>) (a, b) -> a + b);

        OneInputTransformation<?, ?> tx = (OneInputTransformation<?, ?>) reduced.getTransformation();
        assertEquals("reduce", tx.getName());
        assertInstanceOf(HashPartitioner.class, tx.getPartitioner());
        assertNotNull(tx.getKeySelector());
    }
}
