package org.miniflink.api;

import org.junit.jupiter.api.Test;
import org.miniflink.api.function.KeySelector;
import org.miniflink.api.function.ReduceFunction;
import org.miniflink.execution.ForwardPartitioner;
import org.miniflink.execution.HashPartitioner;
import org.miniflink.graph.OneInputTransformation;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class DataStreamKeyByTest {

    @Test
    void keyBy后reduce使用hash分区() {
        StreamExecutionEnvironment env = new StreamExecutionEnvironment();
        DataStream<Integer> reduced = env.fromCollection(List.of(1, 2, 3))
                .keyBy((KeySelector<Integer, Integer>) x -> x)
                .reduce((ReduceFunction<Integer>) (a, b) -> a + b);
        OneInputTransformation<?, ?> tx = (OneInputTransformation<?, ?>) reduced.getTransformation();
        assertInstanceOf(HashPartitioner.class, tx.getPartitioner());
        assertNotNull(tx.getKeySelector());
    }

    @Test
    void reduce后的普通算子恢复forward() {
        StreamExecutionEnvironment env = new StreamExecutionEnvironment();
        DataStream<Integer> s = env.fromCollection(List.of(1, 2, 3))
                .keyBy((KeySelector<Integer, Integer>) x -> x)
                .reduce((ReduceFunction<Integer>) (a, b) -> a + b);
        DataStream<Integer> mapped = s.map(x -> x);
        OneInputTransformation<?, ?> tx = (OneInputTransformation<?, ?>) mapped.getTransformation();
        assertInstanceOf(ForwardPartitioner.class, tx.getPartitioner());
    }

    @Test
    void setParallelism应设到transformation() {
        StreamExecutionEnvironment env = new StreamExecutionEnvironment();
        DataStream<Integer> src = env.fromCollection(List.of(1, 2, 3)).setParallelism(2);
        assertEquals(2, src.getTransformation().getParallelism());
    }
}
