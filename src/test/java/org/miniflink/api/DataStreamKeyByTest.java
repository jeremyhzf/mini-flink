package org.miniflink.api;

import org.junit.jupiter.api.Test;
import org.miniflink.api.function.KeySelector;
import org.miniflink.execution.HashPartitioner;
import org.miniflink.graph.OneInputTransformation;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class DataStreamKeyByTest {

    @Test
    void keyBy应使下一个算子入边用hash分区() {
        StreamExecutionEnvironment env = new StreamExecutionEnvironment();
        DataStream<Integer> keyed = env.fromCollection(List.of(1, 2, 3)).keyBy((KeySelector<Integer, Integer>) x -> x);
        DataStream<Integer> mapped = keyed.map(x -> x);

        OneInputTransformation<?, ?> mapTx = (OneInputTransformation<?, ?>) mapped.getTransformation();
        assertInstanceOf(HashPartitioner.class, mapTx.getPartitioner());
        assertNotNull(mapTx.getKeySelector());
    }

    @Test
    void keyBy后非keyBy算子应恢复forward() {
        StreamExecutionEnvironment env = new StreamExecutionEnvironment();
        DataStream<Integer> s = env.fromCollection(List.of(1, 2, 3)).keyBy((KeySelector<Integer, Integer>) x -> x).map(x -> x);
        DataStream<Integer> m2 = s.map(x -> x); // 不再 keyBy

        OneInputTransformation<?, ?> tx = (OneInputTransformation<?, ?>) m2.getTransformation();
        assertInstanceOf(org.miniflink.execution.ForwardPartitioner.class, tx.getPartitioner());
    }

    @Test
    void setParallelism应设到transformation() {
        StreamExecutionEnvironment env = new StreamExecutionEnvironment();
        DataStream<Integer> src = env.fromCollection(List.of(1, 2, 3)).setParallelism(2);
        assertEquals(2, src.getTransformation().getParallelism());
    }
}
