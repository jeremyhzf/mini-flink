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

    /** keyBy 后的 reduce 使用 hash 分区并携带 keySelector。 */
    @Test
    void keyByThenReduceUsesHashPartitioning() {
        StreamExecutionEnvironment env = new StreamExecutionEnvironment();
        DataStream<Integer> reduced = env.fromCollection(List.of(1, 2, 3))
                .keyBy((KeySelector<Integer, Integer>) x -> x)
                .reduce((ReduceFunction<Integer>) (a, b) -> a + b);
        OneInputTransformation<?, ?> tx = (OneInputTransformation<?, ?>) reduced.getTransformation();
        assertInstanceOf(HashPartitioner.class, tx.getPartitioner());
        assertNotNull(tx.getKeySelector());
    }

    /** reduce 之后的普通算子恢复使用 forward 分区。 */
    @Test
    void operatorAfterReduceRestoresForwardPartitioning() {
        StreamExecutionEnvironment env = new StreamExecutionEnvironment();
        DataStream<Integer> s = env.fromCollection(List.of(1, 2, 3))
                .keyBy((KeySelector<Integer, Integer>) x -> x)
                .reduce((ReduceFunction<Integer>) (a, b) -> a + b);
        DataStream<Integer> mapped = s.map(x -> x);
        OneInputTransformation<?, ?> tx = (OneInputTransformation<?, ?>) mapped.getTransformation();
        assertInstanceOf(ForwardPartitioner.class, tx.getPartitioner());
    }

    /** setParallelism 将并行度写入底层 transformation。 */
    @Test
    void setParallelismPropagatesToTransformation() {
        StreamExecutionEnvironment env = new StreamExecutionEnvironment();
        DataStream<Integer> src = env.fromCollection(List.of(1, 2, 3)).setParallelism(2);
        assertEquals(2, src.getTransformation().getParallelism());
    }
}
