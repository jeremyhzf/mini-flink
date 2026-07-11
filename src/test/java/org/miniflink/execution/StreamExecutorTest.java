package org.miniflink.execution;

import org.junit.jupiter.api.Test;
import org.miniflink.api.StreamExecutionEnvironment;
import org.miniflink.connector.CollectSink;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class StreamExecutorTest {

    /** 多线程端到端执行单并行度 forward 链，顺序保持。 */
    @Test
    void multiThreadedEndToEndExecutionOfSingleParallelismChain() throws Exception {
        StreamExecutionEnvironment env = new StreamExecutionEnvironment();
        CollectSink<Integer> sink = new CollectSink<>();
        env.fromCollection(List.of(1, 2, 3))
           .map(x -> x * 10)
           .addSink(sink::add);

        env.execute("test");

        // 单并行度 forward，顺序保持
        assertEquals(List.of(10, 20, 30), sink.getResults());
    }
}
