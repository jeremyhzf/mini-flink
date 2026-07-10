package org.miniflink.execution;

import org.junit.jupiter.api.Test;
import org.miniflink.api.StreamExecutionEnvironment;
import org.miniflink.connector.CollectSink;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class EndToEndExecutionTest {

    @Test
    void source_map_filter_sink应端到端跑通() throws Exception {
        StreamExecutionEnvironment env = new StreamExecutionEnvironment();
        CollectSink<Integer> sink = new CollectSink<>();

        env.fromCollection(List.of(1, 2, 3, 4, 5))
           .map(x -> x * 10)
           .filter(x -> x > 20)
           .addSink(sink::add);

        env.execute("demo-job");

        assertEquals(List.of(30, 40, 50), sink.getResults());
    }

    @Test
    void flatMap链应端到端跑通() throws Exception {
        StreamExecutionEnvironment env = new StreamExecutionEnvironment();
        CollectSink<String> sink = new CollectSink<>();

        env.fromCollection(List.of("a b", "c"))
           .<String>flatMap((line, out) -> { for (String w : line.split(" ")) out.collect(w); })
           .map(String::toUpperCase)
           .addSink(sink::add);

        env.execute("flatmap-job");

        assertEquals(List.of("A", "B", "C"), sink.getResults());
    }
}
