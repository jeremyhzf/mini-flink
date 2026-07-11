package org.miniflink.examples;

import org.junit.jupiter.api.Test;
import org.miniflink.api.StreamExecutionEnvironment;
import org.miniflink.api.function.KeySelector;
import org.miniflink.api.function.ReduceFunction;
import org.miniflink.connector.CollectSink;
import org.miniflink.window.TumblingEventTimeWindows;

import java.time.Duration;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

/**
 * 阶段④验收：滚动窗口端到端聚合。
 * source → assignTimestampsAndWatermarks → keyBy → window(1s) → reduce(求和) → sink。
 * 注入 watermark 推进时间，验证每窗口输出最终累加值一次。
 */
class WindowExampleTest {

    record Event(String key, int value, long ts) { }

    /** 验证滚动窗口按 key 累加，且在窗口结束时各输出最终累加值一次。 */
    @Test
    void tumblingWindowAccumulatesByKeyAndEmitsAtWindowEnd() throws Exception {
        StreamExecutionEnvironment env = new StreamExecutionEnvironment();
        CollectSink<Event> sink = new CollectSink<>();

        env.fromCollection(List.of(
                new Event("a", 1, 100),    // 窗口 [0,1000)
                new Event("a", 2, 200),
                new Event("a", 10, 1100),  // 窗口 [1000,2000)
                new Event("a", 20, 1200)))
           .assignTimestampsAndWatermarks(
                   org.miniflink.time.WatermarkStrategy
                           .<Event>forBoundedOutOfOrderness(Duration.ofMillis(0), e -> e.ts))
           .keyBy((KeySelector<Event, String>) e -> e.key)
           .window(TumblingEventTimeWindows.of(Duration.ofSeconds(1)))
           .reduce((ReduceFunction<Event>) (a, b) -> new Event(a.key, a.value + b.value, b.ts))
           .addSink(sink::add);

        env.execute("window-example");

        // 窗口 [0,1000) → a: 1+2=3；窗口 [1000,2000) → a: 10+20=30
        List<Event> results = sink.getResults();
        assertEquals(2, results.size());
        assertTrue(results.contains(new Event("a", 3, 200)));
        assertTrue(results.contains(new Event("a", 30, 1200)));
    }
}
