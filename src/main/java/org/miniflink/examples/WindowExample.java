package org.miniflink.examples;

import org.miniflink.api.StreamExecutionEnvironment;
import org.miniflink.connector.CollectSink;
import org.miniflink.time.WatermarkStrategy;
import org.miniflink.window.TumblingEventTimeWindows;

import java.time.Duration;
import java.util.List;

/**
 * 阶段④验收示例（可独立运行版本）。
 *
 * <p>运行方式：
 * <pre>
 *   mvn compile exec:java -Dexec.mainClass="org.miniflink.examples.WindowExample"
 *   # 或
 *   mvn -q compile && java -cp target/classes org.miniflink.examples.WindowExample
 * </pre>
 *
 * <p>演示 event time + watermark + 滚动窗口（阶段④核心能力）：
 * source → assignTimestampsAndWatermarks → keyBy → window(1s) → reduce(求和) → sink。
 * watermark 推进到 window.end 时触发，输出该窗口的最终累加值——每窗口只输出一次
 * （区别于阶段③ ReduceOperator 的 running reduce 每条输出）。
 */
public class WindowExample {

    /** 事件：(key, value, 事件时间戳 ts)。 */
    public record Event(String key, int value, long ts) { }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = new StreamExecutionEnvironment();
        CollectSink<Event> sink = new CollectSink<>();

        List<Event> events = List.of(
                new Event("a", 1, 100),    // 窗口 [0,1000)
                new Event("a", 2, 200),
                new Event("a", 10, 1100),  // 窗口 [1000,2000)
                new Event("a", 20, 1200));

        env.fromCollection(events)
           .assignTimestampsAndWatermarks(
                   WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofMillis(0), e -> e.ts))
           .keyBy(e -> e.key)
           .window(TumblingEventTimeWindows.of(Duration.ofSeconds(1)))
           .reduce((a, b) -> new Event(a.key, a.value + b.value, b.ts))
           .addSink(sink::add);

        env.execute("window-example");

        System.out.println("输入：" + events);
        System.out.println("滚动窗口（1s）聚合结果（watermark 触发，每窗口输出最终累加值一次）：");
        for (Event e : sink.getResults()) {
            System.out.println("  " + e.key + " => " + e.value);
        }
        System.out.println("预期：a => 3（窗口 [0,1s)：1+2），a => 30（窗口 [1,2s)：10+20）");
    }
}
