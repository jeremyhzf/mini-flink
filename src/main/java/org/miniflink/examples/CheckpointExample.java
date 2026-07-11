package org.miniflink.examples;

import org.miniflink.api.StreamExecutionEnvironment;
import org.miniflink.api.function.MapFunction;
import org.miniflink.api.function.ReduceFunction;
import org.miniflink.api.function.SourceFunction;
import org.miniflink.connector.CollectSink;
import org.miniflink.runtime.SourceContext;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 阶段⑤验收示例（可独立运行）：周期 checkpoint + 故障自动恢复（exactly-once）。
 *
 * <p>运行方式：
 * <pre>
 *   mvn -q compile && java -cp target/classes org.miniflink.examples.CheckpointExample
 * </pre>
 *
 * <p>演示链路：throttled source（hello×4）→ FailAt(第3条抛) → keyBy(word) → reduce(计数累加) → sink。
 * 中途故障后从 checkpoint 自动恢复重跑，最终累加 = 4（不丢不重）。
 *
 * <h3>时序保证（核心：checkpoint 必须先于故障完成）</h3>
 * <p>若故障发生时无已完成 checkpoint，executor 将直接抛（无恢复）→ 示例失败。为此：
 * <ul>
 *   <li>{@link ThrottledSource} 每发一条 sleep 50ms，拉长 source 线程存活时间，使 coordinator
 *       周期触发的 checkpoint（interval=5ms）在 record 边界被处理并完成多轮；</li>
 *   <li>{@link FailAt} 的 seen 为实例字段——{@code MapOperator.copy()} 共享用户函数实例，
 *       故 seen 跨恢复重建持久：恢复重跑时 seen 已过 failAt，不再抛（自然只故障一次）。</li>
 * </ul>
 *
 * <h3>exactly-once 原理</h3>
 * <p>恢复时：source 按快照 offset 跳过已发记录（不重发），keyed state 按快照恢复（reduce 累加器回到
 * checkpoint 时刻值），故故障前已计入的记录既不丢也不重算 → 最终 hello = 4。
 */
public class CheckpointExample {

    /** 词频记录：(word, count)。 */
    public record WC(String word, int count) { }

    /**
     * 节流 source：发出预设的 WC 序列，每条之间 sleep，拉长 source 存活时间使若干 checkpoint 先完成。
     * （对照 {@code FailoverRecoveryTest.SlowSource}，这里直接发 WC，省去 String→WC 的 map。）
     */
    public static final class ThrottledSource implements SourceFunction<WC> {
        private final List<WC> data;
        private final long sleepMs;
        ThrottledSource(List<WC> data, long sleepMs) {
            this.data = List.copyOf(data);
            this.sleepMs = sleepMs;
        }
        @Override public void run(SourceContext<WC> ctx) throws Exception {
            for (WC wc : data) {
                ctx.collect(wc);
                Thread.sleep(sleepMs);
            }
        }
    }

    /** 第 N 条抛一次异常，触发 failover；seen 跨 copy/恢复持久 → 只故障一次。 */
    public static final class FailAt implements MapFunction<WC, WC> {
        private int seen = 0;
        private final int failAt;
        FailAt(int failAt) { this.failAt = failAt; }
        @Override public WC map(WC wc) {
            if (++seen == failAt) {
                throw new RuntimeException("inject-failure（演示 failover 自动恢复）");
            }
            return wc;
        }
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = new StreamExecutionEnvironment();
        env.enableCheckpointing(5);    // 周期 checkpoint（毫秒）—— 短间隔保证故障前已有多轮完成
        CollectSink<WC> sink = new CollectSink<>();

        List<WC> data = List.of(
                new WC("hello", 1), new WC("hello", 1),
                new WC("hello", 1), new WC("hello", 1));

        env.addSource(new ThrottledSource(data, 50))   // 每条 sleep 50ms → 故障前有 ~20 个 checkpoint 周期
           .map(new FailAt(3))                          // 第 3 条抛 → 触发 failover
           .keyBy(WC::word)
           .reduce((ReduceFunction<WC>) (a, b) -> new WC(a.word, a.count + b.count))
           .addSink(sink::add);

        env.execute("checkpoint-example");

        // reduce 输出 running（每输入一条输出当前累加）；取每 key 最大 count = 最终值
        Map<String, Integer> result = new HashMap<>();
        for (WC wc : sink.getResults()) {
            result.merge(wc.word, wc.count, Math::max);
        }
        System.out.println("词频统计（经故障自动恢复后）：");
        result.forEach((w, c) -> System.out.println("  " + w + " => " + c));
        System.out.println("预期：hello => 4（failover 后 exactly-once，不丢不重）");
    }
}
