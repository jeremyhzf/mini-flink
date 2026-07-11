package org.miniflink.runtime;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.miniflink.api.StreamExecutionEnvironment;
import org.miniflink.api.function.MapFunction;
import org.miniflink.api.function.SourceFunction;
import org.miniflink.connector.CollectSink;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Task 11 自动 failover capstone：keyed reduce 作业 + 中途故障注入 → 从 checkpoint 恢复 →
 * 最终结果与无故障一致（exactly-once：不丢不重）。
 *
 * 时序保证（核心陷阱：故障必须发生在至少一个 checkpoint 完成之后，否则无 checkpoint → 直接抛）：
 * - SlowSource 在每条 collect 之间 sleep，使 source 线程存活跨多个 checkpoint 周期 →
 *   coordinator 周期触发的 checkpoint 在 source collect 的 record 边界被处理、完成。
 * - 故障点（failAt）放在数据中后段，确保故障前已有多个 checkpoint 完成（lastCp 非 null）。
 * - FailOnce 的 seen 用 AtomicInteger，因 MapOperator.copy() 共享用户函数实例 →
 *   seen 跨恢复重建持久，恢复重跑时 seen 已过 failAt，不再抛（自然只故障一次）。
 */
@Timeout(value = 30, unit = TimeUnit.SECONDS)
class FailoverRecoveryTest {

    /** value载体（不可变 record，可安全作为 keyed state 值）。 */
    record StringInt(String s, int i) { }

    /** 第 failAt 条抛一次异常的 map（String→StringInt）；seen 跨 copy/恢复持久 → 只故障一次。 */
    static final class FailOnce implements MapFunction<String, StringInt> {
        final AtomicInteger seen = new AtomicInteger();
        final int failAt;
        FailOnce(int failAt) { this.failAt = failAt; }
        @Override public StringInt map(String w) {
            if (seen.incrementAndGet() == failAt) {
                throw new RuntimeException("inject-failure");
            }
            return new StringInt(w, 1);
        }
    }

    /** 慢速 source：每条 collect 后 sleep 一会儿，使 source 线程存活跨多个 checkpoint 周期。 */
    static final class SlowSource implements SourceFunction<String> {
        final List<String> data;
        final long sleepMs;
        SlowSource(List<String> data, long sleepMs) {
            this.data = new ArrayList<>(data);
            this.sleepMs = sleepMs;
        }
        @Override public void run(SourceContext<String> ctx) {
            for (String s : data) {
                ctx.collect(s);
                try {
                    Thread.sleep(sleepMs);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
    }

    @Test
    void 故障后从checkpoint恢复结果与无故障一致() throws Exception {
        int total = 10;
        List<String> data = new ArrayList<>();
        for (int i = 0; i < total; i++) {
            data.add("a");
        }
        FailOnce failOnce = new FailOnce(8);   // 第 8 条（索引 7）抛——落在多个 checkpoint 之后
        CollectSink<Map.Entry<String, Integer>> sink = new CollectSink<>();

        StreamExecutionEnvironment env = new StreamExecutionEnvironment();
        env.enableCheckpointing(2);              // 周期 2ms checkpoint
        env.setMaxRestarts(3);

        env.addSource(new SlowSource(data, 3))   // 每条 sleep 3ms → source 存活 ~30ms（跨 ~15 个 checkpoint 周期）
           .map(failOnce)                          // String → StringInt；第 8 条抛
           .keyBy(StringInt::s)
           .reduce((a, b) -> new StringInt(a.s, a.i + b.i))
           .map((MapFunction<StringInt, Map.Entry<String, Integer>>) si -> Map.entry(si.s, si.i))
           .addSink(sink::add);

        env.execute("failover");

        // 取 key=a 的最大计数值 = 最终累加（应 = total=10，不丢不重）
        int max = sink.getResults().stream()
                .filter(e -> e.getKey().equals("a"))
                .mapToInt(Map.Entry::getValue).max().orElse(0);
        assertEquals(total, max, "failover 恢复后求和应 = " + total + "（exactly-once）");
        assertTrue(failOnce.seen.get() >= total,
                "恢复重跑应处理全部 " + total + " 条（seen=" + failOnce.seen.get() + "）");
    }

    /**
     * 对照组：无故障时同一作业结果 = total。验证 FailOnce 未触发（冷启正常路径），
     * 也作为上面 failover 用例期望值的锚点。
     */
    @Test
    void 无故障时reduce求和正确() throws Exception {
        int total = 10;
        List<String> data = new ArrayList<>();
        for (int i = 0; i < total; i++) {
            data.add("a");
        }
        CollectSink<Map.Entry<String, Integer>> sink = new CollectSink<>();

        StreamExecutionEnvironment env = new StreamExecutionEnvironment();
        env.addSource(new SlowSource(data, 1))   // 慢 source（保证有 checkpoint），但无故障
           .map((MapFunction<String, StringInt>) w -> new StringInt(w, 1))
           .keyBy(StringInt::s)
           .reduce((a, b) -> new StringInt(a.s, a.i + b.i))
           .map((MapFunction<StringInt, Map.Entry<String, Integer>>) si -> Map.entry(si.s, si.i))
           .addSink(sink::add);

        env.execute("no-failure");

        int max = sink.getResults().stream()
                .filter(e -> e.getKey().equals("a"))
                .mapToInt(Map.Entry::getValue).max().orElse(0);
        assertEquals(total, max, "无故障求和应 = " + total);
    }
}
