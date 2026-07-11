package org.miniflink.runtime;

import org.junit.jupiter.api.Test;
import org.miniflink.api.StreamExecutionEnvironment;
import org.miniflink.connector.CollectSink;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FailureCloseTest {

    /** 一个在第 N 条抛异常的 map，用于触发 task 故障。 */
    static final class BoomMap implements org.miniflink.api.function.MapFunction<Integer, Integer> {
        final AtomicInteger seen = new AtomicInteger();
        final int boomAt;
        BoomMap(int boomAt) { this.boomAt = boomAt; }
        @Override public Integer map(Integer x) {
            if (seen.incrementAndGet() == boomAt) {
                throw new RuntimeException("boom");
            }
            return x;
        }
    }

    /** 恒等 map：仅为在下游制造一个独立 task 组（配合 setParallelism 打断链化）。 */
    static final class IdentityMap implements org.miniflink.api.function.MapFunction<Integer, Integer> {
        @Override public Integer map(Integer x) { return x; }
    }

    @Test
    void 单task异常时execute干净失败不挂起() {
        StreamExecutionEnvironment env = new StreamExecutionEnvironment();
        CollectSink<Integer> sink = new CollectSink<>();
        env.fromCollection(List.of(1, 2, 3, 4))
           .map(new BoomMap(2))
           .addSink(sink::add);

        long start = System.nanoTime();
        RuntimeException ex = assertThrows(RuntimeException.class, () -> env.execute("fail-close"));
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;
        // 不挂起：应在数秒内返回（无 checkpoint 时直接失败）
        assertTrue(elapsedMs < 10_000, "execute 应干净失败而非挂起，实际耗时 " + elapsedMs + "ms");
        assertTrue(ex.getMessage().contains("作业执行失败") || ex.getCause() != null);
    }

    /**
     * 真正复现「execute 永久挂起」缺口：下游 map 并行度=2 与 BoomMap(=1) 不同，
     * ExecutionGraph 自动改 rebalance → BoomMap 独立成组；BoomMap 抛异常后，
     * 下游两个 map 子任务阻塞在 Channel.receive()，旧代码 join() 无超时 → 永久挂起。
     * 修复后：任一异常 → 中断全部 → 阻塞的 receive 抛 InterruptedException → 干净失败。
     *
     * 多次迭代以稳定复现「早失败 / 晚启动」的 race（单次受调度影响，在隔离运行下未必触发）。
     */
    @Test
    void 多task拓扑下单task异常时execute不挂起() {
        for (int i = 0; i < 5; i++) {
            StreamExecutionEnvironment env = new StreamExecutionEnvironment();
            CollectSink<Integer> sink = new CollectSink<>();
            env.fromCollection(List.of(1, 2, 3, 4))
               .map(new BoomMap(2))
               .map(new IdentityMap()).setParallelism(2)
               .addSink(sink::add);

            final int idx = i;
            long start = System.nanoTime();
            RuntimeException ex = assertThrows(RuntimeException.class,
                    () -> env.execute("fail-close-multi-" + idx));
            long elapsedMs = (System.nanoTime() - start) / 1_000_000;
            assertTrue(elapsedMs < 10_000,
                    "第 " + i + " 轮：execute 应干净失败而非挂起，实际耗时 " + elapsedMs + "ms");
            assertTrue(ex.getMessage().contains("作业执行失败") || ex.getCause() != null);
        }
    }
}
