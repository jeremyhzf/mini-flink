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

    /** 验证单 task 异常时 execute 干净失败而不挂起。 */
    @Test
    void singleTaskExceptionFailsExecuteCleanlyWithoutHanging() {
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

    /** 验证多 task 拓扑（rebalance）下单 task 异常时 execute 干净失败而非永久挂起，多次迭代稳定复现 race。 */
    @Test
    void multiTaskTopologyTaskExceptionFailsExecuteWithoutHanging() {
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
