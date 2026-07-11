package org.miniflink.runtime.operator;

import org.junit.jupiter.api.Test;
import org.miniflink.api.function.ReduceFunction;
import org.miniflink.runtime.ListCollector;
import org.miniflink.runtime.RuntimeContextImpl;
import org.miniflink.runtime.Watermark;
import org.miniflink.window.TumblingEventTimeWindows;

import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

class WindowOperatorTest {

    /** 记录带 ts：(value, ts)。 */
    record TE(int value, long ts) { }

    @Test
    void 窗口结束输出最终累加值一次() throws Exception {
        // keySelector: 按 value 分组；reduce: 求和；窗口 1s
        WindowOperator<TE> op = new WindowOperator<>(
                TumblingEventTimeWindows.<TE>of(java.time.Duration.ofSeconds(1)),
                (ReduceFunction<TE>) (a, b) -> new TE(a.value, a.ts));
        // 简化：reduce 取 a（同 key 同窗口只保留第一条）—— 改用求和验证累加
        op = new WindowOperator<>(
                TumblingEventTimeWindows.<TE>of(java.time.Duration.ofSeconds(1)),
                (ReduceFunction<TE>) (a, b) -> new TE(a.value + b.value, b.ts));
        RuntimeContextImpl ctx = new RuntimeContextImpl(0, 1, (TE t) -> t.value);  // keySelector: TE -> value
        ListCollector<TE> out = new ListCollector<>();
        op.open(out, ctx);

        // 三条 ts 在 [1000,2000) 窗口，key 都为 value 分组？这里 keySelector=t.value，value 不同则不同 key
        // 为简单：用同 key（value 相同）同窗口累加
        ctx.setCurrentTimestamp(1500);
        op.processElement(new TE(5, 1500));   // key=5, 窗口[1000,2000), acc=5
        ctx.setCurrentTimestamp(1700);
        op.processElement(new TE(5, 1700));   // key=5, acc=5+5=10
        assertTrue(out.getResult().isEmpty());  // 窗口未触发，无输出

        op.onWatermark(new Watermark(2000));   // watermark 到 window.end → 触发
        assertEquals(List.of(new TE(10, 1700)), out.getResult());
    }

    @Test
    void 不同key的窗口各自累加() throws Exception {
        WindowOperator<TE> op = new WindowOperator<>(
                TumblingEventTimeWindows.<TE>of(java.time.Duration.ofSeconds(1)),
                (ReduceFunction<TE>) (a, b) -> new TE(a.value + b.value, b.ts));
        RuntimeContextImpl ctx = new RuntimeContextImpl(0, 1, (TE t) -> t.value);
        ListCollector<TE> out = new ListCollector<>();
        op.open(out, ctx);

        ctx.setCurrentTimestamp(1500);
        op.processElement(new TE(1, 1500));   // key=1
        ctx.setCurrentTimestamp(1500);
        op.processElement(new TE(2, 1500));   // key=2
        ctx.setCurrentTimestamp(1600);
        op.processElement(new TE(1, 1600));   // key=1, acc=2
        op.onWatermark(new Watermark(2000));
        // 两个 key 各输出：key=1 → (2,1600)，key=2 → (2,1500)
        assertEquals(2, out.getResult().size());
        assertTrue(out.getResult().contains(new TE(2, 1600)));
        assertTrue(out.getResult().contains(new TE(2, 1500)));
    }

    @Test
    void 不同窗口按end分别触发() throws Exception {
        WindowOperator<TE> op = new WindowOperator<>(
                TumblingEventTimeWindows.<TE>of(java.time.Duration.ofSeconds(1)),
                (ReduceFunction<TE>) (a, b) -> new TE(a.value + b.value, b.ts));
        RuntimeContextImpl ctx = new RuntimeContextImpl(0, 1, (TE t) -> t.value);
        ListCollector<TE> out = new ListCollector<>();
        op.open(out, ctx);

        ctx.setCurrentTimestamp(500);
        op.processElement(new TE(1, 500));    // 窗口 [0,1000)
        ctx.setCurrentTimestamp(1500);
        op.processElement(new TE(9, 1500));   // 窗口 [1000,2000)

        op.onWatermark(new Watermark(1000));   // 仅触发 [0,1000)（end=1000）
        assertEquals(List.of(new TE(1, 500)), out.getResult());

        op.onWatermark(new Watermark(2000));   // 再触发 [1000,2000)（end=2000）
        assertEquals(2, out.getResult().size());
        assertTrue(out.getResult().contains(new TE(9, 1500)));
    }
}
