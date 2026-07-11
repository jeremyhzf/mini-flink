package org.miniflink.window;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.miniflink.api.StreamExecutionEnvironment;
import org.miniflink.api.function.KeySelector;
import org.miniflink.api.function.MapFunction;
import org.miniflink.api.function.ReduceFunction;
import org.miniflink.api.function.SourceFunction;
import org.miniflink.connector.CollectSink;
import org.miniflink.runtime.ListCollector;
import org.miniflink.runtime.RuntimeContextImpl;
import org.miniflink.runtime.SourceContext;
import org.miniflink.runtime.Watermark;
import org.miniflink.runtime.checkpoint.WindowOperatorState;
import org.miniflink.runtime.operator.WindowOperator;
import org.miniflink.runtime.StateSnapshot;
import org.miniflink.time.WatermarkStrategy;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Task 12 Phase 2 端到端验收：window 作业 timer 持久化 + failover。
 *
 * <p>验证决策 B「window timer 持久化」端到端成立：恢复后<b>未触发的窗口</b>（其 timer 在
 * checkpoint 时 pending）经 timer 持久化（WindowOperator.snapshotState/restoreState）+
 * keyed state 持久化（StateBackend.snapshot/restore）后，在水印推进时继续触发输出。
 *
 * <p>分两条线：
 * <ul>
 *   <li><b>A（确定性，必做）</b>：直接测 WindowOperator 的 snapshot→restore→fire 路径，
 *       避开 coordinator/failover 时序——核心断言「未触发窗口经持久化往返后仍能在 watermark
 *       推进时触发输出」。闭合 Task 8 reviewer 指出的「缺 WindowOperator restore 端到端用例」缺口。</li>
 *   <li><b>B（尽力，可放宽）</b>：env 级 window 作业 failover（SlowSource + 中途故障注入 +
 *       周期 checkpoint + 自动恢复）→ 断言两窗口最终值都输出。</li>
 * </ul>
 */
class WindowFailoverTest {

    /** 事件载体：(key, value, 事件时间戳 ts)。reduce 求和：value 相加，ts 取后者。 */
    record Event(String key, int value, long ts) { }

    // ============================ A. 确定性 WindowOperator restore-then-fire ============================

    /**
     * A：未触发窗口经 snapshot→restore 往返后，推进 watermark 仍触发输出最终累加值。
     *
     * <p>这是 window failover 的核心断言，确定性复刻生产恢复路径（OperatorTask.run 第 70-73 行）：
     * open（空 backend 建句柄）→ backend.restore(keyedState) → chain.restoreState(operatorStates)。
     *
     * <p>WindowOperatorState 只含 timers + activeWindows 注册表；窗口累加值在 keyed state
     * （MapState「window-accs」）中，须与算子状态分别持久化/恢复——本测试同时快照两者并还原到
     * <b>全新</b>的 WindowOperator 实例（模拟恢复重建），断言恢复后 timer 能驱动窗口输出。
     *
     * <p>推演（TumblingEventTimeWindows.of(1s) + reduce 求和）：
     * <ol>
     *   <li>Event(a,1,100) + Event(a,2,200) → 窗口 [0,1000) 累加 = Event(a,3,200)，
     *       注册 timer@1000；watermark 未推进 → <b>不触发</b>。</li>
     *   <li>snapshot：WindowOperatorState(pendingTimers=[1000], windows=[(a,0,1000)]) +
     *       keyedState（MapState「window-accs」[a][TimeWindow(0,1000)]=Event(a,3,200)）。</li>
     *   <li>新建 WindowOperator 实例 → open → restore（backend + 算子状态）。
     *       恢复后 pendingTimers=[1000] 已重灌、activeWindows 含 (a,[0,1000))；累加值随 backend 还原。
     *       <b>恢复后未推进 watermark 前 collector 仍为空</b>（timer 在但未到点）。</li>
     *   <li>onWatermark(1000) → timerService.advanceTo 触发 timer@1000 → onEventTime(1000) →
     *       activeWindows.remove(1000) 命中 (a,[0,1000)) → state.get(window)=Event(a,3,200) → 输出。</li>
     * </ol>
     */
    @Test
    void 未触发窗口经快照恢复后推进watermark仍触发输出() throws Exception {
        // ---- 1) 构造 WindowOperator A，process 几条事件（窗口累加但未触发）----
        WindowOperator<Event> opA = new WindowOperator<>(
                TumblingEventTimeWindows.<Event>of(Duration.ofSeconds(1)),
                (ReduceFunction<Event>) (a, b) -> new Event(a.key, a.value + b.value, b.ts));
        RuntimeContextImpl ctxA = new RuntimeContextImpl(0, 1, (KeySelector<Event, String>) e -> e.key);
        ListCollector<Event> collectorA = new ListCollector<>();
        opA.open(collectorA, ctxA);

        ctxA.setCurrentTimestamp(100);
        opA.processElement(new Event("a", 1, 100));   // key=a, 窗口[0,1000), acc=Event(a,1,100), 注册 timer@1000
        ctxA.setCurrentTimestamp(200);
        opA.processElement(new Event("a", 2, 200));   // key=a, acc=Event(a,3,200)
        assertTrue(collectorA.getResult().isEmpty(), "watermark 未推进，窗口不应触发");

        // ---- 2) snapshot：同时取 keyed state（MapState 累加值）+ 算子状态（timers + activeWindows）----
        StateSnapshot keyedState = ctxA.getStateBackend().snapshot();
        WindowOperatorState opState = (WindowOperatorState) opA.snapshotState().orElseThrow(
                () -> new AssertionError("WindowOperator 应产出算子状态"));
        // 断言快照内容：pendingTimers=[1000]，windows=[(key=a, start=0, end=1000)]
        assertEquals(List.of(1000L), opState.getPendingTimers(), "快照应含 pending timer@1000");
        assertEquals(1, opState.getWindows().size(), "应含 1 个已注册窗口");
        assertEquals("a", opState.getWindows().get(0).key());
        assertEquals(0L, opState.getWindows().get(0).start());
        assertEquals(1000L, opState.getWindows().get(0).end());

        // ---- 3) 新建 WindowOperator B（模拟恢复重建）→ open → restore（先 backend 后算子，匹配 OperatorTask.run）----
        WindowOperator<Event> opB = new WindowOperator<>(
                TumblingEventTimeWindows.<Event>of(Duration.ofSeconds(1)),
                (ReduceFunction<Event>) (a, b) -> new Event(a.key, a.value + b.value, b.ts));
        RuntimeContextImpl ctxB = new RuntimeContextImpl(0, 1, (KeySelector<Event, String>) e -> e.key);
        ListCollector<Event> collectorB = new ListCollector<>();
        opB.open(collectorB, ctxB);
        // 恢复前 opB 的算子状态应为空（冷启）
        WindowOperatorState coldSnap = (WindowOperatorState) opB.snapshotState().orElse(null);
        assertTrue(coldSnap == null || (coldSnap.getPendingTimers().isEmpty() && coldSnap.getWindows().isEmpty()),
                "冷启 WindowOperator 应无 timer/窗口");
        assertTrue(collectorB.getResult().isEmpty(), "恢复前无输出");

        // 恢复：先还原 keyed state（累加值），再还原算子状态（timers + activeWindows）
        ctxB.getStateBackend().restore(keyedState);
        opB.restoreState(opState);

        // ---- 4) 恢复后未推进 watermark 前：timer/窗口已重灌，但仍未触发（核心中间态）----
        WindowOperatorState restoredSnap = (WindowOperatorState) opB.snapshotState().orElseThrow();
        assertEquals(List.of(1000L), restoredSnap.getPendingTimers(), "恢复后 timer@1000 应已重灌");
        assertEquals(1, restoredSnap.getWindows().size(), "恢复后 activeWindows 应含窗口");
        assertTrue(collectorB.getResult().isEmpty(),
                "恢复后 watermark 未推进，timer 虽在但不应触发——窗口尚未输出");

        // ---- 5) 推进 watermark ≥ 1000 → 恢复的 timer 触发 → 输出窗口最终累加值 Event(a,3,200)----
        opB.onWatermark(new Watermark(1000));
        assertEquals(List.of(new Event("a", 3, 200)), collectorB.getResult(),
                "恢复后推进 watermark，未触发窗口应输出最终累加值");
    }

    // ============================ B. 全 window failover 集成测试（尽力，放宽断言）============================

    /**
     * 第 failAt 条抛一次异常的 identity map（Event→Event）；seen 跨 copy/恢复持久 → 只故障一次。
     * 复用 Task 11 FailoverRecoveryTest 的 FailOnce 模式（同 WindowOperator.copy() 共享用户函数实例语义）。
     */
    static final class FailOnceIdentity implements MapFunction<Event, Event> {
        final AtomicInteger seen = new AtomicInteger();
        final int failAt;
        FailOnceIdentity(int failAt) { this.failAt = failAt; }
        @Override public Event map(Event e) {
            if (seen.incrementAndGet() == failAt) {
                throw new RuntimeException("inject-window-failure");
            }
            return e;
        }
    }

    /** 慢速 source：每条 collect 后 sleep，使 source 线程存活跨多个 checkpoint 周期（同 Task 11）。 */
    static final class SlowSource implements SourceFunction<Event> {
        final List<Event> data;
        final long sleepMs;
        SlowSource(List<Event> data, long sleepMs) {
            this.data = new ArrayList<>(data);
            this.sleepMs = sleepMs;
        }
        @Override public void run(SourceContext<Event> ctx) {
            for (Event e : data) {
                ctx.collect(e);
                try {
                    Thread.sleep(sleepMs);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
    }

    /**
     * B：window 作业 failover——恢复后两窗口最终值均被输出。
     *
     * <p>拓扑：source(带 ts 事件) → map(identity + 第 N 条故障) → assignTimestampsAndWatermarks →
     * keyBy → window(tumbling 1s) → reduce(求和) → sink；enableCheckpointing(短 interval) +
     * setMaxRestarts。故障点落在窗口 [0,1s) 仍 pending 时（前段数据 ts < 1000，watermark < 1000）。
     *
     * <p>三重时序（window + checkpoint + failover）较脆，断言放宽为「两窗口均触发」：
     * sink 结果中既含 ts<1000 的输出（窗口 [0,1s)），也含 ts>=1000 的输出（窗口 [1,2s)）——
     * 即恢复后未触发窗口继续到点输出。@Timeout 防 flaky。
     */
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void window作业failover后两窗口最终值均输出() throws Exception {
        // 前段：窗口 [0,1000)，多条小 ts 事件（拉长存活供多 checkpoint 周期）；后段：窗口 [1000,2000)
        List<Event> data = new ArrayList<>();
        for (int i = 0; i < 8; i++) {
            data.add(new Event("a", 1, 100 + i * 50));   // ts 100..450，窗口 [0,1000)
        }
        for (int i = 0; i < 8; i++) {
            data.add(new Event("a", 1, 1100 + i * 50));  // ts 1100..1450，窗口 [1000,2000)
        }
        FailOnceIdentity failOnce = new FailOnceIdentity(6);   // 第 6 条抛——落在多个 checkpoint 之后、窗口 [0,1s) pending 期
        CollectSink<Event> sink = new CollectSink<>();

        StreamExecutionEnvironment env = new StreamExecutionEnvironment();
        env.enableCheckpointing(2);   // 周期 2ms checkpoint
        env.setMaxRestarts(3);

        env.addSource(new SlowSource(data, 3))                   // 每条 sleep 3ms → source 存活跨多 checkpoint
           .map(failOnce)                                          // Event→Event；第 6 条抛
           .assignTimestampsAndWatermarks(
                   WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofMillis(0), e -> e.ts))
           .keyBy((KeySelector<Event, String>) e -> e.key)
           .window(TumblingEventTimeWindows.of(Duration.ofSeconds(1)))
           .reduce((ReduceFunction<Event>) (a, b) -> new Event(a.key, a.value + b.value, b.ts))
           .addSink(sink::add);

        env.execute("window-failover");

        // 确认故障注入确实触发并恢复重跑（seen 跨 copy/恢复持久，应已越过 failAt）
        assertTrue(failOnce.seen.get() > failOnce.failAt,
                "故障应触发一次并恢复重跑（seen=" + failOnce.seen.get() + " 应 > failAt=" + failOnce.failAt + "）");

        // 放宽断言：两窗口均触发——既有 ts<1000 的输出（[0,1s)），也有 ts>=1000 的输出（[1,2s)）
        List<Event> results = sink.getResults();
        boolean win0Fired = results.stream().anyMatch(e -> e.ts < 1000);    // 窗口 [0,1s) 触发
        boolean win1Fired = results.stream().anyMatch(e -> e.ts >= 1000);   // 窗口 [1,2s) 触发
        assertTrue(win0Fired, "window failover 后窗口 [0,1s) 应触发输出，results=" + results);
        assertTrue(win1Fired, "window failover 后窗口 [1,2s) 应触发输出，results=" + results);
    }
}
