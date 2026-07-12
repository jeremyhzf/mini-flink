package org.miniflink.runtime.operator;

import org.miniflink.api.function.KeySelector;
import org.miniflink.api.function.ReduceFunction;
import org.miniflink.runtime.Collector;
import org.miniflink.state.MapState;
import org.miniflink.runtime.Operator;
import org.miniflink.state.OperatorState;
import org.miniflink.runtime.RuntimeContext;
import org.miniflink.runtime.Watermark;
import org.miniflink.checkpoint.WindowOperatorState;
import org.miniflink.time.InternalTimerService;
import org.miniflink.time.TimerHandler;
import org.miniflink.window.EventTimeTrigger;
import org.miniflink.window.TimeWindow;
import org.miniflink.window.TriggerContext;
import org.miniflink.window.TriggerResult;
import org.miniflink.window.WindowAssigner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 窗口聚合算子：per-key per-window MapState 增量 reduce；watermark 推进触发 window-end 输出最终值 + 清理。
 * 活跃窗口注册表：end -> [(key, window)]，按 end 直达待触发窗口，避免遍历所有 key。
 * 阶段④仅支持 parallelism=1 的 watermark 语义；多上游 watermark 的 min 对齐留阶段⑤（spec §5）。
 */
public class WindowOperator<IN> implements Operator<IN, IN>, TimerHandler {

    /** (key, window) 对，作注册表条目。 */
    private record KeyedWindow(Object key, TimeWindow window) { }

    private final WindowAssigner<IN, TimeWindow> windowAssigner;
    private final ReduceFunction<IN> reduceFn;
    /** 事件时间触发器：决定窗口何时触发（Task 6 抽象，替代内联触发逻辑）。 */
    private final EventTimeTrigger<IN, TimeWindow> trigger = EventTimeTrigger.create();
    private Collector<IN> out;
    private RuntimeContext ctx;
    private MapState<TimeWindow, IN> state;           // per-key per-window 累加器，按 currentKey 寻址
    private KeySelector<IN, ?> keySelector;
    private final InternalTimerService timerService = new InternalTimerService();
    /** Trigger 上下文：委托 timerService 提供当前 watermark + 定时器注册。 */
    private final TriggerContext triggerCtx = new TriggerContextImpl();
    /** end -> 该 end 下所有 (key, window)（watermark 到 end 时全部触发）。 */
    private final Map<Long, List<KeyedWindow>> activeWindows = new HashMap<>();

    public WindowOperator(WindowAssigner<IN, TimeWindow> windowAssigner, ReduceFunction<IN> reduceFn) {
        this.windowAssigner = windowAssigner;
        this.reduceFn = reduceFn;
    }

    @Override
    public void open(Collector<IN> out, RuntimeContext ctx) {
        this.out = out;
        this.ctx = ctx;
        this.state = ctx.getStateBackend().getMapState("window-accs");
        @SuppressWarnings("unchecked")
        KeySelector<IN, ?> ks = (KeySelector<IN, ?>) ctx.getKeySelector();
        this.keySelector = ks;
    }

    @Override
    public void processElement(IN record) throws Exception {
        long ts = ctx.getCurrentTimestamp();
        Object key = keySelector.getKey(record);
        ctx.setCurrentKey(key);
        for (TimeWindow window : windowAssigner.assignWindows(record, ts)) {
            if (window.end() <= timerService.currentWatermark()) {
                continue;   // 迟到数据丢弃（spec §8）；currentWatermark 初始 Long.MIN_VALUE，首条不误判
            }
            IN acc = state.get(window);
            IN reduced = (acc == null) ? record : reduceFn.reduce(acc, record);
            state.put(window, reduced);

            // 注册窗口：若该 (key, window) 首次出现，加入注册表 + 经 Trigger 注册 end timer
            if (!isRegistered(key, window)) {
                activeWindows.computeIfAbsent(window.end(), k -> new ArrayList<>())
                        .add(new KeyedWindow(key, window));
                trigger.onElement(record, ts, window, triggerCtx);   // EventTimeTrigger.onElement 注册 window.end timer
            }
        }
    }

    @Override
    public void onWatermark(Watermark watermark) {
        // 推进时钟：触发所有 end <= watermark 的窗口
        timerService.advanceTo(watermark.getTimestamp(), this);
    }

    /** TimerService 回调：某 end 到点，触发该 end 下所有 (key, window)。 */
    @Override
    public void onEventTime(long time) {
        List<KeyedWindow> toFire = activeWindows.remove(time);
        if (toFire == null) {
            return;
        }
        for (KeyedWindow kw : toFire) {
            ctx.setCurrentKey(kw.key());
            // 经 Trigger 决定是否触发：EventTimeTrigger.onEventTime(end) 返回 FIRE_AND_PURGE
            TriggerResult r = trigger.onEventTime(time, kw.window(), triggerCtx);
            if (r == TriggerResult.FIRE_AND_PURGE) {
                IN acc = state.get(kw.window());
                if (acc != null) {
                    out.collect(acc);       // 输出窗口最终值
                }
                state.put(kw.window(), null);  // 清理（MapState 无 remove，用 put null）
            }
        }
    }

    private boolean isRegistered(Object key, TimeWindow window) {
        List<KeyedWindow> list = activeWindows.get(window.end());
        if (list == null) {
            return false;
        }
        for (KeyedWindow kw : list) {
            if (kw.key().equals(key) && kw.window().equals(window)) {
                return true;
            }
        }
        return false;
    }

    /** Trigger 上下文实现：委托 timerService 提供 watermark 与定时器注册（Task 6 抽象接入点）。 */
    private class TriggerContextImpl implements TriggerContext {
        @Override
        public long getCurrentWatermark() {
            return timerService.currentWatermark();
        }

        @Override
        public void registerEventTimeTimer(long time) {
            timerService.registerEventTimeTimer(time);
        }

        @Override
        public void deleteEventTimeTimer(long time) {
            timerService.deleteEventTimeTimer(time);
        }
    }

    @Override
    public void close() { /* 无操作 */ }

    @Override
    public WindowOperator<IN> copy() {
        return new WindowOperator<>(windowAssigner, reduceFn);   // 共享无状态 assigner/reduceFn
    }

    @Override
    public java.util.Optional<OperatorState> snapshotState() {
        java.util.List<Long> timers = timerService.snapshotTimers();
        java.util.List<WindowOperatorState.WindowEntry> wins = new java.util.ArrayList<>();
        for (List<KeyedWindow> list : activeWindows.values()) {
            for (KeyedWindow kw : list) {
                wins.add(new WindowOperatorState.WindowEntry(kw.key(), kw.window().start(), kw.window().end()));
            }
        }
        return java.util.Optional.of(new WindowOperatorState(timers, wins));
    }

    @Override
    public void restoreState(OperatorState state) {
        WindowOperatorState s = (WindowOperatorState) state;
        timerService.restoreTimers(s.getPendingTimers());
        activeWindows.clear();
        for (WindowOperatorState.WindowEntry e : s.getWindows()) {
            TimeWindow w = new TimeWindow(e.start(), e.end());
            activeWindows.computeIfAbsent(w.end(), k -> new ArrayList<>()).add(new KeyedWindow(e.key(), w));
        }
    }
}
