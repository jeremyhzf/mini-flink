package org.miniflink.runtime.operator;

import org.miniflink.api.function.KeySelector;
import org.miniflink.api.function.ReduceFunction;
import org.miniflink.runtime.Collector;
import org.miniflink.runtime.MapState;
import org.miniflink.runtime.Operator;
import org.miniflink.runtime.RuntimeContext;
import org.miniflink.runtime.Watermark;
import org.miniflink.time.InternalTimerService;
import org.miniflink.time.TimerHandler;
import org.miniflink.window.TimeWindow;
import org.miniflink.window.WindowAssigner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 窗口聚合算子：per-key per-window MapState 增量 reduce；watermark 推进触发 window-end 输出最终值 + 清理。
 * 活跃窗口注册表：end -> [(key, window)]，按 end 直达待触发窗口，避免遍历所有 key。
 */
public class WindowOperator<IN> implements Operator<IN, IN>, TimerHandler {

    /** (key, window) 对，作注册表条目。 */
    private record KeyedWindow(Object key, TimeWindow window) { }

    private final WindowAssigner<IN, TimeWindow> windowAssigner;
    private final ReduceFunction<IN> reduceFn;
    private Collector<IN> out;
    private RuntimeContext ctx;
    private MapState<TimeWindow, IN> state;           // per-key per-window 累加器，按 currentKey 寻址
    private KeySelector<IN, ?> keySelector;
    private final InternalTimerService timerService = new InternalTimerService();
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
            IN acc = state.get(window);
            IN reduced = (acc == null) ? record : reduceFn.reduce(acc, record);
            state.put(window, reduced);

            // 注册窗口：若该 (key, window) 首次出现，加入注册表 + 注册 end timer
            if (!isRegistered(key, window)) {
                activeWindows.computeIfAbsent(window.end(), k -> new ArrayList<>())
                        .add(new KeyedWindow(key, window));
                timerService.registerEventTimeTimer(window.end());   // 等价 EventTimeTrigger：注册 window.end
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
            IN acc = state.get(kw.window());
            if (acc != null) {
                out.collect(acc);       // 输出窗口最终值
            }
            state.put(kw.window(), null);  // 清理（MapState 无 remove，用 put null）
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

    @Override
    public void close() { /* 无操作 */ }

    @Override
    public WindowOperator<IN> copy() {
        return new WindowOperator<>(windowAssigner, reduceFn);   // 共享无状态 assigner/reduceFn
    }
}
