package org.miniflink.window;

/**
 * 事件时间触发器：onElement 注册 window.end timer（首次）；onEventTime(end) 触发 FIRE_AND_PURGE。
 * TimerService 去重保证重复注册同一 end 安全。
 */
public class EventTimeTrigger<T, W extends Window> extends Trigger<T, W> {

    @SuppressWarnings("rawtypes")
    private static final EventTimeTrigger INSTANCE = new EventTimeTrigger();

    @SuppressWarnings("unchecked")
    public static <T, W extends Window> EventTimeTrigger<T, W> create() {
        return (EventTimeTrigger<T, W>) INSTANCE;
    }

    @Override
    public TriggerResult onElement(T element, long timestamp, W window, TriggerContext ctx) {
        ctx.registerEventTimeTimer(window.end());
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onEventTime(long time, W window, TriggerContext ctx) {
        if (time == window.end()) {
            return TriggerResult.FIRE_AND_PURGE;
        }
        return TriggerResult.CONTINUE;
    }
}
