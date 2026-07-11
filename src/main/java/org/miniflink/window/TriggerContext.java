package org.miniflink.window;

/** Trigger 上下文：提供当前 watermark + 定时器注册（委托 TimerService）。 */
public interface TriggerContext {
    long getCurrentWatermark();
    void registerEventTimeTimer(long time);
    void deleteEventTimeTimer(long time);
}
