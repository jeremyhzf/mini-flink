package org.miniflink.time;

/** timer 触发时的回调（算子实现）。 */
@FunctionalInterface
public interface TimerHandler {
    void onEventTime(long time);
}
