package org.miniflink.time;

import java.util.TreeSet;

/** event time 定时器实现：TreeSet 按 time 去重排序，advanceTo 触发所有 time<=watermark 的 timer。 */
public class InternalTimerService implements TimerService {
    private final TreeSet<Long> eventTimeTimers = new TreeSet<>();
    private long currentWatermark = Long.MIN_VALUE;

    @Override
    public long currentWatermark() {
        return currentWatermark;
    }

    @Override
    public void registerEventTimeTimer(long time) {
        eventTimeTimers.add(time);
    }

    @Override
    public void deleteEventTimeTimer(long time) {
        eventTimeTimers.remove(time);
    }

    /** 推进 watermark 到给定值，触发所有 time <= watermark 的 timer（按升序），回调 handler。 */
    public void advanceTo(long watermark, TimerHandler handler) {
        this.currentWatermark = Math.max(this.currentWatermark, watermark);   // 单调
        while (!eventTimeTimers.isEmpty() && eventTimeTimers.first() <= watermark) {
            long time = eventTimeTimers.pollFirst();
            handler.onEventTime(time);
        }
    }
}
