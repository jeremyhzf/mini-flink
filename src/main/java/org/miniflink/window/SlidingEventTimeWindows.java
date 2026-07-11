package org.miniflink.window;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * 滑动事件时间窗口：size 大小、slide 步长。一记录落入 size/slide 个连续窗口。
 * 窗口 [start, start+size)，start 对齐 slide，且 start <= ts < start+size。
 */
public class SlidingEventTimeWindows<T> extends WindowAssigner<T, TimeWindow> {
    private final long sizeMillis;
    private final long slideMillis;

    public SlidingEventTimeWindows(long sizeMillis, long slideMillis) {
        this.sizeMillis = sizeMillis;
        this.slideMillis = slideMillis;
    }

    public static <T> SlidingEventTimeWindows<T> of(Duration size, Duration slide) {
        return new SlidingEventTimeWindows<>(size.toMillis(), slide.toMillis());
    }

    @Override
    public Collection<TimeWindow> assignWindows(T record, long timestamp) {
        List<TimeWindow> windows = new ArrayList<>();
        long lastStart = timestamp - timestamp % slideMillis;
        // 从 lastStart 向前，每 slide 一个窗口，共 size/slide 个，满足 start <= ts < start+size
        int count = (int) (sizeMillis / slideMillis);
        for (int i = 0; i < count; i++) {
            long start = lastStart - i * slideMillis;
            if (start <= timestamp && timestamp < start + sizeMillis) {
                windows.add(new TimeWindow(start, start + sizeMillis));
            }
        }
        return windows;
    }

    @Override
    public boolean isEventTime() {
        return true;
    }
}
