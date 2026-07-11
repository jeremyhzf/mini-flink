package org.miniflink.window;

import java.time.Duration;
import java.util.Collection;
import java.util.List;

/** 滚动事件时间窗口：固定 size、不重叠。每记录落入单个窗口 [ts - ts%size, +size)。 */
public class TumblingEventTimeWindows<T> extends WindowAssigner<T, TimeWindow> {
    private final long sizeMillis;

    public TumblingEventTimeWindows(long sizeMillis) {
        this.sizeMillis = sizeMillis;
    }

    public static <T> TumblingEventTimeWindows<T> of(Duration size) {
        return new TumblingEventTimeWindows<>(size.toMillis());
    }

    @Override
    public Collection<TimeWindow> assignWindows(T record, long timestamp) {
        long start = timestamp - (timestamp % sizeMillis);
        return List.of(new TimeWindow(start, start + sizeMillis));
    }

    @Override
    public boolean isEventTime() {
        return true;
    }
}
