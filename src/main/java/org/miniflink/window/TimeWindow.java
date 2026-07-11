package org.miniflink.window;

import java.util.Objects;

/** 时间窗口 [start, end)。equals/hashCode 按 start+end（作 per-key per-window state 寻址 key）。 */
public class TimeWindow extends Window {
    private final long start;
    private final long end;

    public TimeWindow(long start, long end) {
        this.start = start;
        this.end = end;
    }

    @Override public long start() { return start; }
    @Override public long end() { return end; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TimeWindow that)) return false;
        return start == that.start && end == that.end;
    }

    @Override
    public int hashCode() {
        return Objects.hash(start, end);
    }

    @Override
    public String toString() {
        return "TimeWindow[" + start + "," + end + ")";
    }
}
