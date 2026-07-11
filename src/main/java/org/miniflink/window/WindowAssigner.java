package org.miniflink.window;

import java.util.Collection;

/** 把记录分配到一个（滚动）或多个（滑动）窗口。 */
public abstract class WindowAssigner<T, W extends Window> {
    public abstract Collection<W> assignWindows(T record, long timestamp);
    public abstract boolean isEventTime();
}
