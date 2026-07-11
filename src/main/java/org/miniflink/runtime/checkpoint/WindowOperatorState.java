package org.miniflink.runtime.checkpoint;

import org.miniflink.runtime.OperatorState;
import java.util.List;

/** WindowOperator 的快照：待触发 timers + 已注册 (key, window)。 */
public class WindowOperatorState implements OperatorState {
    /** 单个已注册窗口：(key, start, end)。 */
    public record WindowEntry(Object key, long start, long end) { }

    private final List<Long> pendingTimers;
    private final List<WindowEntry> windows;

    public WindowOperatorState(List<Long> pendingTimers, List<WindowEntry> windows) {
        this.pendingTimers = pendingTimers;
        this.windows = windows;
    }

    public List<Long> getPendingTimers() { return pendingTimers; }
    public List<WindowEntry> getWindows() { return windows; }
}
