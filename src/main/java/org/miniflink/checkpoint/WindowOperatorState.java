package org.miniflink.checkpoint;

import org.miniflink.state.OperatorState;
import java.io.Serializable;
import java.util.List;

/** WindowOperator 的快照：待触发 timers + 已注册 (key, window)。 */
public class WindowOperatorState implements OperatorState {
    private static final long serialVersionUID = 1L;

    /** 单个已注册窗口：(key, start, end)。Serializable 以随 OperatorState 持久化（key 须可序列化）。 */
    public record WindowEntry(Object key, long start, long end) implements Serializable {
        private static final long serialVersionUID = 1L;
    }

    private final List<Long> pendingTimers;
    private final List<WindowEntry> windows;

    public WindowOperatorState(List<Long> pendingTimers, List<WindowEntry> windows) {
        this.pendingTimers = List.copyOf(pendingTimers);   // 防御性拷贝（checkpoint 状态对象应不可变）
        this.windows = List.copyOf(windows);
    }

    public List<Long> getPendingTimers() { return pendingTimers; }
    public List<WindowEntry> getWindows() { return windows; }
}
