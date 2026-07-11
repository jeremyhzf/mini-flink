package org.miniflink.runtime.checkpoint;

import org.junit.jupiter.api.Test;
import org.miniflink.time.InternalTimerService;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

class WindowOperatorStateTest {

    @Test
    void internalTimerService快照与恢复() {
        InternalTimerService s = new InternalTimerService();
        s.registerEventTimeTimer(100L);
        s.registerEventTimeTimer(300L);
        s.registerEventTimeTimer(200L);

        List<Long> snap = s.snapshotTimers();
        assertEquals(List.of(100L, 200L, 300L), snap);   // 升序去重

        InternalTimerService r = new InternalTimerService();
        r.restoreTimers(snap);
        assertEquals(List.of(100L, 200L, 300L), r.snapshotTimers());
    }

    @Test
    void windowOperatorState持有timers与windows() {
        WindowOperatorState s = new WindowOperatorState(
                List.of(500L),
                List.of(new WindowOperatorState.WindowEntry("k1", 0L, 1000L)));
        assertEquals(List.of(500L), s.getPendingTimers());
        assertEquals(1, s.getWindows().size());
        WindowOperatorState.WindowEntry e = s.getWindows().get(0);
        assertEquals("k1", e.key());
        assertEquals(0L, e.start());
        assertEquals(1000L, e.end());
    }
}
