package org.miniflink.time;

import org.junit.jupiter.api.Test;
import java.util.ArrayList;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

class InternalTimerServiceTest {

    /** advanceTo 按时间顺序触发已到点的 event-time timer。 */
    @Test
    void advanceToFiresDueTimersInTimeOrder() {
        InternalTimerService svc = new InternalTimerService();
        List<Long> fired = new ArrayList<>();
        TimerHandler handler = fired::add;

        svc.registerEventTimeTimer(300);
        svc.registerEventTimeTimer(100);
        svc.registerEventTimeTimer(200);

        svc.advanceTo(150L, handler);   // 只触发 100
        assertEquals(List.of(100L), fired);
        assertEquals(150L, svc.currentWatermark());

        svc.advanceTo(300L, handler);   // 触发 200, 300
        assertEquals(List.of(100L, 200L, 300L), fired);
    }

    /** 已删除的 timer 在 advanceTo 时不再触发。 */
    @Test
    void deleteTimerPreventsFiring() {
        InternalTimerService svc = new InternalTimerService();
        List<Long> fired = new ArrayList<>();
        svc.registerEventTimeTimer(100);
        svc.deleteEventTimeTimer(100);
        svc.advanceTo(200L, fired::add);
        assertTrue(fired.isEmpty());
    }
}
