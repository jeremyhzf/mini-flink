package org.miniflink.time;

import org.junit.jupiter.api.Test;
import java.util.ArrayList;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

class InternalTimerServiceTest {

    @Test
    void advanceTo触发到点timer按time顺序() {
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

    @Test
    void deleteTimer不触发() {
        InternalTimerService svc = new InternalTimerService();
        List<Long> fired = new ArrayList<>();
        svc.registerEventTimeTimer(100);
        svc.deleteEventTimeTimer(100);
        svc.advanceTo(200L, fired::add);
        assertTrue(fired.isEmpty());
    }
}
