package org.miniflink.window;

import org.junit.jupiter.api.Test;
import java.util.ArrayList;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

class EventTimeTriggerTest {

    /** 记录 TriggerContext 注册的 timer。 */
    static class CapturingContext implements TriggerContext {
        final List<Long> registered = new ArrayList<>();
        long wm = Long.MIN_VALUE;
        @Override public long getCurrentWatermark() { return wm; }
        @Override public void registerEventTimeTimer(long time) { registered.add(time); }
        @Override public void deleteEventTimeTimer(long time) { }
    }

    /** onElement 注册 windowEnd 的 timer 并返回 CONTINUE。 */
    @Test
    void onElementRegistersWindowEndTimerAndReturnsContinue() throws Exception {
        EventTimeTrigger<String, TimeWindow> trigger = EventTimeTrigger.create();
        CapturingContext ctx = new CapturingContext();
        TimeWindow window = new TimeWindow(1000, 2000);
        assertEquals(TriggerResult.CONTINUE, trigger.onElement("x", 1500, window, ctx));
        assertEquals(List.of(2000L), ctx.registered);   // 注册 window.end
    }

    /** onEventTime 在 windowEnd 时返回 FIRE_AND_PURGE，否则返回 CONTINUE。 */
    @Test
    void onEventTimeAtWindowEndFiresAndPurges() throws Exception {
        EventTimeTrigger<String, TimeWindow> trigger = EventTimeTrigger.create();
        CapturingContext ctx = new CapturingContext();
        TimeWindow window = new TimeWindow(1000, 2000);
        assertEquals(TriggerResult.FIRE_AND_PURGE, trigger.onEventTime(2000, window, ctx));
        assertEquals(TriggerResult.CONTINUE, trigger.onEventTime(1500, window, ctx));  // 非 end
    }
}
