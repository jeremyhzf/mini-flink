package org.miniflink.window;

import org.junit.jupiter.api.Test;
import java.util.Collection;
import static org.junit.jupiter.api.Assertions.*;

class WindowAssignerTest {

    /** 滚动窗口将单条记录分配到单个 TimeWindow。 */
    @Test
    void tumblingWindowAssignsSingleWindow() {
        TumblingEventTimeWindows<Object> assigner = TumblingEventTimeWindows.of(java.time.Duration.ofSeconds(1));
        Collection<TimeWindow> ws = assigner.assignWindows(null, 1500L);   // 1500ms → [1000, 2000)
        assertEquals(1, ws.size());
        TimeWindow w = ws.iterator().next();
        assertEquals(1000L, w.start());
        assertEquals(2000L, w.end());
        assertTrue(assigner.isEventTime());
    }

    /** 滑动窗口将单条记录分配到多个重叠的 TimeWindow。 */
    @Test
    void slidingWindowAssignsMultipleOverlappingWindows() {
        // size=3s, slide=1s → 一记录落入 3 个窗口
        SlidingEventTimeWindows<Object> assigner = SlidingEventTimeWindows.of(
                java.time.Duration.ofSeconds(3), java.time.Duration.ofSeconds(1));
        Collection<TimeWindow> ws = assigner.assignWindows(null, 3500L);
        assertEquals(3, ws.size());
        // 3500ms 落入 [1000,4000)、[2000,5000)、[3000,6000)
        // 验证每个窗口都含 3500
        for (TimeWindow w : ws) {
            assertTrue(w.start() <= 3500L && 3500L < w.end());
        }
    }

    /** TimeWindow 的 equals 由 start 和 end 决定。 */
    @Test
    void timeWindowEqualsByStartAndEnd() {
        assertEquals(new TimeWindow(1000, 2000), new TimeWindow(1000, 2000));
        assertNotEquals(new TimeWindow(1000, 2000), new TimeWindow(1000, 3000));
    }
}
