package org.miniflink.runtime.checkpoint;

import org.junit.jupiter.api.Test;
import org.miniflink.time.InternalTimerService;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

class WindowOperatorStateTest {

    /** InternalTimerService 的 snapshotTimers 升序去重，restoreTimers 可还原。 */
    @Test
    void internalTimerServiceSnapshotsAndRestores() {
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

    /** WindowOperatorState 持有 pendingTimers 与 windows 注册表。 */
    @Test
    void windowOperatorStateHoldsTimersAndWindows() {
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

    /** WindowOperatorState 可序列化往返不丢失 timers 与 windows 数据。 */
    @Test
    void windowOperatorStateSurvivesSerializationRoundTrip() throws Exception {
        // OperatorState extends Serializable —— WindowEntry 须可序列化，否则 checkpoint 持久化抛 NotSerializableException
        WindowOperatorState s = new WindowOperatorState(
                List.of(500L, 1000L),
                List.of(new WindowOperatorState.WindowEntry("k1", 0L, 1000L),
                        new WindowOperatorState.WindowEntry("k2", 1000L, 2000L)));

        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(bos)) {
            oos.writeObject(s);
        }
        Object roundTripped;
        try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bos.toByteArray()))) {
            roundTripped = ois.readObject();
        }
        assertInstanceOf(WindowOperatorState.class, roundTripped);
        WindowOperatorState r = (WindowOperatorState) roundTripped;
        assertEquals(List.of(500L, 1000L), r.getPendingTimers());
        assertEquals(2, r.getWindows().size());
        assertEquals("k1", r.getWindows().get(0).key());
        assertEquals(1000L, r.getWindows().get(0).end());
        assertEquals("k2", r.getWindows().get(1).key());
    }
}
