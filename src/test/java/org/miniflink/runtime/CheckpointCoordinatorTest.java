package org.miniflink.runtime;

import org.junit.jupiter.api.Test;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

class CheckpointCoordinatorTest {

    @Test
    void 收齐全部ack汇聚成Checkpoint() throws Exception {
        // 两个 subtask 的 key
        CheckpointCoordinator coord = new CheckpointCoordinator(Long.MAX_VALUE, List.of(), List.of("s0", "v0"), 2);
        SubtaskSnapshot snap0 = new SubtaskSnapshot(null, 5L, Map.of());
        SubtaskSnapshot snap1 = new SubtaskSnapshot(null, -1L, Map.of());
        assertNull(coord.lastCompletedCheckpoint());
        coord.ack("s0", 1L, snap0);
        assertNull(coord.lastCompletedCheckpoint(), "未收齐不完成");
        coord.ack("v0", 1L, snap1);
        Checkpoint cp = coord.lastCompletedCheckpoint();
        assertNotNull(cp);
        assertEquals(1L, cp.getCheckpointId());
        assertEquals(2, cp.getSnapshots().size());
    }

    @Test
    void retained只保留最近N个() {
        CheckpointCoordinator coord = new CheckpointCoordinator(Long.MAX_VALUE, List.of(), List.of("s0"), 2);
        for (long id = 1; id <= 3; id++) {
            coord.ack("s0", id, new SubtaskSnapshot(null, id, Map.of()));
        }
        // 仅最近 2 个 retained
        Checkpoint last = coord.lastCompletedCheckpoint();
        assertEquals(3L, last.getCheckpointId());
    }
}
