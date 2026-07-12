package org.miniflink.runtime;

import org.junit.jupiter.api.Test;
import org.miniflink.api.function.MapFunction;
import org.miniflink.runtime.operator.MapOperator;
import org.miniflink.state.ValueState;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class SubtaskSnapshotTest {

    /** 测试用最小 coordinator：记录 ack。 */
    static class CapturingCoordinator extends CheckpointCoordinator {
        SubtaskSnapshot last;
        String lastKey; long lastId;
        CapturingCoordinator() { super(Long.MAX_VALUE, List.of(), List.of(), 1); }
        @Override public void ack(String key, long id, SubtaskSnapshot snap) { lastKey=key; lastId=id; last=snap; }
        @Override public void start() { }
        @Override public void stop() { }
    }

    /** operatorTask 在 onAligned 时对 backend 做快照并向 coordinator ack。 */
    @Test
    void operatorTaskOnAlignedSnapshotsBackendAndAcks() throws Exception {
        // 简化：直接验证 OperatorTask 在有 coordinator 时 onAligned 产出 SubtaskSnapshot
        // 构造一个带 ValueState 的链：用 ReduceOperator 更直观，这里用 MapOperator（无 state）验证 ack 触发
        OperatorChain<Integer,Integer> chain = new OperatorChain<>(List.of(
                new MapOperator<>((MapFunction<Integer,Integer>) x -> x)));
        CapturingCoordinator coord = new CapturingCoordinator();
        OperatorTask task = new OperatorTask(chain, List.of(new InputChannel(new Channel())),
                1, List.of(), new RuntimeContextImpl(0, 1, null), coord, "v0-0");
        task.onAligned(5L);
        assertEquals("v0-0", coord.lastKey);
        assertEquals(5L, coord.lastId);
        assertNotNull(coord.last);
        assertEquals(-1L, coord.last.getSourceOffset(), "非 source 的 offset 为 -1");
    }
}
