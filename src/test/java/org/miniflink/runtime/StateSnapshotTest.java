package org.miniflink.runtime;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class StateSnapshotTest {

    @Test
    void snapshot深拷贝并在restore后还原() {
        MemoryStateBackend b = new MemoryStateBackend();
        b.setCurrentKey("k1");
        ValueState<Integer> v = b.getValueState("cnt");
        v.update(1);
        MapState<String, Integer> m = b.getMapState("map");
        m.put("a", 10);

        StateSnapshot snap = b.snapshot();

        // 快照后修改原 backend，快照不应受影响（深拷贝）
        v.update(99);
        m.put("a", 99);

        MemoryStateBackend restored = new MemoryStateBackend();
        restored.restore(snap);
        restored.setCurrentKey("k1");
        assertEquals(1, restored.getValueState("cnt").value());     // 快照时的值
        assertEquals(10, restored.getMapState("map").get("a"));
    }

    @Test
    void restore重置currentKey() {
        MemoryStateBackend b = new MemoryStateBackend();
        b.setCurrentKey("k1");
        b.getValueState("cnt").update(5);
        StateSnapshot snap = b.snapshot();

        MemoryStateBackend r = new MemoryStateBackend();
        r.setCurrentKey("stale");
        r.restore(snap);
        // restore 后 currentKey 重置；新查询需重新 setCurrentKey
        r.setCurrentKey("k1");
        assertEquals(5, r.getValueState("cnt").value());
    }
}
