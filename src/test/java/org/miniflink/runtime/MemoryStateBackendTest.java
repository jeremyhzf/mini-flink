package org.miniflink.runtime;

import org.junit.jupiter.api.Test;
import java.util.Map;
import static org.junit.jupiter.api.Assertions.*;

class MemoryStateBackendTest {

    /** ValueState 按 currentKey 隔离存取：切 key 后互不影响，回到原 key 值仍在。 */
    @Test
    void valueStateIsolatedByCurrentKey() {
        MemoryStateBackend backend = new MemoryStateBackend();
        ValueState<Integer> state = backend.getValueState("acc");

        backend.setCurrentKey("a");
        assertNull(state.value());
        state.update(1);
        assertEquals(1, state.value());

        backend.setCurrentKey("b");      // 切 key，a 的值不受影响
        assertNull(state.value());
        state.update(2);
        assertEquals(2, state.value());

        backend.setCurrentKey("a");      // 回到 a，值仍在
        assertEquals(1, state.value());
    }

    /** ListState 按 currentKey 累加：同一 key 元素累加，新 key 为空列表。 */
    @Test
    void listStateAccumulatesByKey() {
        MemoryStateBackend backend = new MemoryStateBackend();
        ListState<String> state = backend.getListState("words");

        backend.setCurrentKey("a");
        state.add("x"); state.add("y");
        assertIterableEquals(java.util.List.of("x", "y"), state.get());

        backend.setCurrentKey("b");
        assertIterableEquals(java.util.List.of(), state.get());  // 新 key 无数据（空 list，Flink ListState 语义）
    }

    /** MapState 按 currentKey 存键值，不同 key 之间相互隔离。 */
    @Test
    void mapStateStoresByKey() {
        MemoryStateBackend backend = new MemoryStateBackend();
        MapState<String, Integer> state = backend.getMapState("counts");

        backend.setCurrentKey("a");
        state.put("k1", 1); state.put("k2", 2);
        assertEquals(1, state.get("k1"));

        backend.setCurrentKey("b");
        assertNull(state.get("k1"));      // 隔离
    }
}
