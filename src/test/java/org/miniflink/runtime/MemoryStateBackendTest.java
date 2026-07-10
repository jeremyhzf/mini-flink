package org.miniflink.runtime;

import org.junit.jupiter.api.Test;
import java.util.Map;
import static org.junit.jupiter.api.Assertions.*;

class MemoryStateBackendTest {

    @Test
    void ValueState按currentKey隔离存取() {
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

    @Test
    void ListState按key累加() {
        MemoryStateBackend backend = new MemoryStateBackend();
        ListState<String> state = backend.getListState("words");

        backend.setCurrentKey("a");
        state.add("x"); state.add("y");
        assertIterableEquals(java.util.List.of("x", "y"), state.get());

        backend.setCurrentKey("b");
        assertIterableEquals(java.util.List.of(), state.get());  // 新 key 无数据（空 list，Flink ListState 语义）
    }

    @Test
    void MapState按key存键值() {
        MemoryStateBackend backend = new MemoryStateBackend();
        MapState<String, Integer> state = backend.getMapState("counts");

        backend.setCurrentKey("a");
        state.put("k1", 1); state.put("k2", 2);
        assertEquals(1, state.get("k1"));

        backend.setCurrentKey("b");
        assertNull(state.get("k1"));      // 隔离
    }
}
