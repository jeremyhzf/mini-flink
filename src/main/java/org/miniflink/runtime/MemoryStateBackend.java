package org.miniflink.runtime;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 内存状态后端（per-subtask）：三类存储 name -> (currentKey -> 值)。
 * 句柄实现经 currentKey() 寻址。currentKey 由 RuntimeContext 通过 setCurrentKey 设置。
 */
public class MemoryStateBackend implements StateBackend {
    private Object currentKey;
    private final Map<String, Map<Object, Object>> valueStore = new HashMap<>();
    private final Map<String, Map<Object, List<Object>>> listStore = new HashMap<>();
    private final Map<String, Map<Object, Map<Object, Object>>> mapStore = new HashMap<>();

    @Override
    public void setCurrentKey(Object key) {
        this.currentKey = key;
    }

    Object currentKey() {
        return currentKey;
    }

    // ---- ValueState 存储 ----
    Object getValue(String name, Object key) {
        Map<Object, Object> m = valueStore.get(name);
        return m == null ? null : m.get(key);
    }
    void putValue(String name, Object key, Object value) {
        valueStore.computeIfAbsent(name, k -> new HashMap<>()).put(key, value);
    }

    // ---- ListState 存储 ----
    List<Object> getOrCreateList(String name, Object key) {
        return listStore.computeIfAbsent(name, k -> new HashMap<>())
                .computeIfAbsent(key, k -> new ArrayList<>());
    }

    // ---- MapState 存储 ----
    Map<Object, Object> getOrCreateMap(String name, Object key) {
        return mapStore.computeIfAbsent(name, k -> new HashMap<>())
                .computeIfAbsent(key, k -> new HashMap<>());
    }

    @Override
    public <T> ValueState<T> getValueState(String name) {
        return new ValueStateImpl<>(this, name);
    }

    @Override
    public <T> ListState<T> getListState(String name) {
        return new ListStateImpl<>(this, name);
    }

    @Override
    public <K, V> MapState<K, V> getMapState(String name) {
        return new MapStateImpl<>(this, name);
    }

    @Override
    public StateSnapshot snapshot() {
        return new StateSnapshot(copyVV(valueStore), copyVL(listStore), copyVM(mapStore));
    }

    @Override
    public void restore(StateSnapshot s) {
        this.valueStore.clear();
        for (var e : copyVV(s.getValueStore()).entrySet()) valueStore.put(e.getKey(), e.getValue());
        this.listStore.clear();
        for (var e : copyVL(s.getListStore()).entrySet()) listStore.put(e.getKey(), e.getValue());
        this.mapStore.clear();
        for (var e : copyVM(s.getMapStore()).entrySet()) mapStore.put(e.getKey(), e.getValue());
        this.currentKey = null;
    }

    private static Map<String, Map<Object, Object>> copyVV(Map<String, Map<Object, Object>> src) {
        Map<String, Map<Object, Object>> dst = new HashMap<>();
        for (var e : src.entrySet()) dst.put(e.getKey(), new HashMap<>(e.getValue()));
        return dst;
    }

    private static Map<String, Map<Object, List<Object>>> copyVL(Map<String, Map<Object, List<Object>>> src) {
        Map<String, Map<Object, List<Object>>> dst = new HashMap<>();
        for (var e : src.entrySet()) {
            Map<Object, List<Object>> inner = new HashMap<>();
            for (var ie : e.getValue().entrySet()) inner.put(ie.getKey(), new ArrayList<>(ie.getValue()));
            dst.put(e.getKey(), inner);
        }
        return dst;
    }

    private static Map<String, Map<Object, Map<Object, Object>>> copyVM(Map<String, Map<Object, Map<Object, Object>>> src) {
        Map<String, Map<Object, Map<Object, Object>>> dst = new HashMap<>();
        for (var e : src.entrySet()) {
            Map<Object, Map<Object, Object>> inner = new HashMap<>();
            for (var ie : e.getValue().entrySet()) inner.put(ie.getKey(), new HashMap<>(ie.getValue()));
            dst.put(e.getKey(), inner);
        }
        return dst;
    }
}
