package org.miniflink.state;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * MemoryStateBackend 三类 store 的快照（结构深拷贝，值引用共享）。
 * 用于 checkpoint 持久化与恢复重建。Serializable 以便未来落盘（当前内存）。
 */
public class StateSnapshot implements Serializable {
    private final Map<String, Map<Object, Object>> valueStore;
    private final Map<String, Map<Object, List<Object>>> listStore;
    private final Map<String, Map<Object, Map<Object, Object>>> mapStore;

    public StateSnapshot(Map<String, Map<Object, Object>> valueStore,
                         Map<String, Map<Object, List<Object>>> listStore,
                         Map<String, Map<Object, Map<Object, Object>>> mapStore) {
        this.valueStore = valueStore;
        this.listStore = listStore;
        this.mapStore = mapStore;
    }

    public Map<String, Map<Object, Object>> getValueStore() { return valueStore; }
    public Map<String, Map<Object, List<Object>>> getListStore() { return listStore; }
    public Map<String, Map<Object, Map<Object, Object>>> getMapStore() { return mapStore; }
}
