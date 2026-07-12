package org.miniflink.state;

/** 状态后端：创建 keyed state 句柄；currentKey 由 RuntimeContext 设置。 */
public interface StateBackend {
    <T> ValueState<T> getValueState(String name);
    <T> ListState<T> getListState(String name);
    <K, V> MapState<K, V> getMapState(String name);
    void setCurrentKey(Object key);
    StateSnapshot snapshot();
    void restore(StateSnapshot snapshot);
}
