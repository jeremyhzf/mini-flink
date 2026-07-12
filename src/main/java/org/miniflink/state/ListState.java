package org.miniflink.state;
/** 列表状态（绑定当前 key）。 */
public interface ListState<T> {
    Iterable<T> get();
    void add(T v);
    void clear();
}
