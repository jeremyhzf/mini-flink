package org.miniflink.runtime;
/** 单值状态（绑定当前 key）。 */
public interface ValueState<T> {
    T value();
    void update(T v);
}
