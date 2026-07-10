package org.miniflink.runtime;

/** 丢弃所有元素的 Collector，用作链尾（sink 算子的下游）。 */
public class NoopCollector<T> implements Collector<T> {
    @Override
    public void collect(T record) {
        // 丢弃
    }

    @Override
    public void close() {
        // 无操作
    }
}
