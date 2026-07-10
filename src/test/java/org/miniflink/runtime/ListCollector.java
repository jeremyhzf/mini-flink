package org.miniflink.runtime;

import java.util.ArrayList;
import java.util.List;

/** 测试辅助：把 collect 的元素收集进 List。 */
public class ListCollector<T> implements Collector<T> {
    private final List<T> collected = new ArrayList<>();

    @Override
    public void collect(T record) {
        collected.add(record);
    }

    @Override
    public void close() {
        // 阶段①无需操作
    }

    public List<T> getResult() {
        return collected;
    }
}
