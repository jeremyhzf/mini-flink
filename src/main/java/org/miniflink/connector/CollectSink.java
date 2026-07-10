package org.miniflink.connector;

import org.miniflink.api.function.SinkFunction;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** 内置 sink：把到达的元素收集进 List，供测试断言。 */
public class CollectSink<T> implements SinkFunction<T> {
    private final List<T> results = Collections.synchronizedList(new ArrayList<>());

    @Override
    public void invoke(T value) {
        add(value);
    }

    /** 追加一条结果；同时作为方法引用目标供 addSink(sink::add) 使用。 */
    public void add(T value) {
        results.add(value);
    }

    /** 返回内部结果的独立快照副本：与内部 List 隔离，执行期间迭代不会触发 CME。 */
    public List<T> getResults() {
        return new ArrayList<>(results);
    }
}
