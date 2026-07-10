package org.miniflink.api.function;

import org.miniflink.runtime.Collector;

/** 一对多转换：一条输入通过 Collector 发出零或多条输出。 */
@FunctionalInterface
public interface FlatMapFunction<T, O> {
    void flatMap(T value, Collector<O> out) throws Exception;
}
