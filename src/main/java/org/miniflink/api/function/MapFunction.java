package org.miniflink.api.function;

/** 一对一转换：每条输入产生一条输出。 */
@FunctionalInterface
public interface MapFunction<T, O> {
    O map(T value) throws Exception;
}
