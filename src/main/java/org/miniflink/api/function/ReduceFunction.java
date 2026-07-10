package org.miniflink.api.function;

/** 两两合并函数（keyed reduce 用）。 */
@FunctionalInterface
public interface ReduceFunction<T> {
    T reduce(T a, T b) throws Exception;
}
