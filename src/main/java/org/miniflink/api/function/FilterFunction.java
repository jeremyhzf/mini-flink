package org.miniflink.api.function;

/** 过滤：返回 true 的元素被保留，false 被丢弃。 */
@FunctionalInterface
public interface FilterFunction<T> {
    boolean filter(T value) throws Exception;
}
