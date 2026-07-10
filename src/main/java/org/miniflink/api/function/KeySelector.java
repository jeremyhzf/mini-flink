package org.miniflink.api.function;

/** 从一条记录中提取 key（keyBy 分区用）。 */
@FunctionalInterface
public interface KeySelector<T, K> {
    K getKey(T value) throws Exception;
}
