package org.miniflink.time;

/** 从记录提取事件时间戳。 */
@FunctionalInterface
public interface TimestampAssigner<T> {
    long extractTimestamp(T record);
}
