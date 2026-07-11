package org.miniflink.runtime;

/** 携带一条用户数据及其事件时间戳的通道元素。 */
public record Record<T>(T value, long timestamp) implements StreamElement {
}
