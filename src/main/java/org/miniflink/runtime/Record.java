package org.miniflink.runtime;

/** 携带一条用户数据的通道元素。 */
public record Record<T>(T value) implements StreamElement {
}
