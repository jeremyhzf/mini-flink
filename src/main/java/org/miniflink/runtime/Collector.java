package org.miniflink.runtime;

/** 数据输出抽象：算子向下游发送数据，source 向链路发送数据。 */
public interface Collector<T> {
    void collect(T record);
    void close();
}
