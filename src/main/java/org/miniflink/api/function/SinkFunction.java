package org.miniflink.api.function;

/** 输出端：每条到达的元素被消费（如打印、写文件）。 */
@FunctionalInterface
public interface SinkFunction<T> {
    void invoke(T value) throws Exception;
}
