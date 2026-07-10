package org.miniflink.runtime;

/** source 发数据用的上下文（只有 collect，不暴露 close）。 */
public interface SourceContext<T> {
    void collect(T record);
}
