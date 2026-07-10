package org.miniflink.runtime;

/** 把 SourceContext.collect 转发到下游 Collector。 */
public class SourceContextImpl<T> implements SourceContext<T> {
    private final Collector<T> out;

    public SourceContextImpl(Collector<T> out) {
        this.out = out;
    }

    @Override
    public void collect(T record) {
        out.collect(record);
    }
}
