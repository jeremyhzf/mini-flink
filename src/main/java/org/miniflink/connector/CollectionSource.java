package org.miniflink.connector;

import org.miniflink.api.function.SourceFunction;
import org.miniflink.runtime.SourceContext;

/** 内置 source：从 Iterable 顺序读取数据。 */
public class CollectionSource<T> implements SourceFunction<T> {
    private final Iterable<T> data;

    public CollectionSource(Iterable<T> data) {
        this.data = data;
    }

    @Override
    public void run(SourceContext<T> ctx) {
        for (T item : data) {
            ctx.collect(item);
        }
    }
}
