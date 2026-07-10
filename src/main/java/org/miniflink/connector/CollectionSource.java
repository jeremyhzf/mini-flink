package org.miniflink.connector;

import org.miniflink.api.function.SourceFunction;
import org.miniflink.runtime.SourceContext;

/** 内置 source：从 Iterable 读取，按 subtaskIndex 取模分片（元素 i → i % parallelism == subtaskIndex 的 subtask）。 */
public class CollectionSource<T> implements SourceFunction<T> {
    private final Iterable<T> data;

    public CollectionSource(Iterable<T> data) {
        this.data = data;
    }

    @Override
    public void run(SourceContext<T> ctx) {
        int parallelism = ctx.getParallelism();
        int subtask = ctx.getSubtaskIndex();
        int idx = 0;
        for (T item : data) {
            if (idx % parallelism == subtask) {
                ctx.collect(item);
            }
            idx++;
        }
    }
}
