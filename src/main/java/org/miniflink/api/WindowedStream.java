package org.miniflink.api;

import org.miniflink.api.function.ReduceFunction;
import org.miniflink.execution.HashPartitioner;
import org.miniflink.runtime.operator.WindowOperator;
import org.miniflink.window.Window;
import org.miniflink.window.WindowAssigner;

/**
 * keyBy + window 返回的流：提供窗口聚合操作。
 * reduce 建一个 hash 分区的 WindowOperator transformation（沿用 KeyedStream 的 keySelector）。
 */
public class WindowedStream<T, W extends Window> {
    private final KeyedStream<T, ?> input;
    private final WindowAssigner<T, W> windowAssigner;

    public WindowedStream(KeyedStream<T, ?> input, WindowAssigner<T, W> windowAssigner) {
        this.input = input;
        this.windowAssigner = windowAssigner;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public DataStream<T> reduce(ReduceFunction<T> reduceFn) {
        WindowOperator<T> op = new WindowOperator<>((WindowAssigner) windowAssigner, reduceFn);
        return input.keyedTransformFor("window-reduce", op);
    }
}
