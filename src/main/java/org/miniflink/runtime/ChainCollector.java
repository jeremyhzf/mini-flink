package org.miniflink.runtime;

/**
 * Collector 实现：链内 forward——collect 时直接同步调用下游算子的 processElement（不经 Channel）。
 * 用于 OperatorChain 内相邻算子的连接。
 */
public class ChainCollector<T> implements Collector<T> {
    private final Operator<T, ?> downstream;

    public ChainCollector(Operator<T, ?> downstream) {
        this.downstream = downstream;
    }

    @Override
    public void collect(T record) {
        try {
            downstream.processElement(record);
        } catch (Exception e) {
            throw new RuntimeException("链内算子执行异常", e);
        }
    }

    @Override
    public void close() {
        // 链内无需操作
    }
}
