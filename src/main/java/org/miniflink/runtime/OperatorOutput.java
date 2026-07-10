package org.miniflink.runtime;

/** Collector 实现：collect 时同步调用下游算子的 processElement（阶段①同步链）。 */
public class OperatorOutput<T> implements Collector<T> {
    private final Operator<T, ?> next;

    public OperatorOutput(Operator<T, ?> next) {
        this.next = next;
    }

    @Override
    public void collect(T record) {
        try {
            next.processElement(record);
        } catch (Exception e) {
            // 阶段①把算子异常包装成 RuntimeException 向上传播
            throw new RuntimeException("算子执行异常", e);
        }
    }

    @Override
    public void close() {
        // 下游算子的 close 由执行器统一调用
    }
}
