package org.miniflink.runtime;

/**
 * Collector 实现：把 collect 的值包装成 Record 写入下游 Channel。
 * 算子通过 Collector 接口输出，对通道无感知（稳定边界）。
 */
public class ChannelWriter<T> implements Collector<T> {
    private final Channel channel;

    public ChannelWriter(Channel channel) {
        this.channel = channel;
    }

    @Override
    public void collect(T record) {
        try {
            channel.send(new Record<>(record, Long.MIN_VALUE));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("通道发送被中断", e);
        }
    }

    @Override
    public void close() {
        // EOB 由 Task 统一发送，ChannelWriter 无需操作
    }
}
