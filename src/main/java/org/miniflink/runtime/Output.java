package org.miniflink.runtime;

import org.miniflink.api.function.KeySelector;
import org.miniflink.execution.ForwardPartitioner;
import org.miniflink.execution.Partitioner;

import java.util.List;

/**
 * 一个 fan-out 目标：下游 subtask 的输入 Channel 列表 + 分区器 + keySelector（hash 用）。
 * route 按分区器选一个 Channel 发送 Record；sendEob 按分区器语义向下游发送 EOB（关闭用）。
 */
public class Output {
    private final List<Channel> downstreamChannels;
    private final Partitioner partitioner;
    private final KeySelector<?, ?> keySelector;

    public Output(List<Channel> downstreamChannels, Partitioner partitioner, KeySelector<?, ?> keySelector) {
        this.downstreamChannels = downstreamChannels;
        this.partitioner = partitioner;
        this.keySelector = keySelector;
    }

    public List<Channel> getDownstreamChannels() {
        return downstreamChannels;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public void route(Object value, long timestamp, int upstreamIndex) throws Exception {
        Object key = (keySelector != null) ? ((KeySelector) keySelector).getKey(value) : null;
        int idx = partitioner.selectChannel(downstreamChannels.size(), key, upstreamIndex);
        downstreamChannels.get(idx).send(new Record<>(value, timestamp));
    }

    /**
     * 关闭语义：向下游发送 EOB，分区器感知。
     * - forward（一对一）：只向上游对应的下游通道（downstreamChannels[upstreamIndex]）发 EOB；
     *   因下游 i 只接收上游 i 的数据，其他上游不应向其发 EOB，否则 EOB 可能在真实记录之前/之间到达，
     *   导致下游 i 提前退出并丢数据（forward 下游对齐只计 1 个 EOB）。
     * - 其他分区器（rebalance/hash 等 fan-out）：向所有下游通道广播 EOB（每个下游都要对齐该上游的结束）。
     */
    public void sendEob(int upstreamIndex) {
        if (partitioner instanceof ForwardPartitioner) {
            try {
                downstreamChannels.get(upstreamIndex).send(EndOfBroadcast.INSTANCE);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("发送 EOB 被中断", e);
            }
            return;
        }
        for (Channel c : downstreamChannels) {
            try {
                c.send(EndOfBroadcast.INSTANCE);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("发送 EOB 被中断", e);
            }
        }
    }

    /** 向所有下游 channel 广播 watermark（watermark 不分区）。 */
    public void sendWatermark(Watermark wm) {
        for (Channel c : downstreamChannels) {
            try {
                c.send(wm);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("发送 Watermark 被中断", e);
            }
        }
    }
}
