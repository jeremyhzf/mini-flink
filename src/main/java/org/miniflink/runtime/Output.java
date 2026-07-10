package org.miniflink.runtime;

import org.miniflink.api.function.KeySelector;
import org.miniflink.execution.Partitioner;

import java.util.List;

/**
 * 一个 fan-out 目标：下游 subtask 的输入 Channel 列表 + 分区器 + keySelector（hash 用）。
 * route 按分区器选一个 Channel 发送 Record；sendEob 向所有下游 Channel 广播 EOB。
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
    public void route(Object value, int upstreamIndex) throws Exception {
        Object key = (keySelector != null) ? ((KeySelector) keySelector).getKey(value) : null;
        int idx = partitioner.selectChannel(downstreamChannels.size(), key, upstreamIndex);
        downstreamChannels.get(idx).send(new Record<>(value));
    }

    /** 向所有下游 Channel 广播 EOB（关闭语义用）。 */
    public void sendEob() {
        for (Channel c : downstreamChannels) {
            try {
                c.send(EndOfBroadcast.INSTANCE);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("发送 EOB 被中断", e);
            }
        }
    }
}
