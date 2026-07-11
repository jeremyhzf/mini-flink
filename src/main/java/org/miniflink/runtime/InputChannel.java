package org.miniflink.runtime;

import java.util.ArrayDeque;
import java.util.Deque;

/**
 * InputGate 的单个上游通道：包装一个物理 Channel + 对齐状态 + 缓冲队列。
 * barrier 到达后该 channel 标记对齐，其后续元素缓冲，直到 InputGate 全部对齐后放行。
 */
public class InputChannel {
    private final Channel channel;
    private final Deque<StreamElement> buffered = new ArrayDeque<>();
    private long alignedBarrierId = -1;   // 已对齐的 barrier id；-1 = 未对齐

    public InputChannel(Channel channel) {
        this.channel = channel;
    }

    /** 非阻塞取一个元素（底层 Channel）。 */
    public StreamElement poll() {
        return channel.poll();
    }

    /** 阻塞取一个元素（底层 Channel）。 */
    public StreamElement take() throws InterruptedException {
        return channel.receive();
    }

    public boolean isAligned(long barrierId) {
        return alignedBarrierId == barrierId;
    }

    public void markAligned(long barrierId) {
        this.alignedBarrierId = barrierId;
    }

    public void resetAlignment() {
        this.alignedBarrierId = -1;
    }

    public void buffer(StreamElement e) {
        buffered.add(e);
    }

    /** 取一个缓冲元素（无则 null）。 */
    public StreamElement pollBuffered() {
        return buffered.poll();
    }
}
