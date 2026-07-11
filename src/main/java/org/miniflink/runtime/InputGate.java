package org.miniflink.runtime;

import java.util.List;
import java.util.function.Consumer;

/**
 * 下游 subtask 的输入聚合：N 个 InputChannel（每上游一个）。封装 Chandy-Lamport 对齐：
 * - barrier 在内部消费，不返回给调用方；某 channel barrier 到达 → 标记对齐 + 该 channel 后续元素缓冲。
 * - 所有 channel 对齐 → 回调 SnapshotCallback（task 快照）+ 经 barrierForwarder 广播 + 解除缓冲。
 * - 未对齐 channel 的元素正常放行。单 channel：barrier 立即对齐，零缓冲。
 */
public class InputGate {
    private final List<InputChannel> channels;
    private final SnapshotCallback callback;
    private final Consumer<Barrier> barrierForwarder;
    private int nextChannel = 0;
    private long aligningId = -1;   // 正在对齐的 barrier id；-1 = 无

    public InputGate(List<InputChannel> channels, SnapshotCallback callback,
                     Consumer<Barrier> barrierForwarder) {
        if (channels == null || channels.isEmpty()) {
            throw new IllegalArgumentException("InputGate 至少含一个 InputChannel");
        }
        this.channels = channels;
        this.callback = callback;
        this.barrierForwarder = barrierForwarder;
    }

    /** 返回下一个该由 task 处理的元素（Record/Watermark/EndOfBroadcast）；Barrier 在内部消费。 */
    public StreamElement receive() throws Exception {
        while (true) {
            StreamElement e = nextRaw();   // 阻塞取下一个原始元素（含优先放行缓冲）
            if (e instanceof Barrier b) {
                lastChannel().markAligned(b.getCheckpointId());
                if (aligningId < 0) {
                    aligningId = b.getCheckpointId();
                }
                if (allAligned(b.getCheckpointId())) {
                    callback.onAligned(b.getCheckpointId());
                    barrierForwarder.accept(b);
                    aligningId = -1;
                    for (InputChannel c : channels) {
                        c.resetAlignment();
                    }
                }
                continue;   // barrier 已消费
            }
            // 非 barrier：若该 channel 已对齐当前 barrier，缓冲；否则放行
            if (aligningId >= 0 && lastChannel().isAligned(aligningId)) {
                lastChannel().buffer(e);
                continue;
            }
            return e;
        }
    }

    /** 非阻塞探测：全部 channel 与缓冲均空时返回 null（测试/观测用，仅在対齐完成后调用）。 */
    public StreamElement pollNonBlocking() {
        // 先放行缓冲（仅对齐完成后才放行；対齐进行中缓冲须保留，否则已对齐 channel 的 record 会被错误提前放出）
        if (aligningId < 0) {
            for (InputChannel c : channels) {
                StreamElement b = c.pollBuffered();
                if (b != null) return b;
            }
        }
        for (InputChannel c : channels) {
            StreamElement e = c.poll();
            if (e != null) return e;
        }
        return null;
    }

    private int lastIdx = 0;

    /** 阻塞取下一个原始元素；优先放行已缓冲元素（对齐完成后缓冲先于新元素放行）。 */
    private StreamElement nextRaw() throws InterruptedException {
        // 仅対齐完成后（aligningId < 0）才排空缓冲：缓冲元素只在全部 channel 对齐后才该放行。
        // 若対齐进行中（aligningId >= 0）排空缓冲，被缓冲的 record 会被 receive() 见 aligningId>=0 && isAligned
        // 再次 buffer，形成 nextRaw↔receive 无限循环，且 nextRaw 永不再 poll 未对齐 channel 的 barrier → 硬死锁。
        if (aligningId < 0) {
            for (InputChannel c : channels) {
                StreamElement b = c.pollBuffered();
                if (b != null) {
                    lastIdx = channels.indexOf(c);
                    return b;
                }
            }
        }
        int n = channels.size();
        for (int i = 0; i < n; i++) {
            int idx = (nextChannel + i) % n;
            StreamElement e = channels.get(idx).poll();
            if (e != null) {
                lastIdx = idx;
                nextChannel = (idx + 1) % n;
                return e;
            }
        }
        // 全空：阻塞 take 当前轮询 channel
        lastIdx = nextChannel;
        StreamElement e = channels.get(nextChannel).take();
        nextChannel = (nextChannel + 1) % n;
        return e;
    }

    private InputChannel lastChannel() {
        return channels.get(lastIdx);
    }

    private boolean allAligned(long barrierId) {
        for (InputChannel c : channels) {
            if (!c.isAligned(barrierId)) {
                return false;
            }
        }
        return true;
    }
}
