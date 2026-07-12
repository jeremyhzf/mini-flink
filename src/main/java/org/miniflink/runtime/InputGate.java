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
    /** 已关闭 channel（其 EOB 已被消费、上游不再发数据）；take 不得作用于已关闭 channel，否则永久阻塞。 */
    private final boolean[] channelClosed;

    public InputGate(List<InputChannel> channels, SnapshotCallback callback,
                     Consumer<Barrier> barrierForwarder) {
        if (channels == null || channels.isEmpty()) {
            throw new IllegalArgumentException("InputGate 至少含一个 InputChannel");
        }
        this.channels = channels;
        this.callback = callback;
        this.barrierForwarder = barrierForwarder;
        this.channelClosed = new boolean[channels.size()];
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
                if (b != null) {
                    return b;
                }
            }
        }
        for (InputChannel c : channels) {
            StreamElement e = c.poll();
            if (e != null) {
                return e;
            }
        }
        return null;
    }

    private int lastIdx = 0;

    /** 阻塞取下一个原始元素；优先放行已缓冲元素（对齐完成后缓冲先于新元素放行）。 */
    private StreamElement nextRaw() throws InterruptedException {
        StreamElement e = null;
        // 仅対齐完成后（aligningId < 0）才排空缓冲：缓冲元素只在全部 channel 对齐后才该放行。
        // 若対齐进行中（aligningId >= 0）排空缓冲，被缓冲的 record 会被 receive() 见 aligningId>=0 && isAligned
        // 再次 buffer，形成 nextRaw↔receive 无限循环，且 nextRaw 永不再 poll 未对齐 channel 的 barrier → 硬死锁。
        if (aligningId < 0) {
            for (int i = 0; i < channels.size(); i++) {
                StreamElement b = channels.get(i).pollBuffered();
                if (b != null) {
                    e = b;
                    lastIdx = i;
                    break;
                }
            }
        }
        // 非阻塞轮询各 channel
        if (e == null) {
            int n = channels.size();
            for (int i = 0; i < n; i++) {
                int idx = (nextChannel + i) % n;
                StreamElement p = channels.get(idx).poll();
                if (p != null) {
                    e = p;
                    lastIdx = idx;
                    nextChannel = (idx + 1) % n;
                    break;
                }
            }
        }
        // poll 全空：阻塞 take 一个【活跃】channel。
        // 关键修复：已关闭 channel（EOB 已被消费、上游不再发数据）的 take() 会永久阻塞，
        // 而其他仍活跃 channel 的数据无人读取 → fan-in 下整个 task 挂起 → StreamExecutor.join(30s) 超时。
        // 故 take 只作用于尚未关闭的 channel；活跃 channel 即使瞬时为空，上游存活下必然再发数据或 EOB，take 必返回。
        if (e == null) {
            int n = channels.size();
            int takeIdx = -1;
            for (int i = 0; i < n; i++) {
                int idx = (nextChannel + i) % n;
                if (!channelClosed[idx]) {
                    takeIdx = idx;
                    break;
                }
            }
            if (takeIdx < 0) {
                // 所有 channel 均已关闭：理论上不会到这——OperatorTask 收齐 N 个 EOB 即 remaining=0 退出主循环，不再调 receive/nextRaw。
                // 防御性抛错，避免静默挂起。
                throw new IllegalStateException("InputGate 全部 channel 已关闭");
            }
            lastIdx = takeIdx;
            nextChannel = (takeIdx + 1) % n;
            e = channels.get(takeIdx).take();
        }
        // 统一处理：返回的若是 EOB，标记该 channel 已关闭（其 EOB 已被消费、上游不再发数据，
        // 后续 take 作用于它会永久阻塞）。
        if (e == EndOfBroadcast.INSTANCE) {
            channelClosed[lastIdx] = true;
        }
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
