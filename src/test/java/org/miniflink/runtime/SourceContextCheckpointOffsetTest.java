package org.miniflink.runtime;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Task 11 审查修复的锁定测试：恢复 skip 期 checkpoint 上报坐标系一致性。
 *
 * <p>根因：{@code restoreOffset(M)} 把 emitted 重置为 0，恢复重放时 collect() 对 record 0..M-1
 * 走 skip 分支（不转发），但仍执行 checkpoint 发射块。修复前上报 {@code offset=emitted=k<M}（相对坐标），
 * 而下游 reduce.acc=M（绝对坐标，skip 期无数据流入）→ checkpoint={offset=k, acc=M} 不一致。
 * 若此 skip 窗口发生第二次故障，下次恢复从此 checkpoint：source 重放 k..N + reduce acc=M →
 * records k..M-1 双重累加 → exactly-once 破坏。
 *
 * <p>修复：collect()/drainPending() 上报 {@code Math.max(emitted, skipUntil)}：skip 期报 skipUntil
 * （绝对值）与下游同坐标系；冷启（skipUntil=0）/正常后（emitted>=skipUntil）max 取 emitted，行为不变。
 *
 * <p>本测试用确定性单测锁定该行为（替代难复现的多次故障集成测试）。
 */
class SourceContextCheckpointOffsetTest {

    /** 捕获 (checkpointId, offset) 对的 CheckpointEmitter。 */
    static final class CapturingEmitter implements SourceContextImpl.CheckpointEmitter {
        final List<long[]> emitted = new ArrayList<>();
        @Override
        public void emit(long checkpointId, long offset) {
            emitted.add(new long[]{checkpointId, offset});
        }
    }

    /** 触发一次 checkpoint + collect 一条记录，返回本次 emit 的 offset（确定性触发一次 emit）。 */
    private static long emitOffsetAfterCollect(SourceContextImpl<String> ctx, CapturingEmitter cap,
                                               long checkpointId, String record) {
        int before = cap.emitted.size();
        ctx.requestCheckpoint(checkpointId);
        ctx.collect(record);
        assertEquals(before + 1, cap.emitted.size(), "collect 应触发一次 checkpoint emit");
        return cap.emitted.get(cap.emitted.size() - 1)[1];
    }

    /** 冷启（skipUntil=0）时 checkpoint 上报 offset 等于 emitted。 */
    @Test
    void coldStartCheckpointReportsOffsetEqualsEmitted() {
        ListCollector<String> out = new ListCollector<>();
        SourceContextImpl<String> ctx = new SourceContextImpl<>(out, 0, 1);
        CapturingEmitter cap = new CapturingEmitter();
        ctx.setCheckpointEmitter(cap);

        // emitted=0 → max(0,0)=0；之后 emitted=1 → max(1,0)=1；emitted=2 → max(2,0)=2
        assertEquals(0L, emitOffsetAfterCollect(ctx, cap, 1L, "a"));
        assertEquals(1L, emitOffsetAfterCollect(ctx, cap, 2L, "b"));
        assertEquals(2L, emitOffsetAfterCollect(ctx, cap, 3L, "c"));
        // 冷启无 skip，全部转发
        assertEquals(List.of("a", "b", "c"), out.getResult());
    }

    /** 恢复 skip 期（emitted<skipUntil）checkpoint 上报绝对 offset 等于 skipUntil。 */
    @Test
    void restoreSkipPhaseReportsAbsoluteOffsetEqualsSkipUntil() {
        ListCollector<String> out = new ListCollector<>();
        SourceContextImpl<String> ctx = new SourceContextImpl<>(out, 0, 1);
        ctx.restoreOffset(5L);   // skipUntil=5, emitted=0
        CapturingEmitter cap = new CapturingEmitter();
        ctx.setCheckpointEmitter(cap);

        // skip 期 emitted=0,1,2,3,4 各触发一次 checkpoint，均应上报 5（=skipUntil 绝对坐标）
        for (int i = 0; i < 5; i++) {
            long got = emitOffsetAfterCollect(ctx, cap, (long) (i + 1), "skip-" + i);
            assertEquals(5L, got,
                    "skip 期（emitted=" + i + "）应上报绝对 offset=skipUntil=5，而非相对值 " + i);
        }
        // skip 期 5 条全部丢弃（重放已发条数），下游无数据流入 → acc 保持绝对坐标
        assertTrue(out.getResult().isEmpty(), "skip 期记录不应转发到下游");
    }

    /** 越过 skip 后（emitted>=skipUntil）checkpoint 上报 offset 等于 emitted。 */
    @Test
    void afterSkipCheckpointReportsOffsetEqualsEmitted() {
        ListCollector<String> out = new ListCollector<>();
        SourceContextImpl<String> ctx = new SourceContextImpl<>(out, 0, 1);
        ctx.restoreOffset(5L);
        CapturingEmitter cap = new CapturingEmitter();
        ctx.setCheckpointEmitter(cap);

        // 先走完 5 条 skip（emitted: 0→5）
        for (int i = 0; i < 5; i++) {
            ctx.requestCheckpoint(100L + i);
            ctx.collect("skip-" + i);
        }
        // 越过 skip：emitted=5 第一条转发前 → max(5,5)=5
        assertEquals(5L, emitOffsetAfterCollect(ctx, cap, 200L, "real-0"));
        // emitted=6 → max(6,5)=6
        assertEquals(6L, emitOffsetAfterCollect(ctx, cap, 201L, "real-1"));
        // emitted=7 → max(7,5)=7
        assertEquals(7L, emitOffsetAfterCollect(ctx, cap, 202L, "real-2"));
        assertEquals(List.of("real-0", "real-1", "real-2"), out.getResult());
    }

    /** drainPending 在 skip 期上报绝对 offset（与 collect 同坐标系）。 */
    @Test
    void drainPendingReportsAbsoluteOffsetInSkipPhase() {
        ListCollector<String> out = new ListCollector<>();
        SourceContextImpl<String> ctx = new SourceContextImpl<>(out, 0, 1);
        ctx.restoreOffset(5L);   // emitted=0, skipUntil=5
        CapturingEmitter cap = new CapturingEmitter();
        ctx.setCheckpointEmitter(cap);

        ctx.requestCheckpoint(999L);
        ctx.drainPending();
        assertEquals(1, cap.emitted.size());
        assertEquals(999L, cap.emitted.get(0)[0]);
        assertEquals(5L, cap.emitted.get(0)[1],
                "drainPending 在 skip 期应上报绝对 offset=skipUntil=5");
    }

    /** drainPending 在正常后（emitted>=skipUntil）上报 emitted。 */
    @Test
    void drainPendingReportsEmittedInNormalPhase() {
        ListCollector<String> out = new ListCollector<>();
        SourceContextImpl<String> ctx = new SourceContextImpl<>(out, 0, 1);
        CapturingEmitter cap = new CapturingEmitter();
        ctx.setCheckpointEmitter(cap);

        ctx.collect("a");
        ctx.collect("b");
        ctx.collect("c");
        assertEquals(3L, ctx.snapshotOffset());

        ctx.requestCheckpoint(7L);
        ctx.drainPending();
        assertEquals(1, cap.emitted.size());
        assertEquals(7L, cap.emitted.get(0)[0]);
        assertEquals(3L, cap.emitted.get(0)[1],
                "drainPending 正常后应上报 offset=emitted=3");
    }

    /** 无 checkpoint 请求时 collect 不触发 emit（边界确认）。 */
    @Test
    void collectDoesNotEmitWithoutCheckpointRequest() {
        ListCollector<String> out = new ListCollector<>();
        SourceContextImpl<String> ctx = new SourceContextImpl<>(out, 0, 1);
        CapturingEmitter cap = new CapturingEmitter();
        ctx.setCheckpointEmitter(cap);

        ctx.collect("a");
        ctx.collect("b");
        assertTrue(cap.emitted.isEmpty(), "无 checkpoint 请求时 collect 不应触发 emit");
        assertEquals(List.of("a", "b"), out.getResult());
    }
}
