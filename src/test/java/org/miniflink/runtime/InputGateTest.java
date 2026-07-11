package org.miniflink.runtime;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import static org.junit.jupiter.api.Assertions.*;

class InputGateTest {

    /** 喂给 InputChannel 一串预设元素（非真实 Channel）。 */
    private static InputChannel feed(StreamElement... elements) {
        Channel ch = new Channel();
        for (StreamElement e : elements) {
            try { ch.send(e); } catch (InterruptedException ex) { throw new RuntimeException(ex); }
        }
        return new InputChannel(ch);
    }

    @Test
    void singleChannelBarrierTriggersAlignmentImmediately() throws Exception {
        long[] aligned = {0};
        Deque<Barrier> forwarded = new ArrayDeque<>();
        Consumer<Barrier> fwd = forwarded::add;
        // feed 末尾追加 EOB 哨兵：消费 barrier 后的下一次 receive 不致阻塞
        InputGate gate = new InputGate(List.of(feed(new Record<>("a", 0), new Barrier(1L), EndOfBroadcast.INSTANCE)),
                id -> aligned[0] = id, fwd);
        // 首个元素是 Record（barrier 之前），放行
        assertInstanceOf(Record.class, gate.receive());
        // 下一次 receive：Barrier 在内部消费（单 channel 立即对齐 → 回调 + 转发），继续循环返回 EOB
        assertEquals(EndOfBroadcast.INSTANCE, gate.receive());
        assertNull(gate.pollNonBlocking());   // barrier 已被消费，队列全空
        assertEquals(1L, aligned[0]);
        assertEquals(1L, forwarded.poll().getCheckpointId());
    }

    @Test
    void multiChannelAlignsAfterAllBarriersAndBuffersEarlyChannel() throws Exception {
        long[] aligned = {0};
        InputChannel a = feed(new Record<>("a1", 0), new Barrier(2L), new Record<>("a2", 0));
        InputChannel b = feed(new Record<>("b1", 0), new Barrier(2L), new Record<>("b2", 0));
        InputGate gate = new InputGate(List.of(a, b), id -> aligned[0] = id, b2 -> {});

        // 取出 barrier 之前的 record（a1, b1）——顺序由轮询决定，两个都要被放行
        Object first = gate.receive();      // a1 或 b1
        Object second = gate.receive();     // 另一个
        assertInstanceOf(Record.class, first);
        assertInstanceOf(Record.class, second);

        // 此时两个 channel 各自的 barrier 到达 → 全部对齐
        // 触发 receive 直到对齐完成（barrier 不返回），aligned 被设置
        // 继续接收 barrier 之后的 record（a2, b2 应在对齐后被放行）
        Object third = gate.receive();
        Object fourth = gate.receive();
        assertInstanceOf(Record.class, third);
        assertInstanceOf(Record.class, fourth);
        assertEquals(2L, aligned[0]);
    }

    /**
     * 真触发缓冲路径且不死锁：a 的 barrier 先到（a 立即对齐），a 的后续 record 须被缓冲；
     * 在 b 的 barrier 到达前，nextRaw 必须继续 poll b（而非反复排空 a 的缓冲），否则死锁。
     * 复现 Critical-1：旧实现 nextRaw 无条件排空缓冲 → nextRaw↔receive 死循环 → b 的 barrier 永不消费 → 硬死锁。
     * 修复后：対齐进行中（aligningId>=0）不排空缓冲，b 的 barrier 正常消费 → 全部对齐 → 缓冲放行。
     */
    @Test
    @Timeout(10)
    void partialAlignmentBuffersEarlyChannelRecordWithoutDeadlock() throws Exception {
        long[] aligned = {0};
        Deque<Barrier> forwarded = new ArrayDeque<>();
        // a 的 barrier 在前：barrier 后的 a2 须被缓冲；b 的 record（未对齐 channel）正常放行
        // 各加 EOB 防止对齐后的 take 阻塞
        InputChannel a = feed(new Barrier(2L), new Record<>("a2", 0), EndOfBroadcast.INSTANCE);
        InputChannel b = feed(new Record<>("b1", 0), new Barrier(2L), EndOfBroadcast.INSTANCE);
        InputGate gate = new InputGate(List.of(a, b), id -> aligned[0] = id, forwarded::add);

        // 第 1 次：b1（未对齐 channel 的 record 放行）；a 的 a2 被缓冲
        Object first = gate.receive();
        assertInstanceOf(Record.class, first);
        assertEquals("b1", ((Record<?>) first).value());

        // 第 2 次：a2（全部对齐完成后 aligningId=-1，缓冲经 nextRaw 放行分支放出）
        Object second = gate.receive();
        assertInstanceOf(Record.class, second);
        assertEquals("a2", ((Record<?>) second).value());

        // 对齐已完成：回调触发 + barrier 已转发
        assertEquals(2L, aligned[0]);
        assertEquals(2L, forwarded.poll().getCheckpointId());

        // 第 3、4 次：两个 EOB（关键是不死锁：测试在 @Timeout(10) 内正常结束）
        assertEquals(EndOfBroadcast.INSTANCE, gate.receive());
        assertEquals(EndOfBroadcast.INSTANCE, gate.receive());
        assertNull(gate.pollNonBlocking());
    }

    /**
     * 多轮 checkpoint 对齐验收：第 1 轮对齐 + reset 后，第 2 轮 barrier 仍正确对齐。
     * 验证 aligningId 重置 + 各 channel resetAlignment 后多轮语义不退化（Phase 2 周期 coordinator 反复触发 barrier 的前置依赖）。
     *
     * 数据（两 channel 对称，每轮两 barrier 几乎同时到达，专注轮间 reset 正确性而非缓冲路径——
     * 缓冲路径已由 partialAlignmentBuffersEarlyChannelRecordWithoutDeadlock 覆盖）：
     *   a = [Record("a1"), Barrier(1), Record("a2"), Barrier(2), EOB]
     *   b = [Record("b1"), Barrier(1), Record("b2"), Barrier(2), EOB]
     *
     * 推演（aligningId<0 才排空缓冲的门控下）：
     *   receive#1 → a1, #2 → b1（aligningId<0 放行）
     *   #3 内部：a.poll=Barrier(1)→a 对齐、aligningId=1、未全齐；
     *           b.poll=Barrier(1)→全齐→onAligned(1)+forward Barrier(1)+aligningId=-1+reset；
     *           a.poll=a2 放行 → 返回 a2
     *   #4 → b2
     *   #5 内部：同理对齐 Barrier(2)→onAligned(2)+forward Barrier(2)+reset；a.poll=EOB → 返回 EOB
     *   #6 → b 的 EOB
     * 故 alignedIds=[1,2]、forwarded=[Barrier(1),Barrier(2)]、4 条 record 全放行。
     */
    @Test
    @Timeout(10)
    void multipleConsecutiveAlignmentRoundsResetCorrectly() throws Exception {
        List<Long> alignedIds = new ArrayList<>();
        List<Barrier> forwarded = new ArrayList<>();
        InputChannel a = feed(new Record<>("a1", 0), new Barrier(1L), new Record<>("a2", 0),
                new Barrier(2L), EndOfBroadcast.INSTANCE);
        InputChannel b = feed(new Record<>("b1", 0), new Barrier(1L), new Record<>("b2", 0),
                new Barrier(2L), EndOfBroadcast.INSTANCE);
        InputGate gate = new InputGate(List.of(a, b), id -> alignedIds.add(id), forwarded::add);

        List<Object> emitted = new ArrayList<>();
        int eobSeen = 0;
        // 每个 channel 末尾一个 EOB，收齐 2 个即表示两 channel 数据全消费完
        while (eobSeen < 2) {
            Object e = gate.receive();
            if (e instanceof EndOfBroadcast) {
                eobSeen++;
            } else {
                emitted.add(e);
            }
        }

        // 两轮对齐：onAligned 以 id=1 和 id=2 各触发一次（轮间 reset 后第 2 轮仍正确对齐）
        assertEquals(List.of(1L, 2L), alignedIds);
        // 两个 barrier 都转发到下游
        assertEquals(2, forwarded.size());
        assertEquals(1L, forwarded.get(0).getCheckpointId());
        assertEquals(2L, forwarded.get(1).getCheckpointId());
        // 4 条 record 全部经 receive() 放行（不丢、不被 barrier 消费误吞）
        Set<String> values = new HashSet<>();
        for (Object r : emitted) {
            assertInstanceOf(Record.class, r);
            values.add((String) ((Record<?>) r).value());
        }
        assertEquals(Set.of("a1", "b1", "a2", "b2"), values);
        // 全部消费完毕，无残留
        assertNull(gate.pollNonBlocking());
    }
}
