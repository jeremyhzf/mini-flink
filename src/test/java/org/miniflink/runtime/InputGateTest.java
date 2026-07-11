package org.miniflink.runtime;

import org.junit.jupiter.api.Test;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
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
}
