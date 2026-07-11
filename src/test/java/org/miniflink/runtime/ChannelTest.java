package org.miniflink.runtime;

import org.junit.jupiter.api.Test;
import java.util.concurrent.TimeUnit;
import static org.junit.jupiter.api.Assertions.*;

class ChannelTest {

    /** 验证 send 与 receive 按 FIFO 顺序传递元素（含 Record 与 EOB）。 */
    @Test
    void sendAndReceiveTransferElementsInFifoOrder() throws Exception {
        Channel ch = new Channel(4);
        ch.send(new Record<>("a", 0L));
        ch.send(new Record<>("b", 0L));
        ch.send(EndOfBroadcast.INSTANCE);

        assertInstanceOf(Record.class, ch.receive());                  // 第一个 = a
        assertEquals("b", ((Record<?>) ch.receive()).value());        // 取第二个 Record 校验内容
        assertInstanceOf(EndOfBroadcast.class, ch.receive());         // 第三个，FIFO（EOB）
    }

    /** 验证通道容量满时 send 阻塞（反压）。 */
    @Test
    void sendBlocksWhenChannelFull() throws Exception {
        Channel ch = new Channel(1); // 容量 1
        ch.send(new Record<>("x", 0L)); // 占满
        // 再 send 应阻塞；用另一个线程验证
        Thread blocker = new Thread(() -> {
            try { ch.send(new Record<>("y", 0L)); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        });
        blocker.start();
        Thread.sleep(100);
        assertEquals(Thread.State.WAITING, blocker.getState()); // 阻塞中（=反压）
        blocker.interrupt();
    }

    /** 验证空通道 receive 阻塞。 */
    @Test
    void receiveBlocksOnEmptyChannel() throws Exception {
        Channel ch = new Channel(2);
        Thread[] holder = new Thread[1];
        holder[0] = new Thread(() -> {
            try { ch.receive(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        });
        holder[0].start();
        Thread.sleep(100);
        assertEquals(Thread.State.WAITING, holder[0].getState());
        holder[0].interrupt();
    }
}
