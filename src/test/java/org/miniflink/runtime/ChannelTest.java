package org.miniflink.runtime;

import org.junit.jupiter.api.Test;
import java.util.concurrent.TimeUnit;
import static org.junit.jupiter.api.Assertions.*;

class ChannelTest {

    @Test
    void send与receive应FIFO传递元素() throws Exception {
        Channel ch = new Channel(4);
        ch.send(new Record<>("a"));
        ch.send(new Record<>("b"));
        ch.send(EndOfBroadcast.INSTANCE);

        assertInstanceOf(Record.class, ch.receive());                  // 第一个 = a
        assertEquals("b", ((Record<?>) ch.receive()).value());        // 取第二个 Record 校验内容
        assertInstanceOf(EndOfBroadcast.class, ch.receive());         // 第三个，FIFO（EOB）
    }

    @Test
    void 容量满时send应阻塞() throws Exception {
        Channel ch = new Channel(1); // 容量 1
        ch.send(new Record<>("x")); // 占满
        // 再 send 应阻塞；用另一个线程验证
        Thread blocker = new Thread(() -> {
            try { ch.send(new Record<>("y")); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        });
        blocker.start();
        Thread.sleep(100);
        assertEquals(Thread.State.WAITING, blocker.getState()); // 阻塞中（=反压）
        blocker.interrupt();
    }

    @Test
    void 空通道receive应阻塞() throws Exception {
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
