package org.miniflink.runtime;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * 有界通道：包装有界 BlockingQueue。
 * send = put（满则阻塞生产者 = 天然反压）；receive = take（空则阻塞消费者）。
 * 不提供 close：关闭完全由 EOB 哨兵驱动（见 Task 6）。
 */
public class Channel {
    public static final int DEFAULT_CAPACITY = 64;

    private final BlockingQueue<StreamElement> queue;

    public Channel(int capacity) {
        this.queue = new LinkedBlockingQueue<>(capacity);
    }

    public Channel() {
        this(DEFAULT_CAPACITY);
    }

    /** 发送元素；队列满则阻塞当前线程，直到有空间（反压）。 */
    public void send(StreamElement e) throws InterruptedException {
        queue.put(e);
    }

    /** 接收元素；队列空则阻塞，直到有元素可用。 */
    public StreamElement receive() throws InterruptedException {
        return queue.take();
    }

    /** 非阻塞接收：队列空返回 null（InputGate 轮询用）。 */
    public StreamElement poll() {
        return queue.poll();
    }

    /** 队列是否为空（仅供测试/观测用）。 */
    public boolean isEmpty() {
        return queue.isEmpty();
    }
}
