package org.miniflink.runtime;

/** EOB 哨兵：表示发送方不再发数据，驱动多线程管道级联关闭。单例。 */
public final class EndOfBroadcast implements StreamElement {
    public static final EndOfBroadcast INSTANCE = new EndOfBroadcast();

    private EndOfBroadcast() {
    }
}
