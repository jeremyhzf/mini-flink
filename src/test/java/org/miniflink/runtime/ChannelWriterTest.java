package org.miniflink.runtime;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class ChannelWriterTest {

    @Test
    void collect应把值包装成Record写入Channel() throws Exception {
        Channel ch = new Channel(4);
        ChannelWriter<String> writer = new ChannelWriter<>(ch);

        writer.collect("hello");

        StreamElement e = ch.receive();
        assertInstanceOf(Record.class, e);
        assertEquals("hello", ((Record<String>) e).value());
    }

    @Test
    void 多次collect应按序写入() throws Exception {
        Channel ch = new Channel(8);
        ChannelWriter<Integer> writer = new ChannelWriter<>(ch);

        writer.collect(1);
        writer.collect(2);
        writer.collect(3);

        assertEquals(1, ((Record<Integer>) ch.receive()).value());
        assertEquals(2, ((Record<Integer>) ch.receive()).value());
        assertEquals(3, ((Record<Integer>) ch.receive()).value());
    }
}
