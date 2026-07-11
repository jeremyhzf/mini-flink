package org.miniflink.runtime;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class ChannelWriterTest {

    /** 验证 collect 把值包装成 Record 写入 Channel。 */
    @Test
    void collectWrapsValueIntoRecordWrittenToChannel() throws Exception {
        Channel ch = new Channel(4);
        ChannelWriter<String> writer = new ChannelWriter<>(ch);

        writer.collect("hello");

        StreamElement e = ch.receive();
        assertInstanceOf(Record.class, e);
        assertEquals("hello", ((Record<String>) e).value());
    }

    /** 验证多次 collect 按序写入 Channel。 */
    @Test
    void multipleCollectsWriteInOrder() throws Exception {
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
