package org.miniflink.runtime;

import org.junit.jupiter.api.Test;
import org.miniflink.connector.CollectionSource;
import org.miniflink.execution.ForwardPartitioner;
import org.miniflink.runtime.operator.SourceOperatorImpl;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class SourceTaskTest {

    /** run 后下游 Channel 应依次收到数据记录、+∞ watermark 与 EOB。 */
    @Test
    void downstreamChannelReceivesDataAndEobAfterRun() throws Exception {
        Channel ch = new Channel(8);
        Output out = new Output(List.of(ch), new ForwardPartitioner(), null);
        SourceTask task = new SourceTask(
                new SourceOperatorImpl<>(new CollectionSource<>(List.of("a", "b"))),
                List.of(out), new RuntimeContextImpl(0, 1, null));

        task.run(); // 单线程直接驱动

        assertEquals("a", ((Record<String>) ch.receive()).value());
        assertEquals("b", ((Record<String>) ch.receive()).value());
        // source 结束广播 +∞ watermark（触发下游剩余窗口），再发 EOB
        assertEquals(Long.MAX_VALUE, ((Watermark) ch.receive()).getTimestamp());
        assertInstanceOf(EndOfBroadcast.class, ch.receive());
    }
}
