package org.miniflink.runtime;

import org.junit.jupiter.api.Test;
import org.miniflink.connector.CollectionSource;
import org.miniflink.execution.ForwardPartitioner;
import org.miniflink.runtime.operator.SourceOperatorImpl;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class SourceTaskTest {

    @Test
    void run后下游Channel应收到数据与EOB() throws Exception {
        Channel ch = new Channel(8);
        Output out = new Output(List.of(ch), new ForwardPartitioner(), null);
        SourceTask task = new SourceTask(
                new SourceOperatorImpl<>(new CollectionSource<>(List.of("a", "b"))),
                List.of(out), 0, 1);

        task.run(); // 单线程直接驱动

        assertEquals("a", ((Record<String>) ch.receive()).value());
        assertEquals("b", ((Record<String>) ch.receive()).value());
        assertInstanceOf(EndOfBroadcast.class, ch.receive());
    }
}
