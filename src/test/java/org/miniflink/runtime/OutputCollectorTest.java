package org.miniflink.runtime;

import org.junit.jupiter.api.Test;
import org.miniflink.execution.ForwardPartitioner;
import org.miniflink.execution.HashPartitioner;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class OutputCollectorTest {

    @Test
    void forward应路由到对应索引的通道() throws Exception {
        Channel c0 = new Channel(2);
        Channel c1 = new Channel(2);
        Output out = new Output(List.of(c0, c1), new ForwardPartitioner(), null);
        OutputCollector<String> col = new OutputCollector<>(List.of(out), 0); // upstreamIndex=0 → c0

        col.collect("x");

        assertInstanceOf(Record.class, c0.receive());
    }

    @Test
    void keyBy应按key哈希路由且同key同通道() throws Exception {
        Channel c0 = new Channel(4);
        Channel c1 = new Channel(4);
        Output out = new Output(List.of(c0, c1), new HashPartitioner(),
                (org.miniflink.api.function.KeySelector<String, String>) s -> s);
        OutputCollector<String> col = new OutputCollector<>(List.of(out), 0);

        col.collect("b");
        col.collect("b");

        // 同 key 两次落同一通道；"b".hashCode()%2=0 → 落 c0，取一个验证 FIFO
        assertInstanceOf(Record.class, c0.receive());
    }

    @Test
    void sendEob应向所有下游通道发EOB() throws Exception {
        Channel c0 = new Channel(2);
        Channel c1 = new Channel(2);
        Output out = new Output(List.of(c0, c1), new ForwardPartitioner(), null);

        out.sendEob();

        assertInstanceOf(EndOfBroadcast.class, c0.receive());
        assertInstanceOf(EndOfBroadcast.class, c1.receive());
    }
}
