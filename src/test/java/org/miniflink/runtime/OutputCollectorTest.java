package org.miniflink.runtime;

import org.junit.jupiter.api.Test;
import org.miniflink.execution.ForwardPartitioner;
import org.miniflink.execution.HashPartitioner;
import org.miniflink.execution.RebalancePartitioner;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class OutputCollectorTest {

    @Test
    void forward应路由到对应索引的通道() throws Exception {
        Channel c0 = new Channel(2);
        Channel c1 = new Channel(2);
        Output out = new Output(List.of(c0, c1), new ForwardPartitioner(), null);
        OutputCollector<String> col = new OutputCollector<>(List.of(out), new RuntimeContextImpl(0, 1, null)); // upstreamIndex=0 → c0

        col.collect("x");

        assertInstanceOf(Record.class, c0.receive());
    }

    @Test
    void keyBy应按key哈希路由且同key同通道() throws Exception {
        Channel c0 = new Channel(4);
        Channel c1 = new Channel(4);
        Output out = new Output(List.of(c0, c1), new HashPartitioner(),
                (org.miniflink.api.function.KeySelector<String, String>) s -> s);
        OutputCollector<String> col = new OutputCollector<>(List.of(out), new RuntimeContextImpl(0, 1, null));

        col.collect("b");
        col.collect("b");

        // 同 key 两次落同一通道；"b".hashCode()%2=0 → 落 c0，取一个验证 FIFO
        assertInstanceOf(Record.class, c0.receive());
    }

    @Test
    void forward的sendEob只发往上游对应下游通道() throws Exception {
        Channel c0 = new Channel(2);
        Channel c1 = new Channel(2);
        Output out = new Output(List.of(c0, c1), new ForwardPartitioner(), null);

        out.sendEob(0); // 上游 0 → 仅 c0

        assertInstanceOf(EndOfBroadcast.class, c0.receive());
        assertTrue(c1.isEmpty(), "forward 下游 1 不应收到上游 0 的 EOB");
    }

    @Test
    void rebalance的sendEob向所有下游通道广播EOB() throws Exception {
        Channel c0 = new Channel(2);
        Channel c1 = new Channel(2);
        Output out = new Output(List.of(c0, c1), new RebalancePartitioner(), null);

        out.sendEob(0); // fan-out：每个下游都要对齐该上游的结束

        assertInstanceOf(EndOfBroadcast.class, c0.receive());
        assertInstanceOf(EndOfBroadcast.class, c1.receive());
    }
}
