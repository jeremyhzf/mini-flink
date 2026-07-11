package org.miniflink.runtime;

import org.junit.jupiter.api.Test;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

class BarrierTest {

    @Test
    void barrierCarriesCheckpointId() {
        Barrier b = new Barrier(7L);
        assertEquals(7L, b.getCheckpointId());
    }

    @Test
    void sendBarrierBroadcastsToAllDownstreamChannels() throws Exception {
        Channel c1 = new Channel();
        Channel c2 = new Channel();
        // Output 不需要分区器语义即可广播 barrier：用 rebalance 占位（2 个下游）
        Output output = new Output(List.of(c1, c2),
                new org.miniflink.execution.RebalancePartitioner(), null);
        output.sendBarrier(new Barrier(3L));
        // 每个 channel 只 receive 一次（第二次会阻塞）：c1 验 id（强转非 Barrier 会 ClassCastException）
        assertEquals(3L, ((Barrier) c1.receive()).getCheckpointId());
        // c2 也各收一个 barrier（验证类型）
        assertInstanceOf(Barrier.class, c2.receive());
    }
}
