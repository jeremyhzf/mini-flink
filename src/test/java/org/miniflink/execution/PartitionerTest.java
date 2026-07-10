package org.miniflink.execution;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class PartitionerTest {

    @Test
    void forward返回上游索引() {
        ForwardPartitioner p = new ForwardPartitioner();
        assertEquals(0, p.selectChannel(2, null, 0));
        assertEquals(1, p.selectChannel(2, null, 1));
    }

    @Test
    void forward要求上下游并行度相同() {
        ForwardPartitioner p = new ForwardPartitioner();
        assertThrows(IllegalStateException.class,
                () -> p.selectChannel(2, null, 5)); // upstreamIndex >= numDownstream
    }

    @Test
    void hash同key落同一通道() {
        HashPartitioner p = new HashPartitioner();
        int c1 = p.selectChannel(4, "key-a", 0);
        int c2 = p.selectChannel(4, "key-a", 1); // 不同上游，同 key
        assertEquals(c1, c2);
        assertTrue(c1 >= 0 && c1 < 4);
    }

    @Test
    void rebalance轮询分发() {
        RebalancePartitioner p = new RebalancePartitioner();
        assertEquals(0, p.selectChannel(2, null, 0));
        assertEquals(1, p.selectChannel(2, null, 0));
        assertEquals(0, p.selectChannel(2, null, 0));
    }
}
