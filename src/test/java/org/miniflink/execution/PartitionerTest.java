package org.miniflink.execution;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class PartitionerTest {

    /** forward 分区器返回与上游 subtask 索引相同的下游通道。 */
    @Test
    void forwardReturnsUpstreamIndex() {
        ForwardPartitioner p = new ForwardPartitioner();
        assertEquals(0, p.selectChannel(2, null, 0));
        assertEquals(1, p.selectChannel(2, null, 1));
    }

    /** forward 分区器在上下游并行度不一致（upstreamIndex >= numDownstream）时抛 IllegalStateException。 */
    @Test
    void forwardRequiresSameUpstreamDownstreamParallelism() {
        ForwardPartitioner p = new ForwardPartitioner();
        assertThrows(IllegalStateException.class,
                () -> p.selectChannel(2, null, 5)); // upstreamIndex >= numDownstream
    }

    /** hash 分区器保证相同 key 无论来自哪个上游都路由到同一通道。 */
    @Test
    void hashRoutesSameKeyToSameChannel() {
        HashPartitioner p = new HashPartitioner();
        int c1 = p.selectChannel(4, "key-a", 0);
        int c2 = p.selectChannel(4, "key-a", 1); // 不同上游，同 key
        assertEquals(c1, c2);
        assertTrue(c1 >= 0 && c1 < 4);
    }

    /** rebalance 分区器以轮询方式在通道间循环分发数据。 */
    @Test
    void rebalanceDistributesRoundRobin() {
        RebalancePartitioner p = new RebalancePartitioner();
        assertEquals(0, p.selectChannel(2, null, 0));
        assertEquals(1, p.selectChannel(2, null, 0));
        assertEquals(0, p.selectChannel(2, null, 0));
    }
}
