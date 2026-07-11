package org.miniflink.runtime;

import org.junit.jupiter.api.Test;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

class WatermarkFlowTest {

    @Test
    void Watermark携带timestamp() {
        Watermark wm = new Watermark(123L);
        assertEquals(123L, wm.getTimestamp());
    }

    @Test
    void Output的sendWatermark广播到所有下游channel() throws Exception {
        Channel c1 = new Channel();
        Channel c2 = new Channel();
        Output output = new Output(List.of(c1, c2), new org.miniflink.execution.ForwardPartitioner(), null);
        Watermark wm = new Watermark(42L);
        output.sendWatermark(wm);
        // watermark 不分区：所有下游 channel 都收到同一 Watermark
        Watermark w1 = (Watermark) c1.receive();
        Watermark w2 = (Watermark) c2.receive();
        assertEquals(42L, w1.getTimestamp());
        assertEquals(42L, w2.getTimestamp());
    }
}
