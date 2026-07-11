package org.miniflink.runtime;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class StreamElementTest {

    /** 验证 Record 携带 value 且实现 StreamElement。 */
    @Test
    void recordHoldsValueAndImplementsStreamElement() {
        Record<String> r = new Record<>("hello", 0L);
        assertInstanceOf(StreamElement.class, r);
        assertEquals("hello", r.value());
    }

    /** 验证 EndOfBroadcast 是单例且实现 StreamElement。 */
    @Test
    void endOfBroadcastIsSingletonAndImplementsStreamElement() {
        EndOfBroadcast a = EndOfBroadcast.INSTANCE;
        EndOfBroadcast b = EndOfBroadcast.INSTANCE;
        assertSame(a, b);
        assertInstanceOf(StreamElement.class, a);
    }
}
