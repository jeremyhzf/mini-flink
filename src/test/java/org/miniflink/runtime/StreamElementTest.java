package org.miniflink.runtime;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class StreamElementTest {

    @Test
    void record携带value且实现StreamElement() {
        Record<String> r = new Record<>("hello");
        assertInstanceOf(StreamElement.class, r);
        assertEquals("hello", r.value());
    }

    @Test
    void EndOfBroadcast是单例且实现StreamElement() {
        EndOfBroadcast a = EndOfBroadcast.INSTANCE;
        EndOfBroadcast b = EndOfBroadcast.INSTANCE;
        assertSame(a, b);
        assertInstanceOf(StreamElement.class, a);
    }
}
