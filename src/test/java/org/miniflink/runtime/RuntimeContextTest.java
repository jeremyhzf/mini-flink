package org.miniflink.runtime;

import org.junit.jupiter.api.Test;
import org.miniflink.api.function.KeySelector;

import static org.junit.jupiter.api.Assertions.*;

class RuntimeContextTest {

    @Test
    void setCurrentKey转发backend且key隔离() {
        RuntimeContextImpl ctx = new RuntimeContextImpl(0, 1, null);
        ValueState<Integer> state = ctx.getStateBackend().getValueState("acc");

        ctx.setCurrentKey("a");
        state.update(1);
        ctx.setCurrentKey("b");
        assertNull(state.value());      // b 隔离
        state.update(2);
        ctx.setCurrentKey("a");
        assertEquals(1, state.value()); // a 仍在
    }

    @Test
    void 持有subtaskIndex与keySelector() {
        KeySelector<String, String> ks = s -> s;
        RuntimeContextImpl ctx = new RuntimeContextImpl(1, 2, ks);
        assertEquals(1, ctx.getSubtaskIndex());
        assertEquals(2, ctx.getParallelism());
        assertSame(ks, ctx.getKeySelector());
    }
}
