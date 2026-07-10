package org.miniflink.runtime;

import org.junit.jupiter.api.Test;
import org.miniflink.connector.CollectionSource;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class SourceParallelismTest {

    @Test
    void CollectionSource应按subtaskIndex分片() throws Exception {
        CollectionSource<Integer> src = new CollectionSource<>(List.of(10, 11, 12, 13, 14));

        ListCollector<Integer> out0 = new ListCollector<>();
        src.run(new SourceContextImpl<>(out0, 0, 2)); // subtask 0 取索引 0,2,4
        assertEquals(List.of(10, 12, 14), out0.getResult());

        ListCollector<Integer> out1 = new ListCollector<>();
        src.run(new SourceContextImpl<>(out1, 1, 2)); // subtask 1 取索引 1,3
        assertEquals(List.of(11, 13), out1.getResult());
    }

    @Test
    void parallelism为1时取全部() throws Exception {
        CollectionSource<String> src = new CollectionSource<>(List.of("a", "b", "c"));
        ListCollector<String> out = new ListCollector<>();
        src.run(new SourceContextImpl<>(out, 0, 1));
        assertEquals(List.of("a", "b", "c"), out.getResult());
    }
}
