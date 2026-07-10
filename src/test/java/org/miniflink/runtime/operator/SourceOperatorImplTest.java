package org.miniflink.runtime.operator;

import org.junit.jupiter.api.Test;
import org.miniflink.connector.CollectionSource;
import org.miniflink.runtime.ListCollector;
import org.miniflink.runtime.RuntimeContextImpl;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SourceOperatorImplTest {

    @Test
    void run应把源数据全部输出到下游() throws Exception {
        SourceOperatorImpl<String> op = new SourceOperatorImpl<>(new CollectionSource<>(List.of("x", "y", "z")));
        ListCollector<String> downstream = new ListCollector<>();
        op.open(downstream, new RuntimeContextImpl(0, 1, null));

        op.run();

        assertEquals(List.of("x", "y", "z"), downstream.getResult());
    }
}
