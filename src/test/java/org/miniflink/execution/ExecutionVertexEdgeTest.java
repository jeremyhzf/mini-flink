package org.miniflink.execution;

import org.junit.jupiter.api.Test;
import org.miniflink.api.function.KeySelector;
import org.miniflink.runtime.operator.MapOperator;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ExecutionVertexEdgeTest {

    @Test
    void sourceVertex判定为source() {
        ExecutionVertex sv = new ExecutionVertex(1, 0, 1, List.of(),
                new org.miniflink.runtime.operator.SourceOperatorImpl<>(
                        new org.miniflink.connector.CollectionSource<>(List.of("x"))));
        assertTrue(sv.isSource());
    }

    @Test
    void 处理vertex持有算子且非source() {
        ExecutionVertex v = new ExecutionVertex(2, 0, 2,
                List.of(new MapOperator<>((org.miniflink.api.function.MapFunction<Integer, Integer>) x -> x)), null);
        // 注意：sourceOperator 用一个非 null 占位才 isSource=true；这里传 null → 非 source
        assertFalse(v.isSource());
        assertEquals(1, v.getOperators().size());
        assertEquals(0, v.getSubtaskIndex());
        assertEquals(2, v.getParallelism());
    }

    @Test
    void edge持有上下游与分区器() {
        ExecutionVertex a = new ExecutionVertex(1, 0, 1, List.of(), null);
        ExecutionVertex b = new ExecutionVertex(2, 0, 1, List.of(), null);
        KeySelector<Integer, Integer> ks = x -> x;
        ExecutionEdge edge = new ExecutionEdge(List.of(a), List.of(b), new HashPartitioner(), ks);

        assertEquals(1, edge.getSources().size());
        assertEquals(1, edge.getTargets().size());
        assertInstanceOf(HashPartitioner.class, edge.getPartitioner());
        assertSame(ks, edge.getKeySelector());
    }
}
