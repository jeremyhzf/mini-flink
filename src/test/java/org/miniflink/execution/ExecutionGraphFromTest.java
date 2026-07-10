package org.miniflink.execution;

import org.junit.jupiter.api.Test;
import org.miniflink.api.function.MapFunction;
import org.miniflink.connector.CollectionSource;
import org.miniflink.graph.OneInputTransformation;
import org.miniflink.graph.SourceTransformation;
import org.miniflink.graph.StreamGraph;
import org.miniflink.runtime.operator.MapOperator;
import org.miniflink.runtime.operator.SinkOperator;
import org.miniflink.runtime.operator.SourceOperatorImpl;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ExecutionGraphFromTest {

    private StreamGraph buildLinearGraph() {
        StreamGraph sg = new StreamGraph();
        SourceTransformation<Integer> src = new SourceTransformation<>(1, "source",
                new SourceOperatorImpl<>(new CollectionSource<>(List.of(1, 2, 3))));
        OneInputTransformation<Integer, Integer> map = new OneInputTransformation<>(2, "map", src,
                new MapOperator<>((MapFunction<Integer, Integer>) x -> x * 10));
        OneInputTransformation<Integer, Void> sink = new OneInputTransformation<>(3, "sink", map,
                new SinkOperator<>(v -> {}));
        sg.addTransformation(src);
        sg.addTransformation(map);
        sg.addSink(sink);
        return sg;
    }

    @Test
    void 单线性链应展开为source与chain两组vertices与一条边() {
        ExecutionGraph g = ExecutionGraph.from(buildLinearGraph());
        // source(p1) + [map,sink] chain(p1) → 2 vertices, 1 edge
        assertEquals(2, g.getVertices().size());
        assertTrue(g.getVertices().get(0).isSource());
        assertFalse(g.getVertices().get(1).isSource());
        assertEquals(2, g.getVertices().get(1).getOperators().size()); // map + sink 链化
        assertEquals(1, g.getEdges().size());
    }

    @Test
    void 多sink应抛IllegalStateException() {
        StreamGraph sg = new StreamGraph();
        SourceTransformation<Integer> src = new SourceTransformation<>(1, "s",
                new SourceOperatorImpl<>(new CollectionSource<>(List.of(1))));
        OneInputTransformation<Integer, Void> sink1 = new OneInputTransformation<>(2, "sink1", src,
                new SinkOperator<>(v -> {}));
        OneInputTransformation<Integer, Void> sink2 = new OneInputTransformation<>(3, "sink2", src,
                new SinkOperator<>(v -> {}));
        sg.addTransformation(src);
        sg.addSink(sink1);
        sg.addSink(sink2);
        assertThrows(IllegalStateException.class, () -> ExecutionGraph.from(sg));
    }
}
