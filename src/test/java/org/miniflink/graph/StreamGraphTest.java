package org.miniflink.graph;

import org.junit.jupiter.api.Test;
import org.miniflink.connector.CollectionSource;
import org.miniflink.runtime.operator.MapOperator;
import org.miniflink.runtime.operator.SinkOperator;
import org.miniflink.runtime.operator.SourceOperatorImpl;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class StreamGraphTest {

    @Test
    void 从sink回溯应能得到完整单线性链() {
        SourceTransformation<String> source = new SourceTransformation<>(
                1, "source", new SourceOperatorImpl<>(new CollectionSource<>(List.of("a"))));
        OneInputTransformation<String, String> map = new OneInputTransformation<>(
                2, "map", source, new MapOperator<>(x -> x + "!"));
        OneInputTransformation<String, Void> sink = new OneInputTransformation<>(
                3, "sink", map, new SinkOperator<>(v -> {}));

        StreamGraph graph = new StreamGraph();
        graph.addTransformation(source);
        graph.addTransformation(map);
        graph.addSink(sink);

        assertEquals(1, graph.getSinks().size());
        assertSame(sink, graph.getSinks().get(0));
        // sink 的 input 是 map，map 的 input 是 source
        assertSame(map, ((OneInputTransformation<?, ?>) graph.getSinks().get(0)).getInput());
    }
}
