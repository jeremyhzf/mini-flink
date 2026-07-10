package org.miniflink.execution;

import org.junit.jupiter.api.Test;
import org.miniflink.api.function.KeySelector;
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

    @Test
    void forward边在两侧并行度不同时降级为rebalance() {
        // source(parallelism=2) → map(p=1) → sink(p=1)：map 默认 forward，但 srcs.size=2 != tgts.size=1
        StreamGraph sg = new StreamGraph();
        SourceTransformation<Integer> src = new SourceTransformation<>(1, "source",
                new SourceOperatorImpl<>(new CollectionSource<>(List.of(1, 2, 3))));
        src.setParallelism(2);
        OneInputTransformation<Integer, Integer> map = new OneInputTransformation<>(2, "map", src,
                new MapOperator<>((MapFunction<Integer, Integer>) x -> x * 10));
        OneInputTransformation<Integer, Void> sink = new OneInputTransformation<>(3, "sink", map,
                new SinkOperator<>(v -> {}));
        sg.addTransformation(src);
        sg.addTransformation(map);
        sg.addSink(sink);

        ExecutionGraph g = ExecutionGraph.from(sg);
        // source(2 subtask) + [map,sink](1 subtask) → 3 vertices, 1 edge
        assertEquals(3, g.getVertices().size());
        assertEquals(1, g.getEdges().size());
        assertTrue(g.getEdges().get(0).getPartitioner() instanceof RebalancePartitioner);
    }

    @Test
    void keyBy处的hash分区器断开链化() {
        // source → map1(forward) → map2(hash/keyBy) → sink：chain 在 keyBy 处断开
        StreamGraph sg = new StreamGraph();
        SourceTransformation<Integer> src = new SourceTransformation<>(1, "source",
                new SourceOperatorImpl<>(new CollectionSource<>(List.of(1, 2, 3))));
        OneInputTransformation<Integer, Integer> map1 = new OneInputTransformation<>(2, "map1", src,
                new MapOperator<>((MapFunction<Integer, Integer>) x -> x * 10));
        OneInputTransformation<Integer, Integer> map2 = new OneInputTransformation<>(3, "map2", map1,
                new MapOperator<>((MapFunction<Integer, Integer>) x -> x + 1),
                new HashPartitioner(), (KeySelector<Integer, Integer>) v -> v);
        OneInputTransformation<Integer, Void> sink = new OneInputTransformation<>(4, "sink", map2,
                new SinkOperator<>(v -> {}));
        sg.addTransformation(src);
        sg.addTransformation(map1);
        sg.addTransformation(map2);
        sg.addSink(sink);

        ExecutionGraph g = ExecutionGraph.from(sg);
        // source + [map1] + [map2,sink] → 3 vertices（每个 group p=1），2 edges，2 个处理 group
        assertEquals(3, g.getVertices().size());
        assertEquals(2, g.getVertices().stream().filter(v -> !v.isSource()).count());
        assertEquals(2, g.getEdges().size());
        // 第一条边（source→map1）两侧各 1 subtask，保持 forward
        assertTrue(g.getEdges().get(0).getPartitioner() instanceof ForwardPartitioner);
        // 第二条边（map1→map2）跨 keyBy，断链且为 hash 分区
        assertTrue(g.getEdges().get(1).getPartitioner() instanceof HashPartitioner);
    }
}
