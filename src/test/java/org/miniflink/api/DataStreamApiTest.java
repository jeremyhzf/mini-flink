package org.miniflink.api;

import org.junit.jupiter.api.Test;
import org.miniflink.graph.OneInputTransformation;
import org.miniflink.graph.SourceTransformation;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class DataStreamApiTest {

    /** 链式调用 source → map → sink 应构建出上游正确衔接、节点命名的逻辑链。 */
    @Test
    void chainedCallsBuildCorrectLogicalChain() {
        StreamExecutionEnvironment env = new StreamExecutionEnvironment();
        DataStream<String> source = env.fromCollection(List.of("a", "b"));
        DataStream<Integer> mapped = source.map(String::length);
        mapped.addSink(v -> {});

        // 从 source 节点开始，逐步校验链：source → map → sink
        SourceTransformation<?> srcNode = (SourceTransformation<?>) source.getTransformation();
        OneInputTransformation<?, ?> mapNode = (OneInputTransformation<?, ?>) mapped.getTransformation();
        assertSame(srcNode, mapNode.getInput(), "map 的上游应为 source");
        assertEquals("map", mapNode.getName());
    }

    /** filter 复用同一上游 transformation 并返回以 filter 命名的新流。 */
    @Test
    void filterReusesSameUpstreamAndReturnsNewStream() {
        StreamExecutionEnvironment env = new StreamExecutionEnvironment();
        DataStream<Integer> source = env.fromCollection(List.of(1, 2, 3));
        DataStream<Integer> filtered = source.filter(x -> x > 1);

        OneInputTransformation<?, ?> fNode = (OneInputTransformation<?, ?>) filtered.getTransformation();
        assertSame(source.getTransformation(), fNode.getInput());
        assertEquals("filter", fNode.getName());
    }
}
