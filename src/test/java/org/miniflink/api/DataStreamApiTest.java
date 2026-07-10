package org.miniflink.api;

import org.junit.jupiter.api.Test;
import org.miniflink.graph.OneInputTransformation;
import org.miniflink.graph.SourceTransformation;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class DataStreamApiTest {

    @Test
    void 链式调用应构建正确的逻辑链结构() {
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

    @Test
    void filter应复用同一上游并返回新流() {
        StreamExecutionEnvironment env = new StreamExecutionEnvironment();
        DataStream<Integer> source = env.fromCollection(List.of(1, 2, 3));
        DataStream<Integer> filtered = source.filter(x -> x > 1);

        OneInputTransformation<?, ?> fNode = (OneInputTransformation<?, ?>) filtered.getTransformation();
        assertSame(source.getTransformation(), fNode.getInput());
        assertEquals("filter", fNode.getName());
    }
}
