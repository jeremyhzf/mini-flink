package org.miniflink.examples;

import org.junit.jupiter.api.Test;
import org.miniflink.api.StreamExecutionEnvironment;
import org.miniflink.connector.CollectSink;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * 阶段②验收：parallelism=2 下，数据被分片到 2 个 source subtask，
 * 经 forward 到 2 个 map subtask 并行处理，CollectSink 汇总。
 */
class ParallelEndToEndTest {

    @Test
    void 多并行度forward下数据正确并行处理() throws Exception {
        StreamExecutionEnvironment env = new StreamExecutionEnvironment();
        CollectSink<Integer> sink = new CollectSink<>();

        env.fromCollection(List.of(1, 2, 3, 4, 5, 6))
           .setParallelism(2)        // 2 个 source subtask 各取一半
           .map(x -> x * 10)
           .setParallelism(2)        // 2 个 map subtask，forward 一对一
           .addSink(sink::add);

        env.execute("parallel");

        // 多线程顺序不定，排序后比较
        List<Integer> sorted = new ArrayList<>(sink.getResults());
        Collections.sort(sorted);
        assertEquals(List.of(10, 20, 30, 40, 50, 60), sorted);
    }
}
