package org.miniflink.examples;

import org.junit.jupiter.api.Test;
import org.miniflink.api.StreamExecutionEnvironment;
import org.miniflink.api.function.KeySelector;
import org.miniflink.api.function.ReduceFunction;
import org.miniflink.connector.CollectSink;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * 阶段③验收：带计数的词频统计 WordCount。
 * source → flatMap(分词) → map(word→WC(word,1)) → keyBy(word) → reduce(计数累加) → sink。
 */
class WordCountExampleTest {

    record WC(String word, int count) { }

    @Test
    void 词频统计正确累加() throws Exception {
        StreamExecutionEnvironment env = new StreamExecutionEnvironment();
        CollectSink<WC> sink = new CollectSink<>();

        env.fromCollection(List.of("hello world hello", "world flink"))
           .<String>flatMap((line, out) -> { for (String w : line.split(" ")) out.collect(w); })
           .map(w -> new WC(w, 1))
           .keyBy((KeySelector<WC, String>) wc -> wc.word)
           .reduce((ReduceFunction<WC>) (a, b) -> new WC(a.word, a.count + b.count))
           .addSink(sink::add);

        env.execute("wordcount");

        // reduce 输出 running 结果（每输入一条）；按 word 取最大 count（=最终值）
        Map<String, Integer> result = new HashMap<>();
        for (WC wc : sink.getResults()) {
            result.merge(wc.word, wc.count, Math::max);
        }
        assertEquals(2, result.get("hello"));
        assertEquals(2, result.get("world"));
        assertEquals(1, result.get("flink"));
    }
}
