package org.miniflink.examples;

import org.miniflink.api.StreamExecutionEnvironment;
import org.miniflink.api.function.FlatMapFunction;
import org.miniflink.api.function.KeySelector;
import org.miniflink.api.function.ReduceFunction;
import org.miniflink.connector.CollectSink;
import org.miniflink.runtime.Collector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 阶段③验收示例（可独立运行版本）。
 *
 * <p>运行方式：
 * <pre>
 *   mvn compile exec:java -Dexec.mainClass="org.miniflink.examples.WordCountExample"
 *   # 或
 *   mvn -q compile && java -cp target/classes org.miniflink.examples.WordCountExample
 * </pre>
 *
 * <p>演示 keyed state + 聚合（阶段③核心能力）：
 * source → flatMap(分词) → map(word→(word,1)) → keyBy(word) → reduce(计数累加) → sink。
 * ReduceOperator 持 per-key ValueState（累加器），同 word 经 HashPartitioner 恒落同一 subtask，
 * 故 per-key 计数累加完整、不跨 subtask 分裂。
 */
public class WordCountExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = new StreamExecutionEnvironment();
        CollectSink<WC> sink = new CollectSink<>();

        List<String> lines = List.of("hello world hello", "world flink");

        env.fromCollection(lines)
           .flatMap(new Tokenizer())  // 分词
           .map(w -> new WC(w, 1))                                                                 // → (word, 1)
           .keyBy(wc -> wc.word)                                        // 按 word 分区
           .reduce((a, b) -> new WC(a.word, a.count + b.count))               // per-key 计数累加
           .addSink(sink::add);

        env.execute("WordCount");

        // reduce 输出 running（每输入一条输出当前累加）；取每 key 最大 count = 最终值
        Map<String, Integer> result = new HashMap<>();
        for (WC wc : sink.getResults()) {
            result.merge(wc.word, wc.count, Math::max);
        }

        System.out.println("输入：" + lines);
        System.out.println("词频统计：");
        result.forEach((w, c) -> System.out.println("  " + w + " => " + c));
        System.out.println("预期：hello => 2, world => 2, flink => 1");
    }

    /** 词频记录：(word, count)。 */
    public record WC(String word, int count) { }

    /** 分词算子：把输入字符串按空格切分为单词。 */
    public static final class Tokenizer implements FlatMapFunction<String, String> {
        @Override
        public void flatMap(String value, Collector<String> out) throws Exception {
            String[] split = value.split(" ");
            for (String w : split) {
                if (!w.isEmpty()){
                    out.collect(w);
                }
            }
        }
    }
}
