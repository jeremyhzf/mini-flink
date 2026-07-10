package org.miniflink.examples;

import org.miniflink.api.StreamExecutionEnvironment;
import org.miniflink.api.function.FlatMapFunction;
import org.miniflink.connector.CollectSink;
import org.miniflink.runtime.Collector;

import java.util.List;

/**
 * 阶段①验收示例（可独立运行版本）。
 *
 * <p>运行方式：
 * <pre>
 *   mvn compile exec:java -Dexec.mainClass="org.miniflink.examples.TextProcessingExample"
 *   # 或
 *   mvn -q compile && java -cp target/classes org.miniflink.examples.TextProcessingExample
 * </pre>
 *
 * <p>演示流水线：source → flatMap(分词) → filter(过滤短词) → map(转大写) → sink，
 * 在单线程同步链上端到端跑通。
 */
public class TextProcessingExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = new StreamExecutionEnvironment();

        // CollectSink 收集结果，便于运行后打印
        CollectSink<String> sink = new CollectSink<>();

        env.fromCollection(List.of("hello world", "hi there", "go"))
           .flatMap(new Tokenizer())                    // 分词
           .filter(w -> w.length() > 2)           // 过滤长度 <= 2 的词（hi、go 被过滤）
           .map(String::toUpperCase)                    // 转大写
           .addSink(sink::add);                         // 收集结果

        env.execute("text-processing");

        System.out.println("输入        : [\"hello world\", \"hi there\", \"go\"]");
        System.out.println("处理结果    : " + sink.getResults());
        System.out.println("预期        : [HELLO, WORLD, THERE]");
    }

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
