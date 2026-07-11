package org.miniflink.examples;

import org.junit.jupiter.api.Test;
import org.miniflink.api.StreamExecutionEnvironment;
import org.miniflink.connector.CollectSink;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * 阶段①验收示例：文本处理流水线。
 * 演示 source → flatMap(分词) → filter(过滤短词) → map(转大写) → sink 的完整链路。
 */
class TextProcessingExampleTest {

    /** 验证文本处理流水线（分词→过滤短词→转大写）产出预期结果。 */
    @Test
    void textProcessingPipelineProducesExpectedResult() throws Exception {
        StreamExecutionEnvironment env = new StreamExecutionEnvironment();
        CollectSink<String> sink = new CollectSink<>();

        env.fromCollection(List.of("hello world", "hi there", "go"))
           .<String>flatMap((line, out) -> { for (String w : line.split(" ")) out.collect(w); })
           .filter(w -> w.length() > 2)          // 过滤长度 <=2 的词
           .map(String::toUpperCase)
           .addSink(sink::add);

        env.execute("text-processing");

        // hello(5) world(5) hi(2,过滤) there(5) go(2,过滤)
        assertEquals(List.of("HELLO", "WORLD", "THERE"), sink.getResults());
    }
}
