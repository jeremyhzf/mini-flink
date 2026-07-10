package org.miniflink.examples;

import org.miniflink.api.StreamExecutionEnvironment;
import org.miniflink.connector.CollectSink;

import java.util.List;

/**
 * 阶段②验收示例（可独立运行版本）。
 *
 * <p>运行方式：
 * <pre>
 *   mvn compile exec:java -Dexec.mainClass="org.miniflink.examples.ParallelExample"
 *   # 或
 *   mvn -q compile && java -cp target/classes org.miniflink.examples.ParallelExample
 * </pre>
 *
 * <p>演示多并行度并行处理：
 * forward 分区：source.0 → map.0、source.1 → map.1（一对一）。
 */
public class ParallelExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = new StreamExecutionEnvironment();
        CollectSink<Integer> sink = new CollectSink<>();

        env.fromCollection(List.of(1, 2, 3, 4, 5, 6))
                .setParallelism(2)        // 2 个 source subtask 各取一半数据
                .map(x -> x * 10)
                .setParallelism(2)        // 2 个 map subtask，forward 一对一
                .addSink(sink::add);

        env.execute("parallel");
        List<Integer> results = sink.getResults();
        // 结果（排序后）：[10, 20, 30, 40, 50, 60]
        results.sort(Integer::compareTo);
        System.out.println("输入：" + List.of(1, 2, 3, 4, 5, 6));
        System.out.println("结果：" + results);
        System.out.println("预期：[10, 20, 30, 40, 50, 60]");
    }
}
