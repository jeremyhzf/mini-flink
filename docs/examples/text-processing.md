# 阶段①示例：文本处理流水线

本示例演示 mini-flink 阶段①（骨架）的端到端能力。

## 作业逻辑

```java
StreamExecutionEnvironment env = new StreamExecutionEnvironment();
CollectSink<String> sink = new CollectSink<>();

env.fromCollection(List.of("hello world", "hi there", "go"))
   .<String>flatMap((line, out) -> { for (String w : line.split(" ")) out.collect(w); })  // 分词
   .filter(w -> w.length() > 2)                                                    // 过滤短词
   .map(String::toUpperCase)                                                       // 转大写
   .addSink(sink::add);                                                            // 收集结果

env.execute("text-processing");
// 结果：[HELLO, WORLD, THERE]
```

## 执行流程（对应四层）

1. **API 层**：`DataStream` 链式调用累积 `Transformation`。
2. **StreamGraph 层**：`execute` 时逻辑 DAG 就绪（source → flatMap → filter → map → sink）。
3. **ExecutionGraph 层**：从 sink 回溯成单线性链 `source + [flatMap, filter, map, sink]`。
4. **Runtime 层**：`StreamExecutor` 用 `OperatorOutput` 串成同步链，`source.run()` 触发数据同步流过到 sink。

## 阶段①已实现 / 未实现

- 已实现：source / map / flatMap / filter / sink、单线性链、同步执行。
- 未实现（后续阶段）：并行多线程、keyBy 与 keyed state、窗口与 watermark、checkpoint。

## 独立运行（带 main 的可运行版本）

除了上文测试形式，还有可直接运行、打印结果的版本：`org.miniflink.examples.TextProcessingExample`。

```bash
mvn compile exec:java
```

输出：

```
输入        : ["hello world", "hi there", "go"]
处理结果    : [HELLO, WORLD, THERE]
预期        : [HELLO, WORLD, THERE]
```

> 注：`exec:java` 不会自动编译，需配合 `compile`（或先 `mvn compile`）。pom 已通过 `exec-maven-plugin` 配置默认主类为该示例。
