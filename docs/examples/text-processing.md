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
