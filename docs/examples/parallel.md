# 阶段②示例：多并行度并行处理

演示 `parallelism > 1` 下的多线程并行执行。

## 作业逻辑

```java
StreamExecutionEnvironment env = new StreamExecutionEnvironment();
CollectSink<Integer> sink = new CollectSink<>();

env.fromCollection(List.of(1, 2, 3, 4, 5, 6))
   .setParallelism(2)        // 2 个 source subtask 各取一半数据
   .map(x -> x * 10)
   .setParallelism(2)        // 2 个 map subtask，forward 一对一
   .addSink(sink::add);

env.execute("parallel");
// 结果（排序后）：[10, 20, 30, 40, 50, 60]
```

## 执行模型（阶段②）

- source parallelism=2：数据按 subtaskIndex 取模分片（subtask 0 取索引 0/2/4，subtask 1 取 1/3/5）。
- 每个 subtask 一个线程（`Task`），subtask 间用有界 `Channel` 连接。
- forward 分区：source.0 → map.0、source.1 → map.1（一对一）。
- 反压：Channel 满则上游阻塞。
- 关闭：source 结束发 EOB，经引用计数对齐级联退出。
