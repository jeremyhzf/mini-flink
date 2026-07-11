# 阶段③示例：WordCount（带计数的词频统计）

演示 keyed state + 聚合（阶段③核心能力）。

## 作业逻辑

```java
env.fromCollection(List.of("hello world hello", "world flink"))
   .flatMap((line, out) -> { for (String w : line.split(" ")) out.collect(w); })  // 分词
   .map(w -> new WC(w, 1))                          // → (word, 1)
   .keyBy(wc -> wc.word)                            // 按 word 分区
   .reduce((a, b) -> new WC(a.word, a.count + b.count))  // per-key 计数累加
   .addSink(sink::add);
```

## 工作机制

- `keyBy(word)` 用 HashPartitioner 把同 word 路由到同一 subtask（KeyedStream）。
- `ReduceOperator` 持 per-key `ValueState`（累加器）：每条输入设 currentKey，`reduce(累加, 输入)` 更新 state 并输出 running 结果。
- 同 key 恒落同一 subtask → per-subtask state 内 per-key map 不会跨 subtask 分裂。
- `reduce` 输出 running（每输入一条输出当前累加），sink 收多条，最终值 = 每 key 最大 count。
