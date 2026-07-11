# 阶段④示例：滚动窗口聚合（event time + watermark）

演示 event time + watermark + 窗口（阶段④核心能力）。

## 作业逻辑

```java
env.fromCollection(events)
   .assignTimestampsAndWatermarks(
       WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofMillis(0), e -> e.ts))
   .keyBy(e -> e.key)
   .window(TumblingEventTimeWindows.of(Duration.ofSeconds(1)))
   .reduce((a, b) -> new Event(a.key, a.value + b.value, b.ts))
   .addSink(sink::add);
```

## 工作机制

- `assignTimestampsAndWatermarks`：从记录提取事件时间 ts，按乱序容忍生成 watermark（watermark = maxTs − outOfOrderness），随数据流向下游。
- `keyBy + window`：同 key 经 HashPartitioner 落同一 subtask；`TumblingEventTimeWindows` 把记录按 ts 分配到 1s 窗口。
- `WindowOperator` 持 per-key per-window `MapState<TimeWindow, ACC>` 增量 reduce；记录到达时累加，注册 window.end 的 event time timer。
- watermark 推进到 window.end → `TimerService` 触发 → 输出窗口最终累加值一次 + 清理窗口状态。
- 有界 source 结束广播 `Watermark(+∞)` → 触发所有剩余窗口。
