# Mini-Flink 阶段④（时间与窗口）设计补充

- **日期**：2026-07-11
- **状态**：已通过 brainstorming，待编写实现计划
- **关系**：细化 [整体设计](2026-07-10-mini-flink-design.md) 的阶段④部分（spec §4.6 Time、§4.7 Window、§10 阶段④）。

## 1. 背景与目标

阶段①（骨架）+②（多线程并行）+③（keyed state）已完成在 `main`：四层分层、多线程 Task、有界 Channel 反压、算子链、三分区器、EOB 引用计数关闭、keyed state（StateBackend/三种 state/RuntimeContext）、KeyedStream、ReduceOperator。

阶段③的 `ReduceOperator` 是 **running reduce**（每输入输出当前累加），无法表达"攒一批、到点输出最终值"。阶段④引入 **event time + watermark + 窗口**：数据按事件时间分配到窗口，watermark 推进触发窗口，窗口结束时输出**最终聚合值一次**。这是 Flink 区别于普通流处理的核心机制之一。

### 设计原则（延续）

- **对照学习**：贴近真实 Flink（WatermarkStrategy、TimerService、WindowAssigner/Trigger/WindowOperator）。
- **稳定边界**：`Collector` 接口不变。
- **YAGNI**：processing time、会话窗口、allowedLateness、ProcessWindowFunction/AggregateFunction 留后续。

## 2. 范围

### 包含

- **event time 语义**（watermark 驱动），processing time 不做。
- **WatermarkStrategy**（`forBoundedOutOfOrderness` + `TimestampAssigner`）+ 独立算子 `assignTimestampsAndWatermarks`。
- **TimerService**（event time 定时器，watermark 推进时触发）。
- **Window / WindowAssigner**：`TumblingEventTimeWindows` + `SlidingEventTimeWindows`。
- **Trigger** 接口 + 内置 `EventTimeTrigger`。
- **WindowOperator**：per-key per-window 状态（复用阶段③ `MapState`），窗口结束触发输出。
- **WindowedStream** API（`KeyedStream.window` → `reduce`）。
- **StreamElement 扩展**：加 `Watermark`；`Record` 加事件时间戳。

### 不包含（留后续）

- processing time（墙钟定时器，测试不可控）。
- 会话窗口（留阶段⑤，依赖 watermark + 状态 + 定时器，与容错耦合）。
- `allowedLateness` / 侧输出迟到数据。
- `ProcessWindowFunction`（全窗口函数）/ `AggregateFunction`（IN/ACC/OUT 三态）。阶段④仅窗口 `ReduceFunction`。
- 用户自定义 `Trigger`（仅内置 `EventTimeTrigger`）。

## 3. 已确认的关键决策

| 决策 | 选择 | 备注 |
|---|---|---|
| 时间语义 | **仅 event time** | watermark 驱动；processing time 留后续 |
| watermark 生成 | **WatermarkStrategy 接口（仿 Flink）** | 独立算子 `assignTimestampsAndWatermarks`，含 `TimestampAssigner` + 乱序容忍 |
| 触发机制 | **TimerService 抽象** | event time timer，watermark 推进触发；阶段⑤ `KeyedProcessFunction` 复用 |
| 窗口函数 | **仅窗口 ReduceFunction** | 增量聚合，窗口结束输出最终值（一次） |
| 窗口类型 | **滚动 + 滑动** | 会话留阶段⑤ |
| 迟到数据 | **drop** | watermark > window.end 后到达的记录丢弃；allowedLateness 留后续 |

## 4. 核心抽象

### 4.1 StreamElement 扩展（runtime）

阶段② sealed `StreamElement`（`Record` / `EndOfBroadcast`）加第三种 `Watermark`；`Record` 加事件时间戳。

```java
public final class Watermark implements StreamElement {
    private final long timestamp;
    public Watermark(long timestamp) { this.timestamp = timestamp; }
    public long getTimestamp() { return timestamp; }
}

public record Record<T>(T value, long timestamp) implements StreamElement { }  // 加 timestamp 字段
```

### 4.2 WatermarkStrategy + TimestampAssigner（api/function）

```java
@FunctionalInterface
public interface TimestampAssigner<T> {
    long extractTimestamp(T record);
}

public interface WatermarkStrategy<T> {
    long extractTimestamp(T record);        // 委托 TimestampAssigner
    long currentWatermark();                // 单调递增
    static <T> WatermarkStrategy<T> forBoundedOutOfOrderness(Duration maxOutOfOrderness, TimestampAssigner<T> assigner);
}
```

`BoundedOutOfOrdernessWatermarks<T>` 实现 `WatermarkStrategy`：内部维护 `maxTimestamp`，每条 `extractTimestamp` 更新它，`currentWatermark() = maxTimestamp - maxOutOfOrderness`（单调，只增不减）。

**`TimestampsAndWatermarksOperator<T>`**（runtime/operator，implements `Operator<T,T>`）：
- `processElement(record)`：`ts = strategy.extractTimestamp(record)` → 输出 `Record(record, ts)` → 输出 `Watermark(strategy.currentWatermark())`（每条记录后发当前 watermark）。
- 这是独立算子，挂在 source 之后。

`DataStream.assignTimestampsAndWatermarks(WatermarkStrategy)` 建该算子的 transformation。

### 4.3 TimerService（runtime）

```java
public interface TimerService {
    long currentWatermark();
    void registerEventTimeTimer(long time);
    void deleteEventTimeTimer(long time);
}

/** 算子实现的回调：timer 触发时调用。 */
public interface TimerHandler {
    void onEventTime(long time);
}
```

`InternalTimerService`（per-subtask，WindowOperator 持有）：
- 内部 `TreeSet<Long>`（或优先队列）按 time 存 event time timers（去重）。
- `registerEventTimeTimer(t)` / `deleteEventTimeTimer(t)`。
- `advanceTo(long watermark)`：watermark 推进时，触发所有 `time <= watermark` 的 timer（回调 `handler.onEventTime(time)`），从集合移除。
- `currentWatermark()`：最近一次 `advanceTo` 的值。

### 4.4 Window + WindowAssigner（window）

```java
/** 时间窗口：[start, end)。equals/hashCode 按 start+end。 */
public class TimeWindow {
    private final long start;
    private final long end;
    public TimeWindow(long start, long end) { ... }
    public long start() { return start; }
    public long end() { return end; }
    public static long getWindowStartWithOffset(long timestamp, long offset, long size) {
        return timestamp - (timestamp - offset + size) % size;   // offset=0 简化
    }
}

public abstract class WindowAssigner<T, W extends Window> {
    public abstract Collection<W> assignWindows(T record, long timestamp);
    public abstract boolean isEventTime();
}
```

- `TumblingEventTimeWindows.of(Duration size)`：`assignWindows` 返回单个窗口 `TimeWindow(start, start+size)`，`start = timestamp - timestamp % size`。
- `SlidingEventTimeWindows.of(Duration size, Duration slide)`：返回 `size/slide` 个窗口（每个可能的起点对齐的窗口）。
- **Window 层次**：`abstract class Window`（抽象 `start()/end()`）；`TimeWindow extends Window`。阶段④仅 `TimeWindow`，`Window` 为后续（如 `GlobalWindow`）留口。`WindowAssigner<T, W extends Window>` 泛型——`TumblingEventTimeWindows`/`SlidingEventTimeWindows` 即 `WindowAssigner<T, TimeWindow>`。

### 4.5 Trigger（window）

```java
public enum TriggerResult { CONTINUE, FIRE, FIRE_AND_PURGE }

public abstract class Trigger<T, W extends Window> {
    public abstract TriggerResult onElement(T element, long timestamp, W window, TriggerContext ctx) throws Exception;
    public abstract TriggerResult onEventTime(long time, W window, TriggerContext ctx) throws Exception;
}

public interface TriggerContext {
    long getCurrentWatermark();
    void registerEventTimeTimer(long time);
    void deleteEventTimeTimer(long time);
}
```

内置 `EventTimeTrigger<T, W>`：
- `onElement`：`ctx.registerEventTimeTimer(window.end)`（首次注册，幂等由 TimerService 去重保证）→ `CONTINUE`。
- `onEventTime(time, window)`：`time == window.end` → `FIRE_AND_PURGE`；否则 `CONTINUE`。

### 4.6 WindowOperator（runtime/operator）

```java
public class WindowOperator<IN> implements Operator<IN, IN>, TimerHandler {
    // open(out, ctx) 取：
    //   windowAssigner、trigger、reduceFn、keySelector = ctx.getKeySelector()
    //   accState = ctx.getStateBackend().getMapState("window-accs")   // MapState<TimeWindow, IN>
    //   timerService = new InternalTimerService(this)（currentWatermark 初始 Long.MIN）
}
```

- **`processElement(record)`**（record 已带 ts）：
  - `key = keySelector.getKey(record)`；`ctx.setCurrentKey(key)`。
  - `for (TimeWindow w : assigner.assignWindows(record, ts))`：
    - `IN acc = accState.get(w)`；`IN reduced = (acc == null) ? record : reduceFn.reduce(acc, record)`；`accState.put(w, reduced)`。
    - `trigger.onElement(record, ts, w, ctx)` → 注册 `w.end` timer。
- **`onWatermark(watermark)`**：
  - `timerService.advanceTo(watermark.getTimestamp())` → 触发到点 timer → 回调 `onEventTime(end)`。
- **`onEventTime(time)`**（TimerHandler）：
  - 触发逻辑（**活跃窗口注册表**方案，单一）：WindowOperator 维护 per-subtask 注册表 `TreeMap<Long, List<WindowKey>>`（`end → (key, window)` 列表，按 end 排序；`WindowKey` = (key, window) 对）。`processElement` 注册窗口时，若该 (key,window) 首次出现则加入对应 `end` 桶并 `timerService.registerEventTimeTimer(end)`。`onEventTime(time)`：取注册表中 `end == time` 的桶，对每个 (key, window)：`ctx.setCurrentKey(key)` → `trigger.onEventTime(time, w, ctx)` 为 `FIRE_AND_PURGE` → 输出 `accState.get(w)` + `accState.remove(w)` → 移除该 (key,window)。
  - 此方案避开"遍历所有 key"——按 end 索引直达待触发窗口。

### 4.7 WindowedStream（api）

```java
public class WindowedStream<T, W extends Window> {
    private final KeyedStream<T, ?> input;
    private final WindowAssigner<T, W> windowAssigner;
    public WindowedStream(KeyedStream<T, ?> input, WindowAssigner<T, W> windowAssigner) { ... }
    public DataStream<T> reduce(ReduceFunction<T> reduceFn) {
        // 建 WindowOperator 的 OneInputTransformation（hash 分区 + keySelector 沿用 KeyedStream）
    }
}
```

`KeyedStream.window(WindowAssigner)` 返回 `WindowedStream`。

## 5. watermark 传播机制

- `Watermark` 是 `StreamElement`，经 `Channel` 流动（阶段② Channel 已传 `StreamElement`）。
- **`Operator` 接口扩展**：加 `default void onWatermark(Watermark watermark) {}`（普通算子默认空）。
- **`OperatorChain.onWatermark(watermark)`**：调链内算子的 `onWatermark`（WindowOperator 在此触发窗口）；链整体处理完后由 `OperatorTask` 转发 watermark 到下游 `Output`。
- **`OperatorTask`**：主循环收到 `Watermark` element → `chain.onWatermark(wm)`（处理）→ 转发到下游 `Output`（若非 sink）。收到 `Record` 走原 `processElement` 路径。
- **`SourceTask`**：source 正常结束 → 广播 `Watermark(Long.MAX_VALUE)`（+∞，触发所有剩余窗口）→ 再广播 EOB（spec §7）。
- watermark 经链/算子单调递增传播；下游收到更大的 watermark 才推进时钟。

> 单线性链 + 单 sink（阶段④仍维持），无多输入 watermark 取 min 的对齐复杂度。

## 6. per-key per-window 状态（复用阶段③ keyed state）

窗口累加器状态 = `MapState<TimeWindow, IN>`（命名 `"window-accs"`）：
- keyBy 保证同 key 恒落同一 subtask；`ctx.setCurrentKey(key)` 后该 key 的 MapState 可达。
- MapState 的 key = `TimeWindow`（按 start+end 寻址）→ 每个 key 内按窗口分桶。
- 这正是阶段③ `MapState` 的用武之地（per-key 内再按 window 维度）。状态隔离与阶段③一致（per-subtask RuntimeContext + MemoryStateBackend）。

## 7. API 示例

```java
env.fromCollection(events)
   .assignTimestampsAndWatermarks(
       WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofMillis(100),
           (Event e) -> e.timestamp))
   .keyBy(e -> e.key)
   .window(TumblingEventTimeWindows.of(Duration.ofSeconds(1)))
   .reduce((a, b) -> new Event(a.key, a.value + b.value))  // 窗口结束输出最终值
   .addSink(sink::add);
```

## 8. 边界处理

- **窗口类型**：滚动（`TumblingEventTimeWindows`）+ 滑动（`SlidingEventTimeWindows`）；会话窗口留阶段⑤。
- **迟到数据**：watermark 已超过 `window.end` 后到达的记录，若分配到已触发/清理的窗口，**丢弃**（无 allowedLateness）。
- **有界 source 结束**：source 广播 `Watermark(Long.MAX_VALUE)` → 触发所有剩余窗口 → 广播 EOB。保证有界作业所有窗口都被触发输出。
- **watermark 单调**：`BoundedOutOfOrdernessWatermarks.currentWatermark()` 只增不减；下游算子收到更小 watermark 忽略。

## 9. Operator 接口扩展（影响所有算子）

`Operator` 加 `default void onWatermark(Watermark watermark) {}`。普通算子（Map/Filter/FlatMap/Sink/Source）用默认空实现（watermark 由 OperatorTask 统一转发）；`WindowOperator` 覆盖处理（推进时钟触发窗口）。`SourceOperator` 无 onWatermark（它是 watermark 源头的下游侧）。`OperatorChain.onWatermark` 透传给链内算子。`RuntimeContext` 可选加 `currentWatermark()`（供算子查询，非必需）。

> 这是一次签名扩展（加 default 方法），不破坏阶段①②③既有算子（default 空）。全量回归保证。

## 10. 测试 + 验收

- **单测**：
  - `BoundedOutOfOrdernessWatermarks`：extractTimestamp 更新 maxTs、currentWatermark = maxTs − 乱序、单调。
  - `InternalTimerService`：register/delete/advanceTo 触发顺序与回调。
  - `TumblingEventTimeWindows` / `SlidingEventTimeWindows`：assignWindows 分配正确（边界 timestamp 落入正确窗口、滑动多窗口）。
  - `EventTimeTrigger`：onElement 注册 end timer、onEventTime(end) → FIRE_AND_PURGE。
  - `WindowOperator`：注入 Record+Watermark，验证 per-key per-window 累加、窗口结束输出最终值、窗口清理、迟到丢弃。
- **端到端验收**：
  - **滚动窗口聚合**：多 key、多窗口、注入 watermark 模拟时间前进，验证每窗口输出最终值（一次）。
  - **滑动窗口示例**：一记录落入多窗口，各窗口独立累加触发。
- watermark 注入完全可控（测试直接发 Watermark element），无需真实等待。

## 11. 任务预览（writing-plans 细化）

| # | 任务 |
|---|---|
| 1 | `Watermark` + `Record` 加 timestamp + `StreamElement` 扩展 + watermark 经 Channel/Output 流动 + OperatorTask/SourceTask 处理 Watermark + source 结束发 +∞ watermark + 全量回归 |
| 2 | `WatermarkStrategy` + `TimestampAssigner` + `BoundedOutOfOrdernessWatermarks` + `TimestampsAndWatermarksOperator` + `DataStream.assignTimestampsAndWatermarks` + 单测 |
| 3 | `TimerService` + `TimerHandler` + `InternalTimerService` + `Operator.onWatermark` default + 单测 |
| 4 | `TimeWindow` + `WindowAssigner` + `TumblingEventTimeWindows` + `SlidingEventTimeWindows` + 单测 |
| 5 | `Trigger` + `TriggerResult` + `TriggerContext` + `EventTimeTrigger` + 单测 |
| 6 | `WindowOperator`（per-key per-window MapState + 活跃窗口注册表 + watermark 触发输出清理）+ 单测 |
| 7 | `WindowedStream` + `KeyedStream.window` + reduce API + 单测 |
| 8 | 端到端验收：滚动窗口聚合 + 滑动窗口示例 + 文档 |

## 12. 风险与权衡

- **watermark 触发的 (key, window) 枚举**（WindowOperator 难点）：event time timer 触发需知道哪些窗口 end<=watermark 且属哪个 key。用 per-subtask 活跃窗口注册表（end → (key,window)）解决，避免遍历所有 key。writing-plans 细化数据结构（可用 `TreeMap<Long, List<(key,window)>>` 按 end 排序，advanceTo 取 headWhile end<=watermark）。
- **watermark 转发一致性**：OperatorTask 统一转发 watermark 到下游，需保证链内算子先处理（WindowOperator 触发）再转发（避免下游提前推进）。顺序：chain.onWatermark → output.emit(watermark)。
- **滑动窗口状态膨胀**：一记录落入多窗口，MapState 多桶；阶段④学习范围可接受。
- **Record 加 timestamp 的回归**：阶段② `Record<T>(value)` 改 `Record<T>(value, timestamp)`，影响所有构造 Record 处（source/TimestampsAndWatermarksOperator）。全量回归保证。
- **Operator.onWatermark default 扩展**：不破坏既有算子（default 空），但 OperatorTask/OperatorChain 需接 watermark 处理。
