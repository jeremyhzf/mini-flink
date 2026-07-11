# Mini-Flink 阶段④（时间与窗口）实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在阶段③ keyed state 之上加入 event time + watermark + 窗口（滚动/滑动），实现基于事件时间的窗口聚合——窗口结束时输出最终聚合值一次。

**Architecture:** `Record` 加事件时间戳，ts 经 `RuntimeContext`（持当前 ts，对称 currentKey）在算子间流动；`Watermark` 作 `StreamElement` 流动，驱动 `TimerService` 的 event time 定时器；`WindowOperator` 用 per-key per-window `MapState<TimeWindow, ACC>`（复用阶段③）增量 reduce，watermark 推进到 window end 时触发输出最终值 + 清理。

**Tech Stack:** Java 17、Maven、JUnit 5、纯 JDK。

## Global Constraints

- Java 17（`maven.compiler.release=17`），Maven 构建。
- 包根 `org.miniflink`；依赖仅 JUnit 5（test scope），其余纯 JDK。
- 所有代码注释、commit message 使用中文。
- 每个任务结束必须 commit；TDD（先失败测试 → 实现 → 通过）。
- `Collector<T>` 接口**不可改动**（稳定边界）——`OutputCollector` 可持 `RuntimeContext` 但仍 `implements Collector<T>`，`collect(T)` 签名不变。
- 阶段④仍维持**单线性链 + 单 sink**。

---

## File Structure（阶段④新增/修改）

```
src/main/java/org/miniflink/
├── runtime/
│   ├── Record.java                      # 修改：加 long timestamp
│   ├── Watermark.java                   # 新增（StreamElement 实现）
│   ├── RuntimeContext.java              # 修改：加 getCurrentTimestamp/setCurrentTimestamp
│   ├── RuntimeContextImpl.java          # 修改：加 timestamp 字段
│   ├── Operator.java                    # 修改：加 default onWatermark(Watermark)
│   ├── OperatorChain.java               # 修改：加 onWatermark(Watermark)
│   ├── OperatorTask.java                # 修改：Record 设 ctx ts + Watermark 分发转发
│   ├── OutputCollector.java             # 修改：持 ctx，collect 读 ctx ts 包 Record
│   ├── Output.java                      # 修改：route(value, ts) + sendWatermark(Watermark)
│   ├── SourceTask.java                  # 修改：source 结束广播 Watermark(+∞) 再 EOB
│   ├── ChannelWriter.java               # 修改：Record 加默认 ts（遗留，仅测试用）
│   ├── Task.java                        # 修改：加 default broadcastWatermark
│   └── operator/
│       ├── TimestampsAndWatermarksOperator.java  # 新增（打 ts + 发 watermark）
│       └── WindowOperator.java                   # 新增（窗口聚合）
├── time/                                # 新增包
│   ├── TimestampAssigner.java           # 新增
│   ├── WatermarkStrategy.java           # 新增（接口 + 工厂）
│   ├── BoundedOutOfOrdernessWatermarks.java  # 新增（实现）
│   ├── TimerService.java                # 新增（接口）
│   ├── TimerHandler.java                # 新增（回调接口）
│   └── InternalTimerService.java        # 新增（实现）
├── window/                              # 新增包
│   ├── Window.java                      # 新增（abstract）
│   ├── TimeWindow.java                  # 新增
│   ├── WindowAssigner.java              # 新增（abstract）
│   ├── TumblingEventTimeWindows.java    # 新增
│   ├── SlidingEventTimeWindows.java     # 新增
│   ├── Trigger.java                     # 新增（abstract）
│   ├── TriggerResult.java               # 新增（enum）
│   ├── TriggerContext.java              # 新增（接口）
│   └── EventTimeTrigger.java            # 新增
├── api/
│   ├── WindowedStream.java              # 新增
│   ├── DataStream.java                  # 修改：加 assignTimestampsAndWatermarks
│   └── KeyedStream.java                 # 修改：加 window(WindowAssigner)
└── src/test/java/org/miniflink/...      # 各任务单测
```

### 跨任务类型契约（权威）

```java
// runtime 修改
record Record<T>(T value, long timestamp) implements StreamElement { }       // Task 1
final class Watermark implements StreamElement { long getTimestamp(); }      // Task 2
interface RuntimeContext {                                                  // Task 1 加
    ...; long getCurrentTimestamp(); void setCurrentTimestamp(long ts);
}
interface Operator<IN,OUT> {                                                // Task 2 加
    ...; default void onWatermark(Watermark wm) {}
}
class OperatorChain<IN,OUT> {                                               // Task 2 加
    ...; void onWatermark(Watermark wm);
}
class Output {                                                              // Task 1/2
    void route(Object value, long timestamp, int upstreamIndex);            // Task 1：加 timestamp
    void sendWatermark(Watermark wm);                                       // Task 2：广播所有下游
    void sendEob(int upstreamIndex);
}

// time 新增
interface TimestampAssigner<T> { long extractTimestamp(T record); }                         // Task 3
interface WatermarkStrategy<T> {                                                            // Task 3
    long extractTimestamp(T record); long currentWatermark();
    static <T> WatermarkStrategy<T> forBoundedOutOfOrderness(Duration maxOutOfOrderness, TimestampAssigner<T> assigner);
}
class TimestampsAndWatermarksOperator<T> implements Operator<T,T> { ... }                   // Task 3
interface TimerService { long currentWatermark(); void registerEventTimeTimer(long); void deleteEventTimeTimer(long); }  // Task 4
interface TimerHandler { void onEventTime(long time); }                                     // Task 4
class InternalTimerService implements TimerService { void advanceTo(long wm, TimerHandler h); ... }  // Task 4

// window 新增
abstract class Window { abstract long start(); abstract long end(); }                        // Task 5
class TimeWindow(long start, long end) extends Window { ... }                               // Task 5
abstract class WindowAssigner<T, W extends Window> {                                        // Task 5
    abstract Collection<W> assignWindows(T record, long timestamp); abstract boolean isEventTime();
}
class TumblingEventTimeWindows<T> extends WindowAssigner<T, TimeWindow> { static ...of(Duration); }   // Task 5
class SlidingEventTimeWindows<T> extends WindowAssigner<T, TimeWindow> { static ...of(Duration,Duration); }  // Task 5
enum TriggerResult { CONTINUE, FIRE_AND_PURGE }                                            // Task 6
abstract class Trigger<T,W extends Window> {                                                // Task 6
    abstract TriggerResult onElement(T element, long ts, W window, TriggerContext ctx);
    abstract TriggerResult onEventTime(long time, W window, TriggerContext ctx);
}
interface TriggerContext { long getCurrentWatermark(); void registerEventTimeTimer(long); } // Task 6
class EventTimeTrigger<T,W> extends Trigger<T,W> { ... }                                    // Task 6
class WindowOperator<IN> implements Operator<IN,IN>, TimerHandler { ... }                   // Task 7

// api 新增/修改
class WindowedStream<T, W extends Window> { DataStream<T> reduce(ReduceFunction<T>); }      // Task 8
class KeyedStream<T,K> { <W extends Window> WindowedStream<T,W> window(WindowAssigner<T,W>); }  // Task 8
class DataStream<T> { DataStream<T> assignTimestampsAndWatermarks(WatermarkStrategy<T>); }  // Task 3
```

### ts 流动机制（贯穿全计划，必读）

算子接口收 `value`（从 `Record.value()` 取），timestamp 不在参数里。ts 经 `RuntimeContext` 流动（对称 currentKey）：
- **入链**：`OperatorTask` 收 `Record(value, ts)` → `ctx.setCurrentTimestamp(ts)` → `chain.processElement(value)`。
- **链内**：`ChainCollector.collect(value)` 直接调下游 `processElement(value)`（不经 Record，共享同一 ctx ts）。
- **出链**：链尾 `OutputCollector.collect(value)` → `route(value, ctx.getCurrentTimestamp())` → `new Record<>(value, ts)` → 下游 Channel。
- **TS&WM 算子**（Task 3）：`extractTimestamp(value)` 得 realTs → `ctx.setCurrentTimestamp(realTs)` → 输出 value（OutputCollector 读 ctx realTs 包 Record）。
- **WindowOperator**（Task 7）：`ctx.getCurrentTimestamp()` 读 ts 分配窗口。

source 输出 ts = `Long.MIN_VALUE`（ctx 初始值，TS&WM 覆盖）。`Collector<T>` 接口全程不变。

---

## Task 1: Record 加 timestamp + RuntimeContext 持 ts + ts 流动

> 让 timestamp 随数据经 ctx 流动：Record 加 ts 字段；RuntimeContext 持当前 ts；OperatorTask 收 Record 设 ctx ts；OutputCollector 读 ctx ts 包 Record 出链；Output.route 带 ts。无新行为（尚无窗口算子用 ts），验证 = 全量回归。

**Files:**
- Modify: `src/main/java/org/miniflink/runtime/Record.java`
- Modify: `src/main/java/org/miniflink/runtime/RuntimeContext.java`
- Modify: `src/main/java/org/miniflink/runtime/RuntimeContextImpl.java`
- Modify: `src/main/java/org/miniflink/runtime/OutputCollector.java`
- Modify: `src/main/java/org/miniflink/runtime/Output.java`
- Modify: `src/main/java/org/miniflink/runtime/OperatorTask.java`
- Modify: `src/main/java/org/miniflink/runtime/ChannelWriter.java`
- Modify: 测试 `OperatorTaskTest`/`ChannelTest`/`StreamElementTest`（适配 Record 两参）

**Interfaces:**
- Produces: `Record<T>(T value, long timestamp)`；`RuntimeContext.getCurrentTimestamp()/setCurrentTimestamp(long)`；`Output.route(Object value, long timestamp, int upstreamIndex)`；`OutputCollector(List<Output>, RuntimeContext ctx)`。

- [ ] **Step 1: 修改 `Record` 加 timestamp**

```java
package org.miniflink.runtime;

/** 携带一条用户数据及其事件时间戳的通道元素。 */
public record Record<T>(T value, long timestamp) implements StreamElement {
}
```

- [ ] **Step 2: 修改 `RuntimeContext` 加 ts 访问**

```java
package org.miniflink.runtime;

import org.miniflink.api.function.KeySelector;

/** 算子运行时上下文（per-subtask）：并行位置 + keyed state + currentKey + 当前记录的事件时间戳。 */
public interface RuntimeContext {
    int getSubtaskIndex();
    int getParallelism();
    Object getCurrentKey();
    void setCurrentKey(Object key);
    StateBackend getStateBackend();
    KeySelector<?, ?> getKeySelector();
    long getCurrentTimestamp();          // 当前记录的事件时间戳（OperatorTask 在 processElement 前设置）
    void setCurrentTimestamp(long ts);
}
```

- [ ] **Step 3: 修改 `RuntimeContextImpl` 加 timestamp 字段**

```java
package org.miniflink.runtime;

import org.miniflink.api.function.KeySelector;

/** RuntimeContext 默认实现：内置 MemoryStateBackend，持有 currentKey 与 currentTimestamp。 */
public class RuntimeContextImpl implements RuntimeContext {
    private final int subtaskIndex;
    private final int parallelism;
    private final KeySelector<?, ?> keySelector;
    private final MemoryStateBackend backend = new MemoryStateBackend();
    private long currentTimestamp = Long.MIN_VALUE;   // source 输出初始值；TS&WM 算子覆盖

    public RuntimeContextImpl(int subtaskIndex, int parallelism, KeySelector<?, ?> keySelector) {
        this.subtaskIndex = subtaskIndex;
        this.parallelism = parallelism;
        this.keySelector = keySelector;
    }

    @Override public int getSubtaskIndex() { return subtaskIndex; }
    @Override public int getParallelism() { return parallelism; }
    @Override public Object getCurrentKey() { return backend.currentKey(); }
    @Override public void setCurrentKey(Object key) { backend.setCurrentKey(key); }
    @Override public StateBackend getStateBackend() { return backend; }
    @Override public KeySelector<?, ?> getKeySelector() { return keySelector; }
    @Override public long getCurrentTimestamp() { return currentTimestamp; }
    @Override public void setCurrentTimestamp(long ts) { this.currentTimestamp = ts; }
}
```

- [ ] **Step 4: 修改 `Output.route` 加 timestamp 参数**

把 `route(Object value, int upstreamIndex)` 改为：
```java
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void route(Object value, long timestamp, int upstreamIndex) throws Exception {
        Object key = (keySelector != null) ? ((KeySelector) keySelector).getKey(value) : null;
        int idx = partitioner.selectChannel(downstreamChannels.size(), key, upstreamIndex);
        downstreamChannels.get(idx).send(new Record<>(value, timestamp));
    }
```

- [ ] **Step 5: 修改 `OutputCollector` 持 ctx，collect 读 ctx ts**

整体替换 `OutputCollector.java`：
```java
package org.miniflink.runtime;

import java.util.List;

/**
 * Collector 实现：collect 时把记录按分区器路由到下游 Channel（fan-out）。
 * 持有 RuntimeContext 以读取当前记录的事件时间戳，包装进 Record 出链。
 */
public class OutputCollector<T> implements Collector<T> {
    private final List<Output> outputs;
    private final int upstreamIndex;
    private final RuntimeContext ctx;

    public OutputCollector(List<Output> outputs, RuntimeContext ctx) {
        this.outputs = outputs;
        this.ctx = ctx;
        this.upstreamIndex = ctx.getSubtaskIndex();
    }

    @Override
    public void collect(T record) {
        for (Output o : outputs) {
            try {
                o.route(record, ctx.getCurrentTimestamp(), upstreamIndex);
            } catch (Exception e) {
                throw new RuntimeException("输出路由异常", e);
            }
        }
    }

    @Override
    public void close() {
        // EOB 由 Task 统一发送
    }
}
```

- [ ] **Step 6: 修改 `OperatorTask`——收 Record 设 ctx ts + 适配 OutputCollector 构造**

把 `run()` 内的建 Task 循环体改为（设 ctx ts + OutputCollector 持 ctx）：
```java
    @SuppressWarnings({"rawtypes", "unchecked"})
    @Override
    public void run() {
        Collector outCollector = outputs.isEmpty() ? new NoopCollector<>() : new OutputCollector(outputs, ctx);
        try {
            chain.open((Collector) outCollector, ctx);
            @SuppressWarnings("rawtypes")
            OperatorChain rawChain = chain;
            int remaining = pendingUpstreams;
            while (remaining > 0) {
                StreamElement e = input.receive();
                if (e == EndOfBroadcast.INSTANCE) {
                    remaining--;
                } else if (e instanceof Record<?> r) {
                    ctx.setCurrentTimestamp(r.timestamp());   // 入链：设当前 ts
                    rawChain.processElement(r.value());
                }
            }
            broadcastEob(outputs, ctx.getSubtaskIndex());
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            throw new RuntimeException("OperatorTask 执行异常", e);
        } finally {
            chain.close();
        }
    }
```
（注意：`OutputCollector` 构造从 `(outputs, ctx.getSubtaskIndex())` 改为 `(outputs, ctx)`。）

- [ ] **Step 7: 修改 `SourceTask`——OutputCollector 持 ctx**

把 `run()` 内 `OutputCollector out = new OutputCollector(outputs, ctx.getSubtaskIndex());` 改为：
```java
        OutputCollector out = new OutputCollector(outputs, ctx);
```

- [ ] **Step 8: 修改 `ChannelWriter`——Record 加默认 ts（遗留，仅测试用）**

把 `collect` 内 `channel.send(new Record<>(record));` 改为：
```java
            channel.send(new Record<>(record, Long.MIN_VALUE));
```

- [ ] **Step 9: 适配测试里的 `new Record<>(value)`（加 ts 参数，任意值即可）**

把 `OperatorTaskTest`/`ChannelTest`/`StreamElementTest` 里所有 `new Record<>(X)` 改为 `new Record<>(X, 0L)`：
- `OperatorTaskTest.java:17,18,36,38`
- `ChannelTest.java:12,13,24,27`（注意 27 行在 lambda 内）
- `StreamElementTest.java:10`

- [ ] **Step 10: 全量回归**

Run: `mvn -q test`
Expected: `BUILD SUCCESS`，阶段①②③ 共 57 个测试全部通过（ts 流动无新行为，OutputCollector 持 ctx 不影响现有逻辑）。

- [ ] **Step 11: 提交**

```bash
git add -A
git commit -m "feat(runtime): Record 加事件时间戳 + RuntimeContext 持 ts + ts 经 ctx 流动

Co-Authored-By: Claude <noreply@anthropic.com>"
```

## Task 2: Watermark 元素 + watermark 流动 + source 结束发 +∞

> 新增 `Watermark` StreamElement；`Operator.onWatermark` default + `OperatorChain.onWatermark` 透传；`OperatorTask` 收 Watermark 分发链 + 转发下游；`Output.sendWatermark` 广播；`SourceTask` source 结束广播 `Watermark(Long.MAX_VALUE)` 触发所有剩余窗口，再广播 EOB。

**Files:**
- Create: `src/main/java/org/miniflink/runtime/Watermark.java`
- Modify: `src/main/java/org/miniflink/runtime/Operator.java`
- Modify: `src/main/java/org/miniflink/runtime/OperatorChain.java`
- Modify: `src/main/java/org/miniflink/runtime/OperatorTask.java`
- Modify: `src/main/java/org/miniflink/runtime/Output.java`
- Modify: `src/main/java/org/miniflink/runtime/Task.java`
- Modify: `src/main/java/org/miniflink/runtime/SourceTask.java`
- Test: `src/test/java/org/miniflink/runtime/WatermarkFlowTest.java`

**Interfaces:**
- Consumes: `Record(value, ts)`（Task 1）。
- Produces: `Watermark(long)`；`Operator.onWatermark(Watermark)` default；`OperatorChain.onWatermark(Watermark)`；`Output.sendWatermark(Watermark)`；`Task.broadcastWatermark(List<Output>, Watermark)`。

- [ ] **Step 1: 写失败测试 `WatermarkFlowTest`**

```java
package org.miniflink.runtime;

import org.junit.jupiter.api.Test;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

class WatermarkFlowTest {

    @Test
    void Watermark携带timestamp() {
        Watermark wm = new Watermark(123L);
        assertEquals(123L, wm.getTimestamp());
    }

    @Test
    void Output的sendWatermark广播到所有下游channel() throws Exception {
        Channel c1 = new Channel();
        Channel c2 = new Channel();
        Output output = new Output(List.of(c1, c2), new org.miniflink.execution.ForwardPartitioner(), null);
        Watermark wm = new Watermark(42L);
        output.sendWatermark(wm);
        // watermark 不分区：所有下游 channel 都收到同一 Watermark
        assertInstanceOf(Watermark.class, c1.receive());
        assertInstanceOf(Watermark.class, c2.receive());
        assertEquals(42L, ((Watermark) c2.receive()).getTimestamp());
    }
}
```

- [ ] **Step 2: 运行测试验证失败**

Run: `mvn -q -Dtest=WatermarkFlowTest test`
Expected: 编译失败 —— `Watermark`/`Output.sendWatermark` 不存在。

- [ ] **Step 3: 创建 `Watermark`**

```java
package org.miniflink.runtime;

/** 事件时间水位线：表示 event time 进度。随数据流传播，推进下游算子时钟。 */
public final class Watermark implements StreamElement {
    private final long timestamp;

    public Watermark(long timestamp) {
        this.timestamp = timestamp;
    }

    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public String toString() {
        return "Watermark(" + timestamp + ")";
    }
}
```

- [ ] **Step 4: 修改 `Operator` 加 `onWatermark` default**

```java
package org.miniflink.runtime;

/** 处理算子接口。 */
public interface Operator<IN, OUT> {
    void open(Collector<OUT> out, RuntimeContext ctx);
    void processElement(IN record) throws Exception;
    void close();

    /** 收到 watermark（事件时间推进）。普通算子默认不处理（由 OperatorTask 统一转发）。 */
    default void onWatermark(Watermark watermark) {
        // 默认空：map/filter 等透传算子不消费 watermark
    }

    Operator<IN, OUT> copy();
}
```

- [ ] **Step 5: 修改 `OperatorChain` 加 `onWatermark`（透传链内算子）**

在 `OperatorChain` 加方法：
```java
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void onWatermark(Watermark watermark) {
        // 链内每个算子都收到 watermark（链化算子共享线程，watermark 流过链）
        for (Operator<?, ?> op : operators) {
            ((Operator) op).onWatermark(watermark);
        }
    }
```

- [ ] **Step 6: 修改 `Output` 加 `sendWatermark`（广播所有下游 channel）**

在 `Output` 加方法：
```java
    /** 向所有下游 channel 广播 watermark（watermark 不分区）。 */
    public void sendWatermark(Watermark wm) {
        for (Channel c : downstreamChannels) {
            try {
                c.send(wm);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("发送 Watermark 被中断", e);
            }
        }
    }
```

- [ ] **Step 7: 修改 `Task` 加 `broadcastWatermark` default**

```java
package org.miniflink.runtime;

import java.util.List;

/** 多线程执行单元。 */
public interface Task extends Runnable {

    default void broadcastEob(List<Output> outputs, int upstreamIndex) {
        for (Output o : outputs) {
            o.sendEob(upstreamIndex);
        }
    }

    /** 向所有出边广播 watermark（source 结束 / 算子转发用）。 */
    default void broadcastWatermark(List<Output> outputs, Watermark wm) {
        for (Output o : outputs) {
            o.sendWatermark(wm);
        }
    }
}
```

- [ ] **Step 8: 修改 `OperatorTask`——收 Watermark 分发链 + 转发下游**

在 `run()` 的主循环 `else if (e instanceof Record<?> r)` 后加 Watermark 分支：
```java
                } else if (e instanceof Watermark wm) {
                    chain.onWatermark(wm);                       // 链内算子处理（WindowOperator 触发窗口）
                    broadcastWatermark(outputs, wm);             // 转发到下游（保持 watermark 流）
                }
```
（完整循环：EOB → 计数；Record → 设 ctx ts + processElement；Watermark → chain.onWatermark + broadcastWatermark。）

- [ ] **Step 9: 修改 `SourceTask`——source 结束广播 `Watermark(+∞)` 再 EOB**

把 `run()` 内 `sourceOperator.run();` 后改为：
```java
            sourceOperator.run();
            broadcastWatermark(outputs, new Watermark(Long.MAX_VALUE));  // +∞：触发所有剩余窗口
            broadcastEob(outputs, ctx.getSubtaskIndex());
```

- [ ] **Step 10: 运行测试验证通过 + 全量回归**

Run: `mvn -q -Dtest=WatermarkFlowTest test`
Expected: `BUILD SUCCESS`，`Tests run: 2`。

Run: `mvn -q test`
Expected: 全量 59 通过（57 + Task2 新增 2），无回归。

- [ ] **Step 11: 提交**

```bash
git add src/main/java/org/miniflink/runtime/Watermark.java \
        src/main/java/org/miniflink/runtime/Operator.java \
        src/main/java/org/miniflink/runtime/OperatorChain.java \
        src/main/java/org/miniflink/runtime/OperatorTask.java \
        src/main/java/org/miniflink/runtime/Output.java \
        src/main/java/org/miniflink/runtime/Task.java \
        src/main/java/org/miniflink/runtime/SourceTask.java \
        src/test/java/org/miniflink/runtime/WatermarkFlowTest.java
git commit -m "feat(runtime): Watermark 元素 + watermark 流动 + source 结束发 +∞

Co-Authored-By: Claude <noreply@anthropic.com>"
```

## Task 3: WatermarkStrategy + TimestampsAndWatermarksOperator

> 新增 `TimestampAssigner` + `WatermarkStrategy`（`forBoundedOutOfOrderness`）+ `BoundedOutOfOrdernessWatermarks` + `TimestampsAndWatermarksOperator`（打 ts + 发 watermark）+ `DataStream.assignTimestampsAndWatermarks`。

**Files:**
- Create: `src/main/java/org/miniflink/time/TimestampAssigner.java`
- Create: `src/main/java/org/miniflink/time/WatermarkStrategy.java`
- Create: `src/main/java/org/miniflink/time/BoundedOutOfOrdernessWatermarks.java`
- Create: `src/main/java/org/miniflink/runtime/operator/TimestampsAndWatermarksOperator.java`
- Modify: `src/main/java/org/miniflink/api/DataStream.java`
- Test: `src/test/java/org/miniflink/time/WatermarkStrategyTest.java`

**Interfaces:**
- Consumes: `Operator.open(Collector, RuntimeContext)`、`ctx.setCurrentTimestamp(long)`（Task 1）、`Watermark`（Task 2）。
- Produces: `TimestampAssigner<T>.extractTimestamp(T):long`；`WatermarkStrategy<T>`（含 `forBoundedOutOfOrderness`）；`TimestampsAndWatermarksOperator`；`DataStream.assignTimestampsAndWatermarks(WatermarkStrategy)`。

- [ ] **Step 1: 写失败测试 `WatermarkStrategyTest`**

```java
package org.miniflink.time;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class WatermarkStrategyTest {

    @Test
    void boundedOutOfOrderness生成watermark等于maxTs减乱序容忍() {
        WatermarkStrategy<String> strategy = WatermarkStrategy
                .<String>forBoundedOutOfOrderness(java.time.Duration.ofMillis(100), s -> Long.parseLong(s));
        assertEquals(Long.MIN_VALUE, strategy.currentWatermark());   // 初始

        strategy.extractTimestamp("1000");   // maxTs=1000
        assertEquals(900, strategy.currentWatermark());              // 1000-100

        strategy.extractTimestamp("500");    // 乱序到达，maxTs 不变
        assertEquals(900, strategy.currentWatermark());              // 仍 900（maxTs 单调）

        strategy.extractTimestamp("3000");   // maxTs=3000
        assertEquals(2900, strategy.currentWatermark());             // 3000-100
    }
}
```

- [ ] **Step 2: 运行测试验证失败**

Run: `mvn -q -Dtest=WatermarkStrategyTest test`
Expected: 编译失败 —— `WatermarkStrategy` 等不存在。

- [ ] **Step 3: 创建 `TimestampAssigner`**

```java
package org.miniflink.time;

/** 从记录提取事件时间戳。 */
@FunctionalInterface
public interface TimestampAssigner<T> {
    long extractTimestamp(T record);
}
```

- [ ] **Step 4: 创建 `WatermarkStrategy`**

```java
package org.miniflink.time;

import java.time.Duration;

/**
 * 事件时间策略：提取 timestamp + 生成 watermark。
 * 仿 Flink WatermarkStrategy。
 */
public interface WatermarkStrategy<T> extends TimestampAssigner<T> {
    /** 当前 watermark（单调递增，只增不减）。 */
    long currentWatermark();

    /** 固定乱序容忍策略：watermark = maxObservedTimestamp - maxOutOfOrderness。 */
    static <T> WatermarkStrategy<T> forBoundedOutOfOrderness(Duration maxOutOfOrderness, TimestampAssigner<T> assigner) {
        return new BoundedOutOfOrdernessWatermarks<>(maxOutOfOrderness.toMillis(), assigner);
    }
}
```

- [ ] **Step 5: 创建 `BoundedOutOfOrdernessWatermarks`**

```java
package org.miniflink.time;

/** 固定乱序容忍实现：维护 maxTimestamp，currentWatermark = maxTimestamp - maxOutOfOrderness（单调）。 */
public class BoundedOutOfOrdernessWatermarks<T> implements WatermarkStrategy<T> {
    private final long maxOutOfOrderness;
    private final TimestampAssigner<T> assigner;
    private long maxTimestamp = Long.MIN_VALUE;

    public BoundedOutOfOrdernessWatermarks(long maxOutOfOrderness, TimestampAssigner<T> assigner) {
        this.maxOutOfOrderness = maxOutOfOrderness;
        this.assigner = assigner;
    }

    @Override
    public long extractTimestamp(T record) {
        long ts = assigner.extractTimestamp(record);
        if (ts > maxTimestamp) {
            maxTimestamp = ts;   // 单调：只增不减
        }
        return ts;
    }

    @Override
    public long currentWatermark() {
        // maxTimestamp - maxOutOfOrderness；maxTimestamp 为 MIN 时（无数据）返回 MIN
        return (maxTimestamp == Long.MIN_VALUE) ? Long.MIN_VALUE : maxTimestamp - maxOutOfOrderness;
    }
}
```

- [ ] **Step 6: `RuntimeContext`/`RuntimeContextImpl` 加 `emitWatermark`（算子发 watermark 的通道）**

> 设计：算子只持有 `Collector`（接口不可改，只有 `collect(value)`），无法直接发 `Watermark` 到 `Output`。故 watermark 发送经 `RuntimeContext.emitWatermark`——由 `OperatorTask` 在 open 前注入"把 watermark 广播到本 subtask outputs"的 emitter。`RuntimeContextImpl` 用 setter 接收，避免改 `StreamExecutor` 构造签名。

`RuntimeContext` 接口加方法：
```java
    void emitWatermark(Watermark wm);
```
`RuntimeContextImpl` 加字段 + setter + 实现：
```java
    private java.util.function.Consumer<Watermark> watermarkEmitter;   // 由 OperatorTask 注入

    public void setWatermarkEmitter(java.util.function.Consumer<Watermark> emitter) {
        this.watermarkEmitter = emitter;
    }

    @Override
    public void emitWatermark(Watermark wm) {
        if (watermarkEmitter != null) {
            watermarkEmitter.accept(wm);
        }
    }
```

- [ ] **Step 7: `OperatorTask` 注入 watermark emitter**

在 `run()` 的 `chain.open((Collector) outCollector, ctx);` **之前**加：
```java
            if (ctx instanceof RuntimeContextImpl impl) {
                impl.setWatermarkEmitter(wm -> broadcastWatermark(outputs, wm));
            }
```

- [ ] **Step 8: 创建 `TimestampsAndWatermarksOperator`（打 ts + 发 watermark，最终实现）**

```java
package org.miniflink.runtime.operator;

import org.miniflink.runtime.Collector;
import org.miniflink.runtime.Operator;
import org.miniflink.runtime.RuntimeContext;
import org.miniflink.runtime.Watermark;
import org.miniflink.time.WatermarkStrategy;

/**
 * 打事件时间戳 + 发 watermark 的算子。
 * processElement：extractTimestamp → 设 ctx ts（OutputCollector 读它包 Record(value, ts) 流向下游）
 *               → 输出 value → 发当前 watermark（单调）。
 */
public class TimestampsAndWatermarksOperator<T> implements Operator<T, T> {
    private final WatermarkStrategy<T> strategy;
    private Collector<T> out;
    private RuntimeContext ctx;

    public TimestampsAndWatermarksOperator(WatermarkStrategy<T> strategy) {
        this.strategy = strategy;
    }

    @Override
    public void open(Collector<T> out, RuntimeContext ctx) {
        this.out = out;
        this.ctx = ctx;
    }

    @Override
    public void processElement(T record) {
        long ts = strategy.extractTimestamp(record);
        ctx.setCurrentTimestamp(ts);                                   // 设 ts，OutputCollector 读它包 Record
        out.collect(record);                                           // 输出 value（带 ts 流向下游）
        ctx.emitWatermark(new Watermark(strategy.currentWatermark())); // 每条记录后发当前 watermark
    }

    @Override
    public void close() { /* 无操作 */ }

    @Override
    public TimestampsAndWatermarksOperator<T> copy() {
        return new TimestampsAndWatermarksOperator<>(strategy);        // 共享无状态 strategy
    }
}
```

- [ ] **Step 9: 修改 `DataStream` 加 `assignTimestampsAndWatermarks`**

```java
    /** 打事件时间戳并生成 watermark（独立算子）。 */
    public DataStream<T> assignTimestampsAndWatermarks(org.miniflink.time.WatermarkStrategy<T> strategy) {
        return transform("timestamps-and-watermarks",
                new org.miniflink.runtime.operator.TimestampsAndWatermarksOperator<>(strategy));
    }
```

- [ ] **Step 10: 运行测试验证通过 + 全量回归**

Run: `mvn -q -Dtest=WatermarkStrategyTest test`
Expected: `Tests run: 1`。

Run: `mvn -q test`
Expected: 全量 60 通过（59 + 1），无回归。

- [ ] **Step 11: 提交**

```bash
git add src/main/java/org/miniflink/time/ \
        src/main/java/org/miniflink/runtime/operator/TimestampsAndWatermarksOperator.java \
        src/main/java/org/miniflink/runtime/RuntimeContext.java \
        src/main/java/org/miniflink/runtime/RuntimeContextImpl.java \
        src/main/java/org/miniflink/runtime/OperatorTask.java \
        src/main/java/org/miniflink/api/DataStream.java \
        src/test/java/org/miniflink/time/WatermarkStrategyTest.java
git commit -m "feat(time): WatermarkStrategy + TimestampsAndWatermarksOperator + assignTimestampsAndWatermarks

Co-Authored-By: Claude <noreply@anthropic.com>"
```

## Task 4: TimerService + InternalTimerService

> event time 定时器：注册/删除 timer，watermark 推进时触发到点 timer 回调。

**Files:**
- Create: `src/main/java/org/miniflink/time/TimerService.java`
- Create: `src/main/java/org/miniflink/time/TimerHandler.java`
- Create: `src/main/java/org/miniflink/time/InternalTimerService.java`
- Test: `src/test/java/org/miniflink/time/InternalTimerServiceTest.java`

**Interfaces:**
- Produces: `TimerService.currentWatermark()/registerEventTimeTimer(long)/deleteEventTimeTimer(long)`；`TimerHandler.onEventTime(long)`；`InternalTimerService.advanceTo(long, TimerHandler)`。

- [ ] **Step 1: 写失败测试 `InternalTimerServiceTest`**

```java
package org.miniflink.time;

import org.junit.jupiter.api.Test;
import java.util.ArrayList;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

class InternalTimerServiceTest {

    @Test
    void advanceTo触发到点timer按time顺序() {
        InternalTimerService svc = new InternalTimerService();
        List<Long> fired = new ArrayList<>();
        TimerHandler handler = fired::add;

        svc.registerEventTimeTimer(300);
        svc.registerEventTimeTimer(100);
        svc.registerEventTimeTimer(200);

        svc.advanceTo(150L, handler);   // 只触发 100
        assertEquals(List.of(100L), fired);
        assertEquals(150L, svc.currentWatermark());

        svc.advanceTo(300L, handler);   // 触发 200, 300
        assertEquals(List.of(100L, 200L, 300L), fired);
    }

    @Test
    void deleteTimer不触发() {
        InternalTimerService svc = new InternalTimerService();
        List<Long> fired = new ArrayList<>();
        svc.registerEventTimeTimer(100);
        svc.deleteEventTimeTimer(100);
        svc.advanceTo(200L, fired::add);
        assertTrue(fired.isEmpty());
    }
}
```

- [ ] **Step 2: 运行测试验证失败**

Run: `mvn -q -Dtest=InternalTimerServiceTest test`
Expected: 编译失败 —— `TimerService`/`InternalTimerService` 不存在。

- [ ] **Step 3: 创建 `TimerService`**

```java
package org.miniflink.time;

/** event time 定时器服务：算子注册定时器，watermark 推进时触发。 */
public interface TimerService {
    long currentWatermark();
    void registerEventTimeTimer(long time);
    void deleteEventTimeTimer(long time);
}
```

- [ ] **Step 4: 创建 `TimerHandler`**

```java
package org.miniflink.time;

/** timer 触发时的回调（算子实现）。 */
@FunctionalInterface
public interface TimerHandler {
    void onEventTime(long time);
}
```

- [ ] **Step 5: 创建 `InternalTimerService`**

```java
package org.miniflink.time;

import java.util.TreeSet;

/** event time 定时器实现：TreeSet 按 time 去重排序，advanceTo 触发所有 time<=watermark 的 timer。 */
public class InternalTimerService implements TimerService {
    private final TreeSet<Long> eventTimeTimers = new TreeSet<>();
    private long currentWatermark = Long.MIN_VALUE;

    @Override
    public long currentWatermark() {
        return currentWatermark;
    }

    @Override
    public void registerEventTimeTimer(long time) {
        eventTimeTimers.add(time);
    }

    @Override
    public void deleteEventTimeTimer(long time) {
        eventTimeTimers.remove(time);
    }

    /** 推进 watermark 到给定值，触发所有 time <= watermark 的 timer（按升序），回调 handler。 */
    public void advanceTo(long watermark, TimerHandler handler) {
        this.currentWatermark = Math.max(this.currentWatermark, watermark);   // 单调
        while (!eventTimeTimers.isEmpty() && eventTimeTimers.first() <= watermark) {
            long time = eventTimeTimers.pollFirst();
            handler.onEventTime(time);
        }
    }
}
```

- [ ] **Step 6: 运行测试验证通过**

Run: `mvn -q -Dtest=InternalTimerServiceTest test`
Expected: `Tests run: 2, Failures: 0`。

- [ ] **Step 7: 提交**

```bash
git add src/main/java/org/miniflink/time/TimerService.java \
        src/main/java/org/miniflink/time/TimerHandler.java \
        src/main/java/org/miniflink/time/InternalTimerService.java \
        src/test/java/org/miniflink/time/InternalTimerServiceTest.java
git commit -m "feat(time): TimerService + InternalTimerService（event time 定时器）

Co-Authored-By: Claude <noreply@anthropic.com>"
```

## Task 5: Window + TimeWindow + WindowAssigner（Tumbling/Sliding）

> 窗口定义 + 分配器：滚动（单窗口）/ 滑动（多窗口）。

**Files:**
- Create: `src/main/java/org/miniflink/window/Window.java`
- Create: `src/main/java/org/miniflink/window/TimeWindow.java`
- Create: `src/main/java/org/miniflink/window/WindowAssigner.java`
- Create: `src/main/java/org/miniflink/window/TumblingEventTimeWindows.java`
- Create: `src/main/java/org/miniflink/window/SlidingEventTimeWindows.java`
- Test: `src/test/java/org/miniflink/window/WindowAssignerTest.java`

**Interfaces:**
- Produces: `Window.start()/end()`；`TimeWindow(long start, long end)`；`WindowAssigner<T,W>.assignWindows(T, long):Collection<W>/isEventTime()`；`TumblingEventTimeWindows.of(Duration)`；`SlidingEventTimeWindows.of(Duration, Duration)`。

- [ ] **Step 1: 写失败测试 `WindowAssignerTest`**

```java
package org.miniflink.window;

import org.junit.jupiter.api.Test;
import java.util.Collection;
import static org.junit.jupiter.api.Assertions.*;

class WindowAssignerTest {

    @Test
    void 滚动窗口分配单个窗口() {
        TumblingEventTimeWindows<Object> assigner = TumblingEventTimeWindows.of(java.time.Duration.ofSeconds(1));
        Collection<TimeWindow> ws = assigner.assignWindows(null, 1500L);   // 1500ms → [1000, 2000)
        assertEquals(1, ws.size());
        TimeWindow w = ws.iterator().next();
        assertEquals(1000L, w.start());
        assertEquals(2000L, w.end());
        assertTrue(assigner.isEventTime());
    }

    @Test
    void 滑动窗口分配多个重叠窗口() {
        // size=3s, slide=1s → 一记录落入 3 个窗口
        SlidingEventTimeWindows<Object> assigner = SlidingEventTimeWindows.of(
                java.time.Duration.ofSeconds(3), java.time.Duration.ofSeconds(1));
        Collection<TimeWindow> ws = assigner.assignWindows(null, 3500L);
        assertEquals(3, ws.size());
        // 3500ms 落入 [1000,4000)、[2000,5000)、[3000,6000)
        // 验证每个窗口都含 3500
        for (TimeWindow w : ws) {
            assertTrue(w.start() <= 3500L && 3500L < w.end());
        }
    }

    @Test
    void TimeWindow的equals按start和end() {
        assertEquals(new TimeWindow(1000, 2000), new TimeWindow(1000, 2000));
        assertNotEquals(new TimeWindow(1000, 2000), new TimeWindow(1000, 3000));
    }
}
```

- [ ] **Step 2: 运行测试验证失败**

Run: `mvn -q -Dtest=WindowAssignerTest test`
Expected: 编译失败。

- [ ] **Step 3: 创建 `Window`（abstract）**

```java
package org.miniflink.window;

/** 窗口抽象（时间区间）。阶段④仅 TimeWindow，为后续 GlobalWindow 等留口。 */
public abstract class Window {
    public abstract long start();
    public abstract long end();
}
```

- [ ] **Step 4: 创建 `TimeWindow`**

```java
package org.miniflink.window;

import java.util.Objects;

/** 时间窗口 [start, end)。equals/hashCode 按 start+end（作 per-key per-window state 寻址 key）。 */
public class TimeWindow extends Window {
    private final long start;
    private final long end;

    public TimeWindow(long start, long end) {
        this.start = start;
        this.end = end;
    }

    @Override public long start() { return start; }
    @Override public long end() { return end; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TimeWindow that)) return false;
        return start == that.start && end == that.end;
    }

    @Override
    public int hashCode() {
        return Objects.hash(start, end);
    }

    @Override
    public String toString() {
        return "TimeWindow[" + start + "," + end + ")";
    }
}
```

- [ ] **Step 5: 创建 `WindowAssigner`（abstract）**

```java
package org.miniflink.window;

import java.util.Collection;

/** 把记录分配到一个（滚动）或多个（滑动）窗口。 */
public abstract class WindowAssigner<T, W extends Window> {
    public abstract Collection<W> assignWindows(T record, long timestamp);
    public abstract boolean isEventTime();
}
```

- [ ] **Step 6: 创建 `TumblingEventTimeWindows`**

```java
package org.miniflink.window;

import java.time.Duration;
import java.util.Collection;
import java.util.List;

/** 滚动事件时间窗口：固定 size、不重叠。每记录落入单个窗口 [ts - ts%size, +size)。 */
public class TumblingEventTimeWindows<T> extends WindowAssigner<T, TimeWindow> {
    private final long sizeMillis;

    public TumblingEventTimeWindows(long sizeMillis) {
        this.sizeMillis = sizeMillis;
    }

    public static <T> TumblingEventTimeWindows<T> of(Duration size) {
        return new TumblingEventTimeWindows<>(size.toMillis());
    }

    @Override
    public Collection<TimeWindow> assignWindows(T record, long timestamp) {
        long start = timestamp - (timestamp % sizeMillis);
        return List.of(new TimeWindow(start, start + sizeMillis));
    }

    @Override
    public boolean isEventTime() {
        return true;
    }
}
```

- [ ] **Step 7: 创建 `SlidingEventTimeWindows`**

```java
package org.miniflink.window;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * 滑动事件时间窗口：size 大小、slide 步长。一记录落入 size/slide 个连续窗口。
 * 窗口 [start, start+size)，start 对齐 slide，且 start <= ts < start+size。
 */
public class SlidingEventTimeWindows<T> extends WindowAssigner<T, TimeWindow> {
    private final long sizeMillis;
    private final long slideMillis;

    public SlidingEventTimeWindows(long sizeMillis, long slideMillis) {
        this.sizeMillis = sizeMillis;
        this.slideMillis = slideMillis;
    }

    public static <T> SlidingEventTimeWindows<T> of(Duration size, Duration slide) {
        return new SlidingEventTimeWindows<>(size.toMillis(), slide.toMillis());
    }

    @Override
    public Collection<TimeWindow> assignWindows(T record, long timestamp) {
        List<TimeWindow> windows = new ArrayList<>();
        long lastStart = timestamp - timestamp % slideMillis;
        // 从 lastStart 向前，每 slide 一个窗口，共 size/slide 个，满足 start <= ts < start+size
        int count = (int) (sizeMillis / slideMillis);
        for (int i = 0; i < count; i++) {
            long start = lastStart - i * slideMillis;
            if (start <= timestamp && timestamp < start + sizeMillis) {
                windows.add(new TimeWindow(start, start + sizeMillis));
            }
        }
        return windows;
    }

    @Override
    public boolean isEventTime() {
        return true;
    }
}
```

- [ ] **Step 8: 运行测试验证通过**

Run: `mvn -q -Dtest=WindowAssignerTest test`
Expected: `Tests run: 3, Failures: 0`。

- [ ] **Step 9: 提交**

```bash
git add src/main/java/org/miniflink/window/ \
        src/test/java/org/miniflink/window/WindowAssignerTest.java
git commit -m "feat(window): Window/TimeWindow + Tumbling/Sliding WindowAssigner

Co-Authored-By: Claude <noreply@anthropic.com>"
```

## Task 6: Trigger + TriggerResult + TriggerContext + EventTimeTrigger

> 触发器：onElement 注册 window-end timer，onEventTime(end) 返回 FIRE_AND_PURGE。

**Files:**
- Create: `src/main/java/org/miniflink/window/TriggerResult.java`
- Create: `src/main/java/org/miniflink/window/TriggerContext.java`
- Create: `src/main/java/org/miniflink/window/Trigger.java`
- Create: `src/main/java/org/miniflink/window/EventTimeTrigger.java`
- Test: `src/test/java/org/miniflink/window/EventTimeTriggerTest.java`

**Interfaces:**
- Consumes: `TimerService`（Task 4，经 TriggerContext）。
- Produces: `TriggerResult.{CONTINUE, FIRE_AND_PURGE}`；`TriggerContext`；`Trigger.onElement/onEventTime`；`EventTimeTrigger`。

- [ ] **Step 1: 写失败测试 `EventTimeTriggerTest`**

```java
package org.miniflink.window;

import org.junit.jupiter.api.Test;
import java.util.ArrayList;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

class EventTimeTriggerTest {

    /** 记录 TriggerContext 注册的 timer。 */
    static class CapturingContext implements TriggerContext {
        final List<Long> registered = new ArrayList<>();
        long wm = Long.MIN_VALUE;
        @Override public long getCurrentWatermark() { return wm; }
        @Override public void registerEventTimeTimer(long time) { registered.add(time); }
        @Override public void deleteEventTimeTimer(long time) { }
    }

    @Test
    void onElement注册windowEnd的timer并返回CONTINUE() throws Exception {
        EventTimeTrigger<String, TimeWindow> trigger = EventTimeTrigger.create();
        CapturingContext ctx = new CapturingContext();
        TimeWindow window = new TimeWindow(1000, 2000);
        assertEquals(TriggerResult.CONTINUE, trigger.onElement("x", 1500, window, ctx));
        assertEquals(List.of(2000L), ctx.registered);   // 注册 window.end
    }

    @Test
    void onEventTime在windowEnd触发FIRE_AND_PURGE() throws Exception {
        EventTimeTrigger<String, TimeWindow> trigger = EventTimeTrigger.create();
        CapturingContext ctx = new CapturingContext();
        TimeWindow window = new TimeWindow(1000, 2000);
        assertEquals(TriggerResult.FIRE_AND_PURGE, trigger.onEventTime(2000, window, ctx));
        assertEquals(TriggerResult.CONTINUE, trigger.onEventTime(1500, window, ctx));  // 非 end
    }
}
```

- [ ] **Step 2: 运行测试验证失败**

Run: `mvn -q -Dtest=EventTimeTriggerTest test`
Expected: 编译失败。

- [ ] **Step 3: 创建 `TriggerResult`**

```java
package org.miniflink.window;

/** 触发结果：CONTINUE（继续等待）、FIRE_AND_PURGE（触发计算并清理窗口状态）。 */
public enum TriggerResult {
    CONTINUE,
    FIRE_AND_PURGE
}
```

- [ ] **Step 4: 创建 `TriggerContext`**

```java
package org.miniflink.window;

/** Trigger 上下文：提供当前 watermark + 定时器注册（委托 TimerService）。 */
public interface TriggerContext {
    long getCurrentWatermark();
    void registerEventTimeTimer(long time);
    void deleteEventTimeTimer(long time);
}
```

- [ ] **Step 5: 创建 `Trigger`（abstract）**

```java
package org.miniflink.window;

/** 窗口触发器：决定窗口何时触发计算。 */
public abstract class Trigger<T, W extends Window> {
    public abstract TriggerResult onElement(T element, long timestamp, W window, TriggerContext ctx) throws Exception;
    public abstract TriggerResult onEventTime(long time, W window, TriggerContext ctx) throws Exception;
}
```

- [ ] **Step 6: 创建 `EventTimeTrigger`**

```java
package org.miniflink.window;

/**
 * 事件时间触发器：onElement 注册 window.end timer（首次）；onEventTime(end) 触发 FIRE_AND_PURGE。
 * TimerService 去重保证重复注册同一 end 安全。
 */
public class EventTimeTrigger<T, W extends Window> extends Trigger<T, W> {

    @SuppressWarnings("rawtypes")
    private static final EventTimeTrigger INSTANCE = new EventTimeTrigger();

    @SuppressWarnings("unchecked")
    public static <T, W extends Window> EventTimeTrigger<T, W> create() {
        return (EventTimeTrigger<T, W>) INSTANCE;
    }

    @Override
    public TriggerResult onElement(T element, long timestamp, W window, TriggerContext ctx) {
        ctx.registerEventTimeTimer(window.end());
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onEventTime(long time, W window, TriggerContext ctx) {
        if (time == window.end()) {
            return TriggerResult.FIRE_AND_PURGE;
        }
        return TriggerResult.CONTINUE;
    }
}
```

- [ ] **Step 7: 运行测试验证通过**

Run: `mvn -q -Dtest=EventTimeTriggerTest test`
Expected: `Tests run: 2, Failures: 0`。

- [ ] **Step 8: 提交**

```bash
git add src/main/java/org/miniflink/window/TriggerResult.java \
        src/main/java/org/miniflink/window/TriggerContext.java \
        src/main/java/org/miniflink/window/Trigger.java \
        src/main/java/org/miniflink/window/EventTimeTrigger.java \
        src/test/java/org/miniflink/window/EventTimeTriggerTest.java
git commit -m "feat(window): Trigger/TriggerResult/TriggerContext + EventTimeTrigger

Co-Authored-By: Claude <noreply@anthropic.com>"
```

## Task 7: WindowOperator

> 窗口聚合算子：per-key per-window `MapState<TimeWindow, IN>` 增量 reduce；活跃窗口注册表（end → (key,window)）；onWatermark 推进 TimerService 触发 window-end → 输出最终值 + 清理。

**Files:**
- Create: `src/main/java/org/miniflink/runtime/operator/WindowOperator.java`
- Test: `src/test/java/org/miniflink/runtime/operator/WindowOperatorTest.java`

**Interfaces:**
- Consumes: `Operator.open(Collector, RuntimeContext)`（Task 1-2）、`RuntimeContext.getCurrentTimestamp/setCurrentKey/getStateBackend/getKeySelector`（阶段③/Task1）、`MapState`（阶段③）、`TimerService/InternalTimerService`（Task 4）、`WindowAssigner`（Task 5）、`Trigger/EventTimeTrigger`（Task 6）、`ReduceFunction`（阶段③）、`Watermark`（Task 2）。
- Produces: `WindowOperator<IN>(WindowAssigner<IN, TimeWindow>, ReduceFunction<IN>)`。

- [ ] **Step 1: 写失败测试 `WindowOperatorTest`**

```java
package org.miniflink.runtime.operator;

import org.junit.jupiter.api.Test;
import org.miniflink.api.function.ReduceFunction;
import org.miniflink.runtime.ListCollector;
import org.miniflink.runtime.RuntimeContextImpl;
import org.miniflink.runtime.Watermark;
import org.miniflink.window.TumblingEventTimeWindows;

import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

class WindowOperatorTest {

    /** 记录带 ts：(value, ts)。 */
    record TE(int value, long ts) { }

    @Test
    void 窗口结束输出最终累加值一次() throws Exception {
        // keySelector: 按 value 分组；reduce: 求和；窗口 1s
        WindowOperator<TE> op = new WindowOperator<>(
                TumblingEventTimeWindows.<TE>of(java.time.Duration.ofSeconds(1)),
                (ReduceFunction<TE>) (a, b) -> new TE(a.value, a.ts));
        // 简化：reduce 取 a（同 key 同窗口只保留第一条）—— 改用求和验证累加
        op = new WindowOperator<>(
                TumblingEventTimeWindows.<TE>of(java.time.Duration.ofSeconds(1)),
                (ReduceFunction<TE>) (a, b) -> new TE(a.value + b.value, b.ts));
        RuntimeContextImpl ctx = new RuntimeContextImpl(0, 1, (t) -> t.value);  // keySelector: TE -> value
        ListCollector<TE> out = new ListCollector<>();
        op.open(out, ctx);

        // 三条 ts 在 [1000,2000) 窗口，key 都为 value 分组？这里 keySelector=t.value，value 不同则不同 key
        // 为简单：用同 key（value 相同）同窗口累加
        op.processElement(new TE(5, 1500));   // key=5, 窗口[1000,2000), acc=5
        op.processElement(new TE(5, 1700));   // key=5, acc=5+5=10
        assertTrue(out.getResult().isEmpty());  // 窗口未触发，无输出

        op.onWatermark(new Watermark(2000));   // watermark 到 window.end → 触发
        assertEquals(List.of(new TE(10, 1700)), out.getResult());
    }

    @Test
    void 不同key的窗口各自累加() throws Exception {
        WindowOperator<TE> op = new WindowOperator<>(
                TumblingEventTimeWindows.<TE>of(java.time.Duration.ofSeconds(1)),
                (ReduceFunction<TE>) (a, b) -> new TE(a.value + b.value, b.ts));
        RuntimeContextImpl ctx = new RuntimeContextImpl(0, 1, (t) -> t.value);
        ListCollector<TE> out = new ListCollector<>();
        op.open(out, ctx);

        op.processElement(new TE(1, 1500));   // key=1
        op.processElement(new TE(2, 1500));   // key=2
        op.processElement(new TE(1, 1600));   // key=1, acc=2
        op.onWatermark(new Watermark(2000));
        // 两个 key 各输出：key=1 → (2,1600)，key=2 → (2,1500)
        assertEquals(2, out.getResult().size());
        assertTrue(out.getResult().contains(new TE(2, 1600)));
        assertTrue(out.getResult().contains(new TE(2, 1500)));
    }
}
```

- [ ] **Step 2: 运行测试验证失败**

Run: `mvn -q -Dtest=WindowOperatorTest test`
Expected: 编译失败 —— `WindowOperator` 不存在。

- [ ] **Step 3: 创建 `WindowOperator`**

```java
package org.miniflink.runtime.operator;

import org.miniflink.api.function.KeySelector;
import org.miniflink.api.function.ReduceFunction;
import org.miniflink.runtime.Collector;
import org.miniflink.runtime.Operator;
import org.miniflink.runtime.RuntimeContext;
import org.miniflink.runtime.Watermark;
import org.miniflink.time.InternalTimerService;
import org.miniflink.time.TimerHandler;
import org.miniflink.window.EventTimeTrigger;
import org.miniflink.window.TimeWindow;
import org.miniflink.window.TriggerContext;
import org.miniflink.window.TriggerResult;
import org.miniflink.window.WindowAssigner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 窗口聚合算子：per-key per-window MapState 增量 reduce；watermark 推进触发 window-end 输出最终值 + 清理。
 * 活跃窗口注册表：end -> [(key, window)]，按 end 直达待触发窗口，避免遍历所有 key。
 */
public class WindowOperator<IN> implements Operator<IN, IN>, TimerHandler {

    /** (key, window) 对，作注册表条目。 */
    private record KeyedWindow(Object key, TimeWindow window) { }

    private final WindowAssigner<IN, TimeWindow> windowAssigner;
    private final ReduceFunction<IN> reduceFn;
    private Collector<IN> out;
    private RuntimeContext ctx;
    @SuppressWarnings("rawtypes")
    private org.miniflink.runtime.MapState state;           // MapState<TimeWindow, IN>，按 currentKey 寻址
    private KeySelector<IN, ?> keySelector;
    private final InternalTimerService timerService = new InternalTimerService();
    /** end -> 该 end 下所有 (key, window)（watermark 到 end 时全部触发）。 */
    private final Map<Long, List<KeyedWindow>> activeWindows = new HashMap<>();

    public WindowOperator(WindowAssigner<IN, TimeWindow> windowAssigner, ReduceFunction<IN> reduceFn) {
        this.windowAssigner = windowAssigner;
        this.reduceFn = reduceFn;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Override
    public void open(Collector<IN> out, RuntimeContext ctx) {
        this.out = out;
        this.ctx = ctx;
        this.state = ctx.getStateBackend().getMapState("window-accs");
        this.keySelector = (KeySelector<IN, ?>) ctx.getKeySelector();
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Override
    public void processElement(IN record) throws Exception {
        long ts = ctx.getCurrentTimestamp();
        Object key = keySelector.getKey(record);
        ctx.setCurrentKey(key);
        for (TimeWindow window : windowAssigner.assignWindows(record, ts)) {
            IN acc = (IN) state.get(window);
            IN reduced = (acc == null) ? record : reduceFn.reduce(acc, record);
            state.put(window, reduced);

            // 注册窗口：若该 (key, window) 首次出现，加入注册表 + 注册 end timer
            if (!isRegistered(key, window)) {
                activeWindows.computeIfAbsent(window.end(), k -> new ArrayList<>())
                        .add(new KeyedWindow(key, window));
                timerService.registerEventTimeTimer(window.end());   // EventTimeTrigger 等价：注册 end
            }
        }
    }

    @Override
    public void onWatermark(Watermark watermark) {
        // 推进时钟：触发所有 end <= watermark 的窗口
        timerService.advanceTo(watermark.getTimestamp(), this);
    }

    /** TimerService 回调：某 end 到点，触发该 end 下所有 (key, window)。 */
    @Override
    public void onEventTime(long time) {
        List<KeyedWindow> toFire = activeWindows.remove(time);
        if (toFire == null) {
            return;
        }
        for (KeyedWindow kw : toFire) {
            ctx.setCurrentKey(kw.key());
            @SuppressWarnings("unchecked")
            IN acc = (IN) state.get(kw.window());
            if (acc != null) {
                out.collect(acc);       // 输出窗口最终值
            }
            state.put(kw.window(), null);  // 清理（MapState 无 remove，用 put null）
        }
    }

    private boolean isRegistered(Object key, TimeWindow window) {
        List<KeyedWindow> list = activeWindows.get(window.end());
        if (list == null) {
            return false;
        }
        for (KeyedWindow kw : list) {
            if (kw.key().equals(key) && kw.window().equals(window)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void close() { /* 无操作 */ }

    @Override
    public WindowOperator<IN> copy() {
        return new WindowOperator<>(windowAssigner, reduceFn);   // 共享无状态 assigner/reduceFn
    }
}
```

> **注**：`MapState` 无 `remove`，清理用 `put(window, null)`。`onEventTime` 是 `TimerHandler` 回调（Task 4）。`MapState.get(null-key)`：state 经 ctx.setCurrentKey 寻址，put null 后 get 返回 null（清理生效）。

- [ ] **Step 4: 运行测试验证通过**

Run: `mvn -q -Dtest=WindowOperatorTest test`
Expected: `Tests run: 2, Failures: 0`。

- [ ] **Step 5: 提交**

```bash
git add src/main/java/org/miniflink/runtime/operator/WindowOperator.java \
        src/test/java/org/miniflink/runtime/operator/WindowOperatorTest.java
git commit -m "feat(runtime): WindowOperator（per-key per-window MapState + watermark 触发）

Co-Authored-By: Claude <noreply@anthropic.com>"
```

## Task 8: WindowedStream + KeyedStream.window + reduce API

> API 层：`KeyedStream.window(WindowAssigner)` 返回 `WindowedStream`，其 `reduce` 建 `WindowOperator` transformation。

**Files:**
- Create: `src/main/java/org/miniflink/api/WindowedStream.java`
- Modify: `src/main/java/org/miniflink/api/KeyedStream.java`
- Test: `src/test/java/org/miniflink/api/WindowedStreamTest.java`

**Interfaces:**
- Consumes: `WindowOperator`（Task 7）、`OneInputTransformation`/`HashPartitioner`（阶段②/③）、`WindowAssigner`（Task 5）、`ReduceFunction`（阶段③）。
- Produces: `WindowedStream<T,W>.reduce(ReduceFunction):DataStream<T>`；`KeyedStream<T,K>.window(WindowAssigner):WindowedStream<T,W>`。

- [ ] **Step 1: 写失败测试 `WindowedStreamTest`**

```java
package org.miniflink.api;

import org.junit.jupiter.api.Test;
import org.miniflink.api.function.KeySelector;
import org.miniflink.api.function.ReduceFunction;
import org.miniflink.execution.HashPartitioner;
import org.miniflink.graph.OneInputTransformation;
import org.miniflink.runtime.operator.WindowOperator;
import org.miniflink.window.TumblingEventTimeWindows;

import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

class WindowedStreamTest {

    @Test
    void window后reduce建WindowOperator的hash分区transformation() {
        StreamExecutionEnvironment env = new StreamExecutionEnvironment();
        DataStream<Integer> reduced = env.fromCollection(List.of(1, 2, 3))
                .keyBy((KeySelector<Integer, Integer>) x -> x)
                .window(TumblingEventTimeWindows.of(java.time.Duration.ofSeconds(1)))
                .reduce((ReduceFunction<Integer>) (a, b) -> a + b);
        OneInputTransformation<?, ?> tx = (OneInputTransformation<?, ?>) reduced.getTransformation();
        assertEquals("window-reduce", tx.getName());
        assertInstanceOf(HashPartitioner.class, tx.getPartitioner());
        assertNotNull(tx.getKeySelector());
        assertInstanceOf(WindowOperator.class, tx.getOperator());
    }
}
```

- [ ] **Step 2: 运行测试验证失败**

Run: `mvn -q -Dtest=WindowedStreamTest test`
Expected: 编译失败 —— `KeyedStream.window` 不存在。

- [ ] **Step 3: 创建 `WindowedStream`**

```java
package org.miniflink.api;

import org.miniflink.api.function.ReduceFunction;
import org.miniflink.execution.HashPartitioner;
import org.miniflink.runtime.operator.WindowOperator;
import org.miniflink.window.Window;
import org.miniflink.window.WindowAssigner;

/**
 * keyBy + window 返回的流：提供窗口聚合操作。
 * reduce 建一个 hash 分区的 WindowOperator transformation（沿用 KeyedStream 的 keySelector）。
 */
public class WindowedStream<T, W extends Window> {
    private final KeyedStream<T, ?> input;
    private final WindowAssigner<T, W> windowAssigner;

    public WindowedStream(KeyedStream<T, ?> input, WindowAssigner<T, W> windowAssigner) {
        this.input = input;
        this.windowAssigner = windowAssigner;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public DataStream<T> reduce(ReduceFunction<T> reduceFn) {
        WindowOperator<T> op = new WindowOperator<>((WindowAssigner) windowAssigner, reduceFn);
        return input.keyedTransformFor("window-reduce", op);
    }
}
```

- [ ] **Step 4: 修改 `KeyedStream` 加 `window` + 复用 hash 分区建 transformation**

`KeyedStream` 加方法：
```java
    private final org.miniflink.execution.HashPartitioner hashPartitioner = new org.miniflink.execution.HashPartitioner();

    /** 按 windowAssigner 开窗，返回 WindowedStream。 */
    public <W extends org.miniflink.window.Window> WindowedStream<T, W> window(
            org.miniflink.window.WindowAssigner<T, W> windowAssigner) {
        return new WindowedStream<>(this, windowAssigner);
    }

    /** 供 WindowedStream 使用：建一个 hash 分区（沿用本 KeyedStream 的 keySelector）的 transformation。 */
    <O> DataStream<O> keyedTransformFor(String name, org.miniflink.runtime.Operator<T, O> operator) {
        return dataStream.keyedTransform(name, operator, hashPartitioner, keySelector);
    }
```
（`keyedTransform` 是阶段③ DataStream 的 package-private 方法：`<O> DataStream<O> keyedTransform(String, Operator<T,O>, Partitioner, KeySelector<T,?>)`。）

- [ ] **Step 5: 运行测试验证通过 + 全量回归**

Run: `mvn -q -Dtest=WindowedStreamTest test`
Expected: `Tests run: 1, Failures: 0`。

Run: `mvn -q test`
Expected: 全量通过（无回归）。

- [ ] **Step 6: 提交**

```bash
git add src/main/java/org/miniflink/api/WindowedStream.java \
        src/main/java/org/miniflink/api/KeyedStream.java \
        src/test/java/org/miniflink/api/WindowedStreamTest.java
git commit -m "feat(api): WindowedStream + KeyedStream.window + 窗口 reduce

Co-Authored-By: Claude <noreply@anthropic.com>"
```

## Task 9: 端到端验收（滚动窗口聚合 + 滑动窗口示例）+ 文档

**Files:**
- Test: `src/test/java/org/miniflink/examples/WindowExampleTest.java`
- Create: `docs/examples/window.md`

**Interfaces:** Consumes 全部阶段④ API（Task 1-8）。

- [ ] **Step 1: 写验收测试 `WindowExampleTest`（滚动窗口端到端）**

```java
package org.miniflink.examples;

import org.junit.jupiter.api.Test;
import org.miniflink.api.StreamExecutionEnvironment;
import org.miniflink.api.function.KeySelector;
import org.miniflink.api.function.ReduceFunction;
import org.miniflink.connector.CollectSink;
import org.miniflink.window.TumblingEventTimeWindows;

import java.time.Duration;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

/**
 * 阶段④验收：滚动窗口端到端聚合。
 * source → assignTimestampsAndWatermarks → keyBy → window(1s) → reduce(求和) → sink。
 * 注入 watermark 推进时间，验证每窗口输出最终累加值一次。
 */
class WindowExampleTest {

    record Event(String key, int value, long ts) { }

    @Test
    void 滚动窗口按key累加并窗口结束输出() throws Exception {
        StreamExecutionEnvironment env = new StreamExecutionEnvironment();
        CollectSink<Event> sink = new CollectSink<>();

        env.fromCollection(List.of(
                new Event("a", 1, 100),    // 窗口 [0,1000)
                new Event("a", 2, 200),
                new Event("a", 10, 1100),  // 窗口 [1000,2000)
                new Event("a", 20, 1200)))
           .assignTimestampsAndWatermarks(
                   org.miniflink.time.WatermarkStrategy
                           .<Event>forBoundedOutOfOrderness(Duration.ofMillis(0), e -> e.ts))
           .keyBy((KeySelector<Event, String>) e -> e.key)
           .window(TumblingEventTimeWindows.of(Duration.ofSeconds(1)))
           .reduce((ReduceFunction<Event>) (a, b) -> new Event(a.key, a.value + b.value, b.ts))
           .addSink(sink::add);

        env.execute("window-example");

        // 窗口 [0,1000) → a: 1+2=3；窗口 [1000,2000) → a: 10+20=30
        List<Event> results = sink.getResults();
        assertEquals(2, results.size());
        assertTrue(results.contains(new Event("a", 3, 200)));
        assertTrue(results.contains(new Event("a", 30, 1200)));
    }
}
```

- [ ] **Step 2: 运行验收测试**

Run: `mvn -q -Dtest=WindowExampleTest test`
Expected: `Tests run: 1, Failures: 0`。若失败，详查 watermark 流动 / 窗口触发（Task 1-7 实现）。

- [ ] **Step 3: 创建示例文档 `docs/examples/window.md`**

````markdown
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
````

- [ ] **Step 4: 全量测试最终回归**

Run: `mvn -q test`
Expected: `BUILD SUCCESS`，全量通过（阶段①②③④）。

- [ ] **Step 5: 提交**

```bash
git add src/test/java/org/miniflink/examples/WindowExampleTest.java \
        docs/examples/window.md
git commit -m "docs(examples): 添加阶段④窗口聚合验收示例（event time + watermark）

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

## Self-Review

### 1. Spec 覆盖（对照 `2026-07-11-mini-flink-stage4-window-design.md`）

| spec 节 | 覆盖任务 |
|---|---|
| StreamElement 扩展（Watermark + Record ts） | Task 1（ts）+ Task 2（Watermark） |
| watermark 传播（OperatorTask/Output/SourceTask） | Task 2 |
| WatermarkStrategy + TimestampAssigner + TS&WM 算子 + assignTimestampsAndWatermarks | Task 3 |
| TimerService + InternalTimerService | Task 4 |
| Window + TimeWindow + WindowAssigner（Tumbling/Sliding） | Task 5 |
| Trigger + TriggerResult + TriggerContext + EventTimeTrigger | Task 6 |
| WindowOperator（per-key per-window MapState + 活跃窗口注册表 + watermark 触发） | Task 7 |
| WindowedStream + KeyedStream.window + reduce | Task 8 |
| 端到端验收（滚动 + 滑动） | Task 9（滚动；滑动由 Task 5 单测覆盖 assignWindows，端到端可后续） |
| 边界（迟到 drop、source 发 +∞） | Task 2（source +∞）；迟到 drop 由 WindowOperator 自然实现（窗口已清理后 state 为 null，onEventTime acc==null 不输出） |

**缺口**：滑动窗口的端到端验收（Task 9 仅滚动）——Task 5 已单测 assignWindows 滑动多窗口，端到端滑动可后续补。spec 验收"滑动窗口示例"建议 Task 9 加一个滑动端到端测试，但为控制 Task 9 规模，标记为可选（final review 决定）。

### 2. Placeholder 扫描

无 TBD/TODO。Task 3 的 watermark 发送边界（算子只持 `Collector` 无法直接发 `Watermark`，故经 `RuntimeContext.emitWatermark`，由 `OperatorTask` 注入广播 emitter）已在 Step 6-8 明确给出最终实现。

### 3. 类型一致性

跨任务签名一致：`Record<T>(value, timestamp)`、`RuntimeContext.getCurrentTimestamp/setCurrentTimestamp/emitWatermark`、`Watermark.getTimestamp()`、`Operator.onWatermark`、`Output.route(value, timestamp, upstreamIndex)`/`sendWatermark`、`OutputCollector(outputs, ctx)`、`WatermarkStrategy.forBoundedOutOfOrderness`、`TimerService/InternalTimerService.advanceTo(wm, handler)`、`TimeWindow(start,end)`、`WindowAssigner.assignWindows(record, ts)`、`Trigger.onElement/onEventTime`、`WindowOperator(assigner, reduceFn)`、`WindowedStream.reduce`、`KeyedStream.window`。

### 已知简化（Self-Review 接受）

- `RuntimeContext.emitWatermark` 经 setter 注入 emitter（避免改 StreamExecutor/RuntimeContextImpl 构造签名）——边界权衡，Collector 仍不变。
- `MapState` 无 remove，窗口清理用 `put(window, null)`（阶段③ MapState 接口未含 remove）。
- 滑动窗口端到端验收留可选（Task 5 单测已覆盖 assignWindows）。
- `TimestampsAndWatermarksOperator` 每条记录后发 watermark（punctuated-ish，但 watermark 单调）——简化，非 Flink 周期性。
- Task 3 的设计推导过程较长（watermark 发送边界），因这是阶段④最微妙的边界决策，保留推导助 implementer 理解。
