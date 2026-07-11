# Mini-Flink 阶段⑤（容错）设计

- **日期**：2026-07-11
- **状态**：已通过 brainstorming（6 节设计逐节确认），待编写实现计划
- **关系**：细化 [整体设计](2026-07-10-mini-flink-design.md) 的阶段⑤部分（spec §10 阶段⑤）。前置阶段①②③④已合并 `main`（HEAD `5ce4358`，75 测试全绿）。

## 1. 背景与目标

阶段①（骨架）+②（多线程并行）+③（keyed state）+④（时间/窗口）已完成在 `main`：四层分层、多线程 Task、有界 Channel 反压、算子链、三分区器、EOB 引用计数关闭、keyed state（StateBackend/三种 state/RuntimeContext）、KeyedStream/ReduceOperator、event time + watermark + TimerService + 滚动/滑动窗口 + WindowOperator。

阶段⑤引入**容错核心**：周期 checkpoint → 一致快照 → task 故障自动检测 → 干净关闭 → 从最近 checkpoint 重启恢复（exactly-once）。这是 Flink 区别于普通流处理的另一核心机制（与④的时间/窗口并列）。

### 头号前置（失败关闭缺口，阶段② final review flag）

单 Task 抛异常时不广播 EOB → 下游 `Channel.receive()` 永久阻塞 + 上游有界 Channel 满 → `StreamExecutor.join()` 无超时 → `env.execute()` 永久挂起（而非干净失败）。阶段⑤一并修复：**任一 Task 异常 → 中断所有未结束 Task → 干净停止**。这是 failover 的最小底座。

### 设计原则（延续）

- **对照学习**：贴近真实 Flink（Chandy-Lamport barrier 对齐、`CheckpointCoordinator`、`InputGate`/`InputChannel`、`snapshot/restore`）。
- **稳定边界**：`Collector` 接口不变；`Operator` 只加 `default` 方法。
- **YAGNI**：savepoint、增量 checkpoint、region failover、unaligned checkpoint 推迟。

## 2. 范围

### 包含

- **Barrier 流动 + InputGate per-upstream 对齐**（Chandy-Lamport）：barrier 随数据流异步流动，下游 per-upstream 缓冲对齐。
- **失败关闭缺口**（A1）：task 异常 → 中断全部 → 干净停止。
- **CheckpointCoordinator**：独立 daemon 线程按 interval 周期触发。
- **状态快照**：keyed state（StateBackend）+ source offset + window 算子 timer/活跃窗口。
- **自动 failover**：故障检测 → 干净关闭 → 从最近 checkpoint 重启（可重放 source 从 offset 重放 + backend/算子状态恢复）。
- **多并行作业 checkpoint**（引入 InputGate）。

### 不包含（推迟）

- savepoint（手动触发、跨版本兼容）。
- 增量 checkpoint（仅全量快照）。
- Fine-grained / region recovery（仅全作业重启）。
- unaligned checkpoint。
- 多并行度 watermark min 对齐、会话窗口、allowedLateness、MapState remove（推迟为独立小阶段）。

## 3. 已确认的关键决策

| 决策 | 选择 | 备注 |
|---|---|---|
| 范围 | **仅容错核心** | deferred 项（watermark 对齐/会话窗口/allowedLateness/MapState remove）推迟独立小阶段 |
| 恢复形态 | **自动 failover** | 完整闭环：检测→干净关闭→从 checkpoint 重启 |
| checkpoint 触发 | **周期 coordinator** | 独立 daemon 线程按 interval 注入 barrier |
| 多并行 checkpoint | **支持（引入 InputGate）** | 改造 Channel 模型为 per-(上游,下游) pair；含阶段②全量回归 |
| timer 持久化 | **持久化（window 作业 failover 完整）** | WindowOperator timers + activeWindows 经算子级快照钩子持久化 |
| 分解 | **一个 spec 两 phase** | Phase 1 barrier 基础设施；Phase 2 checkpoint/恢复 |

## 4. 核心抽象

### 4.1 Barrier（runtime）

第 4 种 `StreamElement`，经 Channel 流动，语义类似 Watermark 但触发对齐+快照。

```java
public final class Barrier implements StreamElement {
    private final long checkpointId;
    public Barrier(long checkpointId) { this.checkpointId = checkpointId; }
    public long getCheckpointId() { return checkpointId; }
    // equals/hashCode by checkpointId
}
```

`Output.sendBarrier(Barrier b)`：向所有下游 channel 广播（不分区，语义同 `sendWatermark`）。

### 4.2 InputGate / InputChannel + 对齐算法（runtime）

替换下游的「单 Channel fan-in」。当前 `StreamExecutor` 的 `inputChannelOf` 是 per-target-vertex 一个 Channel，多上游数据混杂，无法区分上游 → 做不了正确 per-upstream 对齐。

**模型**：
- `InputChannel`：包装一个物理 `Channel` + 对齐状态（是否已收到当前 barrier）+ 缓冲队列（barrier 后该上游的 record 暂存）。
- `InputGate`：下游 subtask 的输入聚合，含 N 个 `InputChannel`（每个绑定一个上游 subtask）。forward 退化为一 InputChannel。
- `InputGate.receive()`：返回下一个该由 task 处理的 `StreamElement`（Record/Watermark/EOB），**Barrier 被 InputGate 内部对齐消费，不返回给 task**。

**对齐算法（Chandy-Lamport，封装在 InputGate）**：

```
某 InputChannel 收到 Barrier(id):
  标记该 channel 「已对齐 id」
  该 channel 后续 Record 缓冲到 channel 内部队列（不交给 task）
  当所有 InputChannel 都已对齐 id:
    回调 task 的 snapshot(id)（task 做快照）
    快照完成后：向所有 output 广播 Barrier(id) + 解除所有 channel 缓冲（缓冲 Record 按序重新放出）
未对齐 channel 的 Record 正常放行 → task 继续处理（不阻塞整个 task）
```

单上游（forward）：barrier 到 → 立即对齐 → 快照 → 广播，零缓冲。

**关键封装**：对齐逻辑全在 InputGate 内，`OperatorTask` 主循环不感知 barrier——它只看到 InputGate 过滤后的 Record/Watermark/EOB；对齐完成时 InputGate 回调 task 快照并自动转发 barrier。Phase 1 的 snapshot 回调先用占位接口（如 `SnapshotCallback.onAligned(long id)`），Phase 2 接真快照实现。

### 4.3 状态快照（StateBackend 扩展）

```java
public interface StateBackend {
    // 阶段③既有
    <T> ValueState<T> getValueState(String name);
    <T> ListState<T> getListState(String name);
    <K, V> MapState<K, V> getMapState(String name);
    void setCurrentKey(Object key);
    // 阶段⑤新增
    StateSnapshot snapshot();
    void restore(StateSnapshot snapshot);
}

public class StateSnapshot implements Serializable {
    private final Map<String, Map<Object, Object>> valueStore;
    private final Map<String, Map<Object, List<Object>>> listStore;
    private final Map<String, Map<Object, Map<Object, Object>>> mapStore;
    // 构造 / getter；深拷贝语义
}
```

`MemoryStateBackend.snapshot()`：深拷贝三类 store 生成 `StateSnapshot`。`restore(s)`：替换内部三类 store + 重置 `currentKey`。

### 4.4 source offset（exactly-once 关键，runtime）

不持久化则恢复重放会重复累加。`CollectionSource` 当前用 `idx % parallelism == subtask` 遍历 `Iterable` 分片，不持位置。

改造：
- `env.fromCollection` 内部转 `ArrayList`（保证可重复遍历）；`CollectionSource.data` 改持 `List`。
- `SourceContextImpl` 加 `long emitted`（已转发数）+ `long skipUntil`（恢复时跳过前 N 条，初值 0）：
  - `collect(r)`：`emitted < skipUntil` → `emitted++` 丢弃；否则 `emitted++` 转发。
  - `snapshotOffset()` 返回 `emitted`；`restoreOffset(long n)` 设 `skipUntil = n`、`emitted = 0`。
- `CollectionSource.run` 逻辑不变；重放时 SourceContext 自动跳过已发条数 → 每个 subtask 从断点继续。**CollectionSource 零逻辑改动**（仅 data 字段类型 Iterable→List）。

### 4.5 算子级快照钩子（Operator 扩展）

因选 timer 持久化（决策 B），WindowOperator 的 `InternalTimerService`（timers `Set<Long>`）+ `activeWindows`（`Map<Long, List<KeyedWindow>>`）是算子内部状态，不在 keyed StateBackend 内，需独立持久化。

```java
public interface Operator<IN, OUT> {
    // 阶段②③既有（含 ④ 的 onWatermark default）
    // 阶段⑤新增 default
    default Optional<OperatorState> snapshotState() { return Optional.empty(); }
    default void restoreState(OperatorState state) { }
}

public interface OperatorState extends Serializable { }   // 标记接口

// WindowOperator 的快照
public class WindowOperatorState implements OperatorState {
    private final List<Long> pendingTimers;                       // TreeSet<Long> 展平
    private final List<KeyedWindow> registeredWindows;            // activeWindows 展平：(key, TimeWindow)
}
```

- `WindowOperator.snapshotState()`：收集 timerService 的 timers + activeWindows 全部 (key,window) 对。
- `WindowOperator.restoreState(s)`：重建 `InternalTimerService`（重灌 timers）+ activeWindows 注册表（重灌）。恢复后未触发窗口能继续在 watermark 推进时触发输出。
- `OperatorChain.snapshotState()`：收集链内各算子状态（按算子索引）→ `Map<Integer, OperatorState>`；`restoreState(Map)`：按索引恢复。

### 4.6 SubtaskSnapshot + Checkpoint

```java
public class SubtaskSnapshot implements Serializable {
    private final StateSnapshot keyedState;                       // backend 快照
    private final long sourceOffset;                              // 仅 source subtask，其余 -1
    private final Map<Integer, OperatorState> operatorStates;     // 算子额外状态（WindowOperator timer 等）
}

public class Checkpoint implements Serializable {
    private final long checkpointId;
    private final Map<String, SubtaskSnapshot> snapshots;         // key = vertexId + "-" + subtaskIndex
}
```

### 4.7 CheckpointCoordinator（runtime）

独立 daemon 线程，按 `interval` 周期触发：

```
每次触发：
  1. 生成 checkpointId（递增）
  2. 调所有 source subtask 的 triggerCheckpoint(id)（source 在 collect() 插队发 Barrier）
  3. 各 subtask 对齐 + 快照后 ack(id, SubtaskSnapshot)
  4. 收齐全部 subtask ack → 汇聚成 Checkpoint → 存入 retainedCheckpoints（保留最近 N 个）
  5. 任一 ack 缺失/超时 → 废弃该 checkpoint（不存）
```

- ack 汇聚线程安全（`ConcurrentHashMap` + 计数）。
- `lastCompletedCheckpoint()`：返回 retainedCheckpoints 最近一个（failover 用）。
- `stop()`：作业结束/失败时停止 daemon（interrupt）。

**Barrier 注入**：`Coordinator` 持 source task 引用，调 `SourceTask.triggerCheckpoint(id)` → `SourceContextImpl.enqueueBarrier(id)` → `collect()` 时先发 Barrier 再发 record（保证 barrier 在 source 产出的数据流中）。

## 5. 失败关闭 + 自动 failover（StreamExecutor 改造）

当前 `execute()`：启动所有 Thread（`UncaughtExceptionHandler` 记录到 `AtomicReference<Throwable> error`）→ `join()` 全部（**无超时**）→ 抛 error。

### 5.1 失败关闭（A1）

- 任一 task 异常（`UncaughtExceptionHandler`）→ 设置失败标志 → **interrupt 所有未结束 task**。
- `Channel.put/take` 响应 `InterruptedException` → task 的 `catch (InterruptedException)` 块退出 → 解锁所有阻塞（下游 `receive()`、上游 `send()`）。
- executor join 全部退出（带超时兜底）。作业**干净停止**，不再永久挂起。

### 5.2 自动 failover 循环

`execute()` 重构为带重试循环：

```
int retries = 0;
while (retries <= maxRestarts):
    启动 tasks（首次冷启；后续从 checkpoint 恢复重建）+ 启动 Coordinator 周期 checkpoint
    等待终止条件：
      全部 task 正常结束 → Coordinator.stop(); return（成功）
      任一 task 失败   → 进入失败路径
    失败路径：
      interrupt 全部 task（干净关闭）；Coordinator.stop()
      last = Coordinator.lastCompletedCheckpoint()
      if last == null 或 retries >= maxRestarts:
          throw 失败异常（带原 cause）
      retries++
      下一轮用 last 恢复重建
```

`maxRestarts` 可配（默认 3）。持续故障源达上限即失败抛出（防无限循环）。

### 5.3 重启重建（从 Checkpoint）

每 vertex subtask：
1. 新建 `RuntimeContextImpl`。
2. `backend.restore(snapshot.keyedState)`。
3. `chain.restoreState(snapshot.operatorStates)`（WindowOperator 重建 timers + activeWindows）。
4. source subtask：`SourceContextImpl.restoreOffset(snapshot.sourceOffset)`。
5. 重新启动线程（`CollectionSource.run` 从头跑，SourceContext 跳过前 skipUntil 条 → 从断点继续）。

拓扑/ExecutionGraph 不变，仅 runtime 层重建。

## 6. 数据流

### 6.1 checkpoint 流程（正常）

```
Coordinator(daemon) --trigger(id)--> 各 source subtask
source.collect() 插队发 Barrier(id) --> 下游 Channel
下游 InputGate per-upstream 对齐 --> 全部对齐 --> 回调 task.snapshot(id)
task.snapshot: backend.snapshot + sourceOffset + chain.snapshotState --> SubtaskSnapshot
task ack(id, SubtaskSnapshot) --> Coordinator
Coordinator 收齐 --> Checkpoint --> retainedCheckpoints
InputGate 广播 Barrier(id) 到下游 + 解缓冲 --> 对齐沿 DAG 传播到 sink
```

### 6.2 failover 流程（故障）

```
某 task 抛异常 --> UncaughtExceptionHandler 设失败标志
StreamExecutor 检测 --> interrupt 全部 task --> 干净关闭；Coordinator.stop()
last = Coordinator.lastCompletedCheckpoint()
若存在且 retries < maxRestarts:
    从 last 重建（backend.restore + chain.restoreState + source.restoreOffset）
    重新启动 --> source 从 offset 重放 --> 继续 checkpoint
否则: throw 失败异常
```

## 7. 影响的现有代码

| 文件 | 改动 |
|---|---|
| `StreamElement` | 加 `Barrier`（第 4 种） |
| `StreamExecutor` | Channel 分配改 per-(上游,下游) pair；为每个 target 建 InputGate；失败检测 + interrupt；failover 重试循环 + 恢复重建 |
| `OperatorTask` | `input` 从 `Channel` 改 `InputGate`；主循环（barrier 在 InputGate 内消费，task 不感知）；`snapshot(id)`；`restore(SubtaskSnapshot)` |
| `Output` | 加 `sendBarrier`（广播所有下游） |
| `Channel` | 不变（仍是 `BlockingQueue<StreamElement>`，响应 interrupt） |
| `OperatorChain` | 加 `snapshotState()` / `restoreState(Map)`（收集/恢复链内算子状态） |
| `Operator` | 加 `default snapshotState()` / `default restoreState()` |
| `StateBackend` / `MemoryStateBackend` | 加 `snapshot()` / `restore()` |
| `RuntimeContextImpl` | 持久化钩子透传（snapshot 聚合 backend + 算子状态） |
| `SourceContext` / `SourceContextImpl` | 加 `emitted` / `skipUntil` / `snapshotOffset` / `restoreOffset` / `enqueueBarrier` |
| `SourceTask` | `triggerCheckpoint(id)`；snapshot 含 sourceOffset；restore 设 offset |
| `SourceOperatorImpl` | open 建 SourceContextImpl 时透传恢复 offset（恢复模式） |
| `CollectionSource` | `data` 字段 `Iterable`→`List`（逻辑不变） |
| `WindowOperator` | 实现 `snapshotState` / `restoreState`（timers + activeWindows） |
| `env.fromCollection` | 内部转 `ArrayList` |

全量回归阶段①②③④（InputGate/Channel 分配改造是阶段②核心改动，最大回归面）。

## 8. 测试与验收

### Phase 1 单测

- `InputGate` 对齐：单 channel（forward）零缓冲直接对齐 + 快照回调 + 广播；多 channel 交错（A 先到 barrier → A 后续缓冲、B 正常放行 → B 到齐 → 快照 → 广播 + 解缓冲按序放出）；快照回调仅在全部对齐后触发。
- `Barrier` 经 Channel 流动；`Output.sendBarrier` 广播所有下游。
- **StreamExecutor Channel 分配改造回归**：forward/hash/rebalance 路由仍正确（阶段②全量回归）。
- **失败关闭**：注入 task 异常 → 所有 task 在超时内退出（断言不挂起）。

### Phase 2 单测

- `MemoryStateBackend.snapshot/restore`：三类 store 深拷贝一致；restore 后 currentKey 重置。
- `SourceContextImpl` offset：emitted 累加 / skipUntil 跳过 / snapshot-restore 断点续传。
- `WindowOperatorState` snapshot/restore：timers + activeWindows 重建一致（恢复后窗口能继续触发）。
- `CheckpointCoordinator`：trigger→ack 汇聚→Checkpoint 完成；ack 缺失→废弃；lastCompletedCheckpoint。

### 端到端验收

- 周期 checkpoint 产出一致快照（多并行 keyed reduce 作业）。
- **failover exactly-once**：故障注入（第 N 条抛异常）→ 自动恢复重放 → 最终结果 = 无故障运行结果。两条线：
  - keyed reduce 作业（核心，无 window/timer）。
  - window 作业（验证 timer 持久化：恢复后未触发窗口继续到点触发输出）。
- 失败关闭：无 checkpoint 时 task 异常 → 干净失败抛异常（不挂起）。
- `maxRestarts` 耗尽：持续故障 → 达上限 → 抛失败。

## 9. 任务预览（writing-plans 细化）

**Phase 1（barrier 基础设施）**：

| # | 任务 |
|---|---|
| 1 | `Barrier` + `StreamElement` 扩展 + `Output.sendBarrier` + 单测（已完成） |
| 2 | `InputChannel` + `InputGate`（per-upstream 对齐算法 + SnapshotCallback 占位）+ 单测（已完成） |
| 3 | `StreamExecutor` Channel 分配改 per-pair + 建 InputGate + `OperatorTask.input` 改 InputGate + 阶段②全量回归（已完成） |
| 4 | `OperatorTask` barrier 对齐回调（task 不感知 barrier，InputGate 回调 snapshot 占位）+ 单测（已完成） |
| 5 | 失败关闭（StreamExecutor interrupt 全部 + 不挂起 + 超时兜底）+ 单测（已完成） |
| 6 | Phase 1 端到端：多并行 barrier 对齐流动（snapshot 占位）+ 文档（已完成） |

**Phase 2（checkpoint 与恢复）**：

| # | 任务 |
|---|---|
| 7 | `StateBackend.snapshot/restore` + `StateSnapshot` + 单测（已完成） |
| 8 | `SourceContext` offset（emitted/skipUntil/snapshot/restore）+ `fromCollection` 转 List + `CollectionSource` 持 List + 单测（已完成） |
| 9 | `Operator` 算子级快照钩子 + `OperatorChain.snapshotState/restoreState` + `WindowOperatorState` + `WindowOperator` snapshot/restore + 单测（已完成） |
| 10 | `SubtaskSnapshot` + `Checkpoint` + `OperatorTask.snapshot/restore` 整合 + 单测（已完成） |
| 11 | `CheckpointCoordinator`（周期 daemon + trigger source + ack 汇聚 + retained + stop）+ 单测（已完成） |
| 12 | 自动 failover 循环（StreamExecutor 重构 maxRestarts + 从 checkpoint 重启重建）+ 单测（已完成） |
| 13 | Phase 2 端到端：周期 checkpoint + failover exactly-once（keyed reduce + window）+ 文档（已完成） |
| 14 | 可运行示例 `CheckpointExample`（演示周期 checkpoint + 故障恢复）+ 文档 |

## 10. 风险与权衡

- **InputGate 并发对齐测试**：用确定性测试（直接控制 channel 发送顺序）而非真实时序，避免 flaky。
- **Channel 分配改造影响阶段②核心**：最大回归面；`InputGate` 保持 `receive()` 接口让 `OperatorTask` 主循环改动最小。
- **Coordinator / 作业竞态**：作业正常结束时 `Coordinator.stop()`（daemon + interrupt）避免线程泄漏；每次 trigger 用新 checkpointId，ack 状态隔离。
- **exactly-once 边界**：恢复后 keyed state 是快照值、source 从 offset 重放 → state 累加不重复；`CollectSink` 可能在恢复边界收到 running reduce 的重复输出条（最终聚合值仍正确）——验收以最终值为准。
- **`maxRestarts` 防无限循环**：持续故障源达上限即失败抛出。
- **key 序列化**：`WindowOperatorState` 含 `key:Object`，内存 backend 直接深拷贝（要求 key 可拷贝/可序列化）。
- **snapshot 回调分层**：Phase 1 用占位 `SnapshotCallback`，Phase 2 接真快照——保证 Phase 1 可独立测试对齐逻辑。
