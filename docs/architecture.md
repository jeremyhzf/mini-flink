# Mini-Flink 架构文档

> 本文档描述 mini-flink **当前实现的真实架构**（5 阶段全部完成后，`main` HEAD `6c66bb0`），基于源码梳理，重点说明分层设计、核心抽象与数据流转。学习目的：理解 Apache Flink 区别于普通流处理的核心机制——有状态计算、事件时间与窗口、基于 barrier 的容错——通过简化实现对照学习。

- **技术栈**：Java 17（records / 模式匹配）、Maven、JUnit 5、纯 JDK（无第三方运行时依赖）
- **部署形态**：单进程多线程模拟（一个 JVM，每 subtask 一个 `Thread`，内存 `BlockingQueue` 交换数据）
- **规模**：约 80 个主类，102 个测试，6 个可运行示例

---

## 1. 分层架构

mini-flink 采用 **4 层纵向分层 + 横切关注点**，命名与职责贴近真实 Flink，便于学到的概念直接迁移。

```
┌───────────────────────────────────────────────────────────────────┐
│  API 层 (org.miniflink.api)                                        │
│  StreamExecutionEnvironment · DataStream · KeyedStream · WindowedStream │
│  + 函数接口 (Map/Filter/FlatMap/Reduce/KeySelector/Source/Sink)    │  用户写作业
├───────────────────────────────────────────────────────────────────┤
│  StreamGraph 层 (org.miniflink.graph)                              │
│  Transformation · OneInputTransformation · SourceTransformation ·  │  用户代码编译出的
│  StreamGraph                                                       │  逻辑 DAG
├───────────────────────────────────────────────────────────────────┤
│  ExecutionGraph 层 (org.miniflink.execution)                       │
│  ExecutionGraph · ExecutionVertex · ExecutionEdge                  │  按并行度展开的
│  + 分区器 Forward / Hash / Rebalance                               │  物理执行计划
├───────────────────────────────────────────────────────────────────┤
│  Runtime 层 (org.miniflink.runtime)                                │
│  StreamExecutor · Task · OperatorTask · SourceTask                 │  线程与算子
│  Operator · OperatorChain · Channel · InputGate · InputChannel     │  实际运行
│  Output · OutputCollector · StreamElement(Record/Watermark/Barrier/EOB) │
│  StateBackend · RuntimeContext · CheckpointCoordinator             │
├───────────────────────────────────────────────────────────────────┤
│  横切：time（时钟/水印/定时器）· window（窗口分配/触发）            │
└───────────────────────────────────────────────────────────────────┘
```

> **与原设计的差异**：原设计 spec（`docs/superpowers/specs/2026-07-10-mini-flink-design.md`）计划了独立的 `state`/`checkpoint` 包，最终实现把状态后端放在 `runtime`（`MemoryStateBackend`），快照类（`StateSnapshot`/`Checkpoint`/`SubtaskSnapshot`）也在 `runtime`，仅 `WindowOperatorState` 在 `runtime/checkpoint`。会话窗口、`KeyedProcessOperator` 未实现（推迟）。

---

## 2. 核心抽象

### 2.1 API 层 —— 用户入口

| 类 | 职责 |
|---|---|
| `StreamExecutionEnvironment` | 作业入口，持有 `StreamGraph`；`fromCollection`/`addSource` 建 source，`enableCheckpointing(ms)`/`setMaxRestarts(n)` 配置容错，`execute(jobName)` 编译并执行 |
| `DataStream<T>` | 不可变流抽象，链式算子方法（`map`/`filter`/`flatMap`/`keyBy`/`assignTimestampsAndWatermarks`/`reduce`/`addSink`），每次返回新 `DataStream` |
| `KeyedStream<T,K>` | `keyBy` 的产物，持 hash 分区 + `KeySelector`；`reduce`/`window` 在此之上 |
| `WindowedStream<T,W>` | `KeyedStream.window(assigner)` 的产物；`reduce` 建 `WindowOperator` 转换 |
| 函数接口 | `MapFunction`/`FilterFunction`/`FlatMapFunction`/`ReduceFunction`（函数式）、`KeySelector`、`SourceFunction`（`run(SourceContext)`）、`SinkFunction`（`invoke(T)`） |

### 2.2 StreamGraph 层 —— 逻辑 DAG

用户链式调用每一步产生一个 `Transformation`，串成单线性链（当前仅支持单 source → 单 sink 的线性拓扑）。

- `Transformation<T>`：逻辑节点基类（持算子/并行度/分区器/keySelector）。
- `OneInputTransformation<IN,OUT>`：单输入转换（map/filter/keyBy/reduce/window 等）。
- `SourceTransformation<T>`：source 转换（持 `SourceOperator`）。
- `StreamGraph`：收集所有 transformation + 标记 sink；`execute` 时回溯 sink→source 链。

### 2.3 ExecutionGraph 层 —— 物理计划

`ExecutionGraph.from(StreamGraph)` 把逻辑图编译成物理计划：链化分组 → 按 parallelism 展开 → 组间建边。

- `ExecutionVertex(id, subtaskIndex, parallelism, operators, sourceOperator)`：一个 subtask。`id` 全局唯一（checkpoint 的 subtask 标识用它）。source vertex 的 `operators` 为空、`sourceOperator` 非 null；处理 vertex 反之。
- `ExecutionEdge(sources, targets, partitioner, keySelector)`：**组间边**（上游 subtask 组 → 下游 subtask 组），携带分区器。
- **链化分组**（`ExecutionGraph.from` 内部）：source 单独一组；连续 `ForwardPartitioner` + 同并行度的处理算子合并进同一组（**算子链**），在同一线程内函数调用直连，省去队列中转。
- **分区器**（`Partitioner.selectChannel(numDownstream, key, upstreamIndex)`）：
  - `ForwardPartitioner`：一对一（上游 i → 下游 i），要求同并行度。
  - `HashPartitioner`：`floorMod(key.hashCode(), n)`，同 key 恒落同一下游（keyBy 用）。
  - `RebalancePartitioner`：轮询（`AtomicInteger` 循环分发）。

### 2.4 Runtime 层 —— 实际执行

#### 数据元素（`StreamElement` 接口，在 `Channel` 里流动的统一类型）

| 元素 | 含义 | 引入阶段 |
|---|---|---|
| `Record<T>(value, timestamp)` | 数据记录，携带事件时间戳 | ①/④（加 ts） |
| `EndOfBroadcast`（`INSTANCE` 单例） | 广播哨兵，表示某上游结束（引用计数关闭） | ② |
| `Watermark(timestamp)` | 事件时间水位线，表示「≤ ts 的数据已基本到齐」 | ④ |
| `Barrier(checkpointId)` | checkpoint 屏障，触发对齐与快照 | ⑤ |

#### 通道与输入

- `Channel`：有界 `LinkedBlockingQueue<StreamElement>`（默认容量 64）。`send`=put（满则阻塞=反压）、`receive`=take（空则阻塞）、`poll`（非阻塞，InputGate 轮询用）。**不提供 close**——关闭完全由 EOB 哨兵驱动。
- `InputChannel`：包装一个 `Channel` + 对齐状态（已收到的 barrier id）+ 缓冲队列（barrier 后该上游的 record 暂存）。
- `InputGate`：下游 subtask 的**输入聚合**，含 N 个 `InputChannel`（每上游 subtask 一个）。封装 Chandy-Lamport 对齐：**Barrier 在 InputGate 内部消费，不返回给调用方**；全部上游对齐时回调 `SnapshotCallback` 并经 `barrierForwarder` 广播。`receive()` 只向 task 返回 Record/Watermark/EOB。
- `Output(downstreamChannels, partitioner, keySelector)`：一个 fan-out 目标。`route(value, ts, upstreamIndex)` 按分区器选 channel 发 Record；`sendEob`/`sendWatermark`/`sendBarrier` 广播所有下游。
- `OutputCollector(outputs, ctx)`：算子侧 Collector，`collect(value)` 读 `ctx.getCurrentTimestamp()` → 路由到下游。

#### 算子

- `Operator<IN,OUT>` 接口：`open(Collector, RuntimeContext)` / `processElement(IN)` / `close()` / `copy()` / `default onWatermark(Watermark)` / `default snapshotState()` / `default restoreState(OperatorState)`。
- `OperatorChain<IN,OUT>`：链内算子序列，`open` 从尾到头接线（链尾接 output，其余接 `ChainCollector` 直连下游算子）。`processElement` 经链头算子级联到链尾。
- 内置算子（`runtime/operator/`）：`MapOperator`/`FilterOperator`/`FlatMapOperator`/`ReduceOperator`（per-key ValueState running reduce）/`SinkOperator`/`SourceOperatorImpl`/`TimestampsAndWatermarksOperator`/`WindowOperator`。

#### 执行单元

- `Task extends Runnable`：执行单元基类（`broadcastEob`/`broadcastWatermark`/`broadcastBarrier` 默认方法）。
- `SourceTask(sourceOperator, outputs, ctx[, coordinator, snapshotKey])`：`open source → run → 广播 Watermark(+∞) → 广播 EOB`；Phase 2 加源线程 checkpoint 钩子（`requestCheckpoint`/`configureCheckpointEmitter`）。
- `OperatorTask(chain, inputChannels, pendingUpstreams, outputs, ctx[, coordinator, snapshotKey, restoreSnapshot])`：主循环 `InputGate.receive()` 分发 Record（设 ts + `chain.processElement`）/ Watermark（`chain.onWatermark` + 转发）/ EOB（计数）；Phase 2 对齐回调 `onAligned` 做快照。
- `StreamExecutor`：编排者——建 per-pair Channel + InputGate + Task，启动线程，失败关闭，Phase 2 的 failover 重试循环。

#### 状态

- `StateBackend` 接口：`getValueState/getListState/getMapState(name)` / `setCurrentKey(key)` / `snapshot()` / `restore(StateSnapshot)`。
- `MemoryStateBackend`：per-subtask，三类存储 `name → (currentKey → 值)`（`valueStore`/`listStore`/`mapStore`）。`snapshot` 三层深拷贝；`restore` 整体替换 + 重置 currentKey。
- `ValueState<T>`/`ListState<T>`/`MapState<K,V>`：句柄，经 `backend.currentKey()` 寻址（`ValueStateImpl` 等）。
- `RuntimeContext` 接口（per-subtask）：`getSubtaskIndex`/`getParallelism`/`getCurrentKey`/`setCurrentKey`/`getStateBackend`/`getKeySelector`/`getCurrentTimestamp`/`setCurrentTimestamp`/`emitWatermark`。`RuntimeContextImpl` 持 `MemoryStateBackend` + 当前 ts + watermark emitter。

#### 横切：时间与窗口

- `time/`：`WatermarkStrategy`（`forBoundedOutOfOrderness` + `TimestampAssigner` + `currentWatermark` + `copy`）、`BoundedOutOfOrdernessWatermarks`（`maxTimestamp - 乱序容忍`）、`InternalTimerService`（`TreeSet<Long>` 定时器，`advanceTo(wm, handler)` 触发 ≤ wm 的 timer）、`TimerHandler`（`onEventTime` 回调）。
- `window/`：`Window`/`TimeWindow(start,end)`、`WindowAssigner`（`TumblingEventTimeWindows`/`SlidingEventTimeWindows`）、`Trigger`/`EventTimeTrigger`（`onElement` 注册 window.end timer，`onEventTime(end)` → `FIRE_AND_PURGE`）、`TriggerContext`/`TriggerResult`。

#### 横切：checkpoint 与快照

- `CheckpointCoordinator`：daemon 线程周期触发 + ack 汇聚 + retained 管理。
- `StateSnapshot`/`SubtaskSnapshot`/`Checkpoint`/`OperatorState`/`WindowOperatorState`：快照数据类（均 `Serializable`）。

---

## 3. 数据流转（三个视角）

### 3.1 视角一：作业编译流（API → 物理计划 → 线程）

```
用户代码                          StreamExecutionEnvironment.execute(jobName)
  │                                │
  │  DataStream 链式调用            │  ① StreamGraph: 回溯 sink→source，反转得 [source, op1, ..., sink]
  │  (map/filter/keyBy/reduce…)    ▼
  │                               StreamGraph  (Transformation 线性链)
  │                                │
  │                                │  ② ExecutionGraph.from(streamGraph):
  │                                │     - 链化分组（source 单组；forward 同并行度合并）
  │                                │     - 按 parallelism 展开（每 subtask copy() 独立算子）
  │                                │     - 组间建边（带分区器）
  │                                ▼
  │                               ExecutionGraph (ExecutionVertex[subtask] + ExecutionEdge)
  │                                │
  │                                │  ③ StreamExecutor.execute(graph):
  │                                │     - per-pair Channel 分配（见 3.2）
  │                                │     - 每 vertex 建 RuntimeContextImpl + Task
  │                                │     - 启动线程 + 失败检测
  │                                ▼
  │                               Task 线程跑算子主循环（SourceTask / OperatorTask）
```

**关键**：`ExecutionGraph.from` 是编译核心——`canChain = 前序非空 && ForwardPartitioner && 同并行度`，链化后的算子组在同一线程内函数直连（不经 Channel）；组间才建 Channel 边（按分区器）。forward 同并行度 → 算子链；hash（keyBy）/不同并行度 → 跨组 Channel。

### 3.2 视角二：运行时数据流（per-pair channel + 算子链 + Record 流）

`StreamExecutor` 为每个 **(上游 subtask, 下游 subtask)** 对建一个独立 `Channel`（per-pair），而非共享单 channel。这让下游能区分上游 → 支持 barrier 对齐。

```
拓扑示例: source(p=1) → map(p=1, forward, 链化) → keyBy → reduce(p=2, hash) → sink(p=1)

编译后:
  Group A: [source] (p=1)          Group B: [map] (p=1)          ← A→B forward 同并行度，本可链化
                                                                  （若连续 forward 则合并；此处 keyBy 打断）
  Group C: [reduce] (p=2, hash)    Group D: [sink] (p=1)

per-pair channel:
  source.sub0 ──ch──► map.sub0     (forward: 1 对)
  map.sub0 ──ch0──► reduce.sub0    (hash fan-out: map 连 2 个 reduce subtask)
         └──ch1──► reduce.sub1
  reduce.sub0 ──ch0──► sink.sub0   (rebalance fan-in: 2 个 reduce → 1 个 sink)
  reduce.sub1 ──ch1──┘

每个下游 subtask 的 InputGate 收集其入边 channel:
  reduce.sub0.InputGate = [ch(map→reduce0)]              (1 上游)
  sink.sub0.InputGate   = [ch(reduce0→sink), ch(reduce1→sink)]   (2 上游 fan-in)
```

**一个 Record 的旅程**（`sensor-1` 温度读数，hash 落 reduce.sub0）：

```
SourceTask.sub0
  │ SourceContext.collect(record)
  │   └─ OutputCollector.collect → Output.route(value, ts, upstreamIndex)
  │        └─ HashPartitioner.selectChannel(numDownstream=2, key="sensor-1", _) → 0
  │        └─ ch(map→reduce0).send(Record(value, ts))     [BlockingQueue.put，满则反压]
  ▼
OperatorTask.sub0 (reduce.sub0)
  │ InputGate.receive()
  │   └─ InputChannel.poll() → 拿到 Record（barrier 对齐期间该 channel 的 record 会被缓冲）
  │   └─ 非 barrier → 返回 Record 给 task
  │ ctx.setCurrentTimestamp(record.ts)
  │ chain.processElement(record.value())
  │   └─ ReduceOperator.processElement:
  │        ctx.setCurrentKey("sensor-1")
  │        ValueState acc = backend.getValueState("reduce-acc")
  │        acc.update(reduceFn.reduce(acc.value(), record))   [per-key ValueState 累加]
  │        out.collect(累加结果)                              [running reduce 每输入输出]
  │   └─ OutputCollector.collect → route → 下游 ch
  ▼
... 直到 sink
```

**反压**：每条 `Channel` 是有界 `BlockingQueue`（容量 64）。下游慢 → channel 满 → 上游 `send`(put) 阻塞 → 反压自然形成，逐级传导到 source。

**算子链内的直连**：链化算子（如 source→map forward 同并行度合并）不经 Channel，`ChainCollector` 直接调下游算子 `processElement`——线程内函数调用，零队列开销。

### 3.3 视角三：控制信号流（Watermark / Barrier / EOB）

`StreamElement` 的三种非 Record 元素各承担一类控制语义，与数据同通道流动。

#### Watermark —— 事件时间推进

```
有界 source 结束 / TS&WM 算子生成
  │ source.run() 结束 → broadcastWatermark(Watermark(+∞))      [触发所有未触发窗口]
  │ 或 TimestampsAndWatermarksOperator.processElement:
  │     ts = strategy.extractTimestamp(record)
  │     ctx.setCurrentTimestamp(ts); out.collect(Record(record, ts))
  │     ctx.emitWatermark(Watermark(strategy.currentWatermark()))   [每条记录后发当前 watermark]
  ▼
OperatorTask 主循环收到 Watermark:
  │ chain.onWatermark(wm)        [链内每个算子收到]
  │   └─ WindowOperator.onWatermark: timerService.advanceTo(wm.ts, this)
  │        └─ 触发所有 end ≤ wm.ts 的 timer → onEventTime(end) → 输出窗口最终值 + 清理
  │ broadcastWatermark(outputs, wm)   [转发下游，保持 watermark 流]
```

- watermark **单调**（`BoundedOutOfOrdernessWatermarks.currentWatermark()` 只增不减；`InternalTimerService.currentWatermark` 取 `Math.max`）。
- 作业正常结束：source 广播 `Watermark(+∞)` → 推进所有剩余窗口 → 再广播 EOB。
- **当前限制**：阶段④仅 parallelism=1 的 watermark 语义；多上游 watermark 的 per-upstream min 对齐未做（推迟）。

#### Barrier —— checkpoint 对齐（Chandy-Lamport）

```
CheckpointCoordinator (daemon 线程，按 interval)
  │ triggerOnce: currentId = idCounter++;  各 sourceTask.requestCheckpoint(currentId)
  ▼
SourceTask.requestCheckpoint → SourceContextImpl.requestedBarrierId = id   [仅置 volatile 标志]
  │
  ▼  源线程的 collect() 内，发下一条 record 之前处理 pending:
SourceContextImpl.collect(record_k):
  │ if (requestedBarrierId >= 0):
  │     id = requestedBarrierId; requestedBarrierId = -1
  │     checkpointEmitter.emit(id, Math.max(emitted, skipUntil))   [源线程内：backend.snapshot + ack + sendBarrier]
  │     └─ barrier 物理上在 record_k 之前入 channel（FIFO）→ offset=k 反映「barrier 前已发 record_0..k-1」
  │ emitted++; out.collect(record_k)
  ▼
barrier 经 channel 流到下游 InputGate:
InputGate.receive() 内部:
  │ 某 InputChannel 收到 Barrier(id):
  │   markAligned(id); 该 channel 后续 Record 缓冲
  │   当所有 InputChannel 对齐 id:
  │     callback.onAligned(id)   [→ OperatorTask.onAligned: backend.snapshot + chain.snapshotState → SubtaskSnapshot → coordinator.ack]
  │     barrierForwarder.accept(barrier)   [广播 barrier 到下游]
  │     解除所有 channel 缓冲（缓冲 Record 按序放出）
  │ 未对齐 channel 的 Record 正常放行（不阻塞整个 task）
  ▼
CheckpointCoordinator.ack(snapshotKey, id, snapshot):
  │ synchronized: pendingAcks.put(snapshotKey, snapshot)
  │ 收齐全部 subtask ack → completed.addLast(new Checkpoint(id, snapshots))   [retained 最近 N 个]
```

- **关键封装**：对齐逻辑全在 `InputGate` 内，`OperatorTask` 主循环**不感知 barrier**——它只看到过滤后的 Record/Watermark/EOB；对齐完成时回调快照并自动转发 barrier。
- **exactly-once 一致性**：source 的 offset 与下游算子状态在**同一个 barrier 切割面**——source 在 record_k 前发 barrier（offset=k），下游在 barrier 处快照（状态 = 处理完 record_0..k-1 的结果），两者坐标系一致。
- **单上游（forward）**：InputGate 仅 1 channel，barrier 到达即对齐，零缓冲。

#### EOB —— 优雅关闭（引用计数）

```
上游 subtask 结束 → broadcastEob(outputs, subtaskIndex)
  │ Output.sendEob(upstreamIndex):
  │   forward: 仅向 downstreamChannels[upstreamIndex] 发 EOB（一对一）
  │   其他分区器: 向所有下游 channel 广播 EOB
  ▼
下游 OperatorTask 收到 EOB: remaining--（初始 = pendingUpstreams = InputChannel 数）
  │ remaining == 0 → 所有上游结束 → broadcastEob 转发 → 退出主循环
  ▼
逐级关闭到 sink，作业退出
```

- forward 一对一：下游 i 只收上游 i 的 EOB（`countUpstreams` 计 1）。
- fan-in：下游收 N 个 EOB（每上游一个），全部到齐才退出（引用计数关闭，不丢数据）。

---

## 4. 关键机制

### 4.1 分区 + 算子链

| 连接类型 | 分区器 | channel 模型 | 是否链化 |
|---|---|---|---|
| source→op 或 op→op（同并行度 forward） | Forward | per-pair，下游 Output 位置 i 放 channel 余 null 占位 | ✅ 链化（同线程函数直连） |
| keyBy | Hash | per-pair 全连接 | ❌ 跨组（hash 路由） |
| 不同并行度 forward | 自动改 Rebalance | per-pair 全连接 | ❌ 跨组 |
| rebalance | Rebalance | per-pair 全连接 | ❌ 跨组 |

> **forward 的 per-pair 占位**：source `s_i` 的 forward `Output.downstreamChannels` 是长 |targets| 列表，**位置 `subtaskIndex` 放真 channel、其余 `null`**。`ForwardPartitioner.selectChannel` 返回 `subtaskIndex` 命中真 channel；`sendWatermark`/`sendBarrier`/`sendEob` 遍历时跳过 null。这样 `ForwardPartitioner` 无需改动（保持 `selectChannel=num→upstreamIndex`），且 fan-in 下游的 InputGate 各 channel 来自独立上游。

### 4.2 Keyed State —— per-key 寻址

```
ReduceOperator.processElement(record):
  ctx.setCurrentKey(keySelector.getKey(record))     [keyBy 保证同 key 恒落同一 subtask]
  ValueState acc = ctx.getStateBackend().getValueState("reduce-acc")
  acc.update(reduce(acc.value(), record))            [ValueStateImpl 经 backend.currentKey() 寻址]
```

- **并发不变式**：`HashPartitioner` 保证同 key 恒落同一 subtask → 该 key 的所有状态访问在同一 Task 线程 → 无跨线程竞争。
- **per-key 隔离**：`MemoryStateBackend` 的三类 store 都是 `name → (currentKey → 值)`；`setCurrentKey` 切换寻址键。
- **per-subtask 隔离**：每 subtask 一个 `RuntimeContextImpl` + 独立 `MemoryStateBackend`，多 subtask 间状态不共享。`Operator.copy()` 共享无状态用户函数，状态句柄在 `open()` 从 per-subtask ctx 获取。

### 4.3 事件时间 + 窗口

```
Record(v, ts) 流入 WindowOperator:
  │ for window ∈ assigner.assignWindows(v, ts):       [滚动: 1 个窗口；滑动: size/slide 个]
  │   if window.end <= currentWatermark: continue      [迟到丢弃，无 allowedLateness]
  │   acc = MapState.get(window); reduced = reduce(acc, v); MapState.put(window, reduced)
  │   if (key,window) 首次出现: 注册 activeWindows[end] + trigger.onElement(注册 window.end timer)
  ▼
Watermark 推进到 window.end:
  │ InternalTimerService.advanceTo 触发 timer@end → WindowOperator.onEventTime(end)
  │   for (key,window) in activeWindows[end]:
  │     ctx.setCurrentKey(key); trigger.onEventTime(end) → FIRE_AND_PURGE
  │     out.collect(MapState.get(window))   [输出窗口最终累加值，一次]
  │     MapState.put(window, null)          [清理]
```

- **per-key per-window 状态**：`MapState<TimeWindow, IN>`（命名 `"window-accs"`），按 currentKey + window 双维寻址。
- **活跃窗口注册表**：`activeWindows: end → [(key, window)]`，按 end 直达待触发窗口，避免遍历所有 key。
- **迟到丢弃**：watermark 超过 `window.end` 后到达的记录直接丢弃（无 allowedLateness）。

### 4.4 Checkpoint + 自动 Failover（exactly-once）

**快照内容**（每 subtask 的 `SubtaskSnapshot`）：
- `keyedState`：`StateBackend.snapshot()`（三类 store 深拷贝）—— 所有 per-key 累加器。
- `operatorStates`：`OperatorChain.snapshotState()`（按算子索引）—— `WindowOperator` 的 timers + activeWindows（`WindowOperatorState`）。
- `sourceOffset`：source 已发条数（仅 source subtask，其余 -1）—— 恢复重放的断点。

**Failover 循环**（`StreamExecutor.execute` 重构为重试）：

```
lastCp = null
for attempt in 0..maxRestarts:
    buildTasks(graph, lastCp)          # 冷启 lastCp=null；恢复 lastCp=上轮 checkpoint
      # 每 vertex: copy() 新算子 → RuntimeContextImpl → 若 lastCp: backend.restore + chain.restoreState + source.restoreOffset
    coordinator = new CheckpointCoordinator(interval, sources, snapshotKeys, retained=2)
    failure = runOnce(tasks, coordinator)   # 启动 + coordinator.start + 失败检测 + join
    coordinator.stop()
    if failure == null: return              # 正常结束
    lastCp = coordinator.lastCompletedCheckpoint()
    if lastCp == null: throw(失败, cause)    # 无可用 checkpoint → 直接失败
# 达 maxRestarts → throw(已达上限, lastFailure)
```

**恢复重建顺序**（在 Task.run() 内，确保句柄已建）：
- OperatorTask：`chain.open`（建 state 句柄）→ `backend.restore(keyedState)` → `chain.restoreState(operatorStates)` → 主循环。
- SourceTask：`open`（建 SourceContextImpl）→ `restoreOffset(offset)`（设 skipUntil）→ `configureEmitter` → `run`（重放时跳过前 offset 条）。

**exactly-once 保证**：恢复时 backend 整体替换（非累加）+ source 从 offset 重放（跳过已 checkpoint 的）→ 不丢不重。`Math.max(emitted, skipUntil)` 修复了多重启 skip 期的坐标系不一致（避免双重累加）。

---

## 5. 并发模型与失败关闭

### 5.1 线程模型

- 每 subtask 一个 `Thread`（`miniflink-task-N`），跑 `SourceTask` 或 `OperatorTask`。
- `CheckpointCoordinator` 一个 daemon 线程（`miniflink-checkpoint`），周期触发。
- `StreamExecutor` 主线程：启动 + join + 失败检测 + failover 协调。
- 数据交换：有界 `BlockingQueue`（反压）+ `InputGate` 轮询/阻塞。

### 5.2 失败关闭（修了阶段②遗留的头号缺口）

任一 Task 抛未捕获异常 → `UncaughtExceptionHandler`：
1. `error.compareAndSet(null, e)` 记录首个 cause。
2. `interruptOthers`：中断所有其他未结束线程（`CopyOnWriteArrayList` 快照遍历，消除 CME）。
3. `Channel.put/take` 响应 `InterruptedException` → task 的 `catch(InterruptedException)` 退出 → 解锁所有阻塞。
4. 主线程 join（带 30s 超时兜底，极端不响应中断的 task 再 interrupt + 5s）。

→ 作业**干净停止**，不再永久挂起（阶段②的 join 无超时缺口）。

### 5.3 并发安全要点

- `threads` 用 `CopyOnWriteArrayList`（UCE 遍历与主线程 add 并发安全）。
- `CheckpointCoordinator.ack`/`triggerOnce` 在 `inflightLock` 上 synchronized；`completed` 用 `ConcurrentLinkedDeque`。
- `SourceContextImpl.requestedBarrierId` / `SourceOperatorImpl.ctx` 用 `volatile`（daemon 线程读、source 线程写）。
- keyed state 按 key + subtask 隔离，Task 内单线程访问，无竞争。
- `InputGate.nextRaw` 的 buffer 排空门控于 `aligningId < 0`（避免对齐中死循环）；阻塞 take 只作用于**未关闭** channel（`channelClosed[]`，避免 fan-in 永久挂起）。

---

## 6. 五阶段能力演进

| 阶段 | 核心能力 | 关键类 | 可运行示例 |
|---|---|---|---|
| **① 骨架** | 四层分层、单线程同步链 source→map→sink | `DataStream`/`StreamGraph`/`ExecutionGraph`/`Operator`/`Channel` | `TextProcessingExample` |
| **② 并行** | 多线程 Task、有界 Channel 反压、算子链、3 分区器、EOB 引用计数关闭 | `ExecutionVertex` 切分、`Task` 多线程、`Forward/Hash/Rebalance` | `ParallelExample` |
| **③ 状态** | keyed state、keyBy、StateBackend 统一注入 | `StateBackend`/`MemoryStateBackend`/3 state/`RuntimeContext`/`ReduceOperator`/`KeyedStream` | `WordCountExample` |
| **④ 时间/窗口** | event time、watermark、TimerService、滚动/滑动窗口、WindowOperator | `Watermark`/`WatermarkStrategy`/`InternalTimerService`/`WindowAssigner`/`Trigger`/`WindowOperator`/`WindowedStream` | `WindowExample` |
| **⑤ 容错** | barrier 对齐、状态快照、自动 failover、window timer 持久化 | `Barrier`/`InputGate`/`StateBackend.snapshot`/`CheckpointCoordinator`/`SubtaskSnapshot`/`Checkpoint`/failover 循环 | `CheckpointExample` |
| 扩展 | 自定义 Source/Sink 边界 | `SourceFunction`/`SinkFunction`/`SourceContext` | `CustomSourceSinkExample` |

---

## 7. 已知简化与后续

阶段⑤ scope 是「仅容错核心」，以下推迟为独立小阶段（final review 标非阻断）：

- **多并行 watermark min 对齐**：keyBy 下游收到多上游 watermark 需 per-upstream 跟踪取 min，否则快上游提前推进时钟。
- **会话窗口** / **allowedLateness**（迟到直接 drop）/ **MapState.remove**（用 put null 清理，entry 残留）。
- **InternalTimerService.currentWatermark 持久化**：恢复时重置 `Long.MIN_VALUE`，极端 late record 场景行为差异。
- **maxRestarts 耗尽测试**（实现逻辑正确，缺锁定测试）。
- checkpoint 快照当前**内存持有**（`Serializable` 但未落盘），如需持久化可序列化到文件。

---

## 8. 示例索引

| 示例 | 演示 |
|---|---|
| `TextProcessingExample` | ① 骨架：source→map→filter→sink 单线程链 |
| `ParallelExample` | ② 并行：多 subtask + forward/rebalance 分区 |
| `WordCountExample` | ③ keyed state：keyBy + per-key ValueState 累加 |
| `WindowExample` | ④ 窗口：event time + watermark + 滚动窗口聚合 |
| `CheckpointExample` | ⑤ 容错：周期 checkpoint + 故障自动恢复（exactly-once） |
| `CustomSourceSinkExample` | 自定义扩展：`SourceFunction` + `SinkFunction` 边界 |

运行任一示例：`mvn -q compile && java -cp target/classes org.miniflink.examples.<Name>`

---

## 9. 参考文档

- 整体设计 spec：`docs/superpowers/specs/2026-07-10-mini-flink-design.md`
- 各阶段设计：`docs/superpowers/specs/2026-07-1*-mini-flink-stage*-design.md`
- 各阶段实现计划：`docs/superpowers/plans/2026-07-1*-mini-flink-stage*.md`
- 示例文档：`docs/examples/*.md`

