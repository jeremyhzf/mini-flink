# Mini-Flink 阶段②（并行执行）设计补充

- **日期**：2026-07-10
- **状态**：已通过 brainstorming，待编写实现计划
- **关系**：本文档细化 [整体设计](2026-07-10-mini-flink-design.md) 的阶段②部分。整体 spec 定了高层方向，本文档定阶段②的具体架构决策（多线程模型、关闭语义、反压、算子链等），作为 `writing-plans` 的输入。

## 1. 背景与目标

阶段①（骨架）已在 `main` 完成：四层分层（API / StreamGraph / ExecutionGraph / Runtime）+ 单线程同步执行链（`OperatorOutput` 同步调用、`StreamExecutor` 单线程 `open→source.run→close`）。

阶段②把同步链改造为**多线程管道**：每个 subtask 一个线程，subtask 间用有界通道连接，实现真正的并行、反压，并支持 `parallelism` 与分区。

### 设计原则（延续）

- **对照学习**：分层与命名贴近真实 Flink。
- **稳定边界**：`Collector<T>` 接口不变，算子对多线程/通道无感知。
- **YAGNI**：`Watermark`/`Barrier` 不在阶段②引入，只预留 `StreamElement` 接口可扩展点。
- **先正确后优化**：关闭语义优先于性能。

## 2. 范围

### 包含

- 多线程 `Task`（每个 subtask 一个线程，`open→循环→close` 纳入 try/finally）。
- 有界 `Channel` + 天然反压。
- `StreamElement` 接口 + `Record<T>`（通道统一元素）。
- 三种分区器：`ForwardPartitioner` / `HashPartitioner`(keyBy) / `RebalancePartitioner`。
- `parallelism` 落地：`ExecutionGraph` 按并行度切 `ExecutionVertex`。
- **算子链**（Operator Chaining）：同并行度 + forward 的相邻算子合并进同一 Task。
- 关闭语义：EOB 哨兵 + 引用计数对齐。
- API：`setParallelism`、`keyBy`（分区；keyed state 留阶段③）。
- final review flag 项：`StreamExecutor` 重构（Collector 稳定边界、open 纳入 try/finally、list 封装加固）、补 `ExecutionGraph.from` 两分支单测。

### 不包含（留后续阶段）

- `Watermark` / event time / 窗口（阶段④）。
- `CheckpointBarrier` / 状态快照（阶段⑤）。
- keyed state（阶段③；阶段②的 keyBy 只做分区）。
- unaligned checkpoint、动态扩缩容、失败恢复（阶段⑤及以后）。

## 3. 已确认的关键决策

| 决策 | 选择 | 备注 |
|---|---|---|
| 算子链 | **做** | 同并行度 + forward 算子合并进同一 Task，省队列跳转 |
| 分区器 | forward + hash + rebalance（全做） | spec 第 6 节既定 |
| 关闭语义 | **EOB 哨兵 + 引用计数对齐** | 见 §6 |
| StreamElement | 接口 + `Record<T>`，不预留 Watermark/Barrier | 接口可扩展，阶段④⑤ 再加实现 |
| Collector 边界 | 接口不变 | 实现从 `OperatorOutput` 换成 `ChannelWriter` |

## 4. 核心抽象

### 4.1 `StreamElement` + `Record<T>`（runtime）

```java
public interface StreamElement { }              // 通道统一元素，阶段④⑤ 可加 Watermark/Barrier 实现
public record Record<T>(T value) implements StreamElement { }
```

### 4.2 `Channel`（runtime）

有界通道，包装 `BlockingQueue<StreamElement>`：

```java
public class Channel {
    private final BlockingQueue<StreamElement> queue;   // 有界，如容量 64
    public Channel(int capacity) { ... }
    public void send(StreamElement e) throws InterruptedException;  // put：满则阻塞 = 反压
    public StreamElement receive() throws InterruptedException;     // take：空则阻塞
}
```

- 无 `close`：关闭完全由 EOB 哨兵驱动（见 §6），Channel 无外部资源需释放。
- EOB 是一个 `StreamElement` 单例哨兵，经 `send`/`receive` 正常流动。

### 4.3 `Collector` 边界不变，新增 `ChannelWriter`

`Collector<T>` 接口（`collect(T)` / `close()`）完全不变。新增实现 `ChannelWriter<T> implements Collector<T>`：`collect(T)` 把值包装成 `Record<T>` 写入下游 `Channel`。算子依赖 `Collector` 接口，对通道无感知。

### 4.4 `OperatorChain`（runtime）

一个 Task 内链化的算子序列（同并行度 + forward 连接的相邻算子）：

- 链内算子间**直接函数调用**（不经 Channel）：上游算子的 `Collector` 是一个 `ChainCollector`，`collect` 直接调下游算子的 `processElement`。
- 链尾算子的 `Collector` 是 `ChannelWriter`，写入下游 Channel。
- 链头算子接收 Task 从输入 Channel 解包的 value。

### 4.5 `Task`（runtime，`Runnable`）

两种：

- **`SourceTask`**：持有 `SourceOperator` + 下游 `Channel` 列表（含分区器）。`run`：`open` → `source.run()`（数据经 SourceContext → ChannelWriter → 按 partitioner 写下游 Channel）→ `finally close`。结束后向每个下游 Channel 发 EOB。
- **`OperatorTask`**：持有 `OperatorChain` + 输入 `Channel` 列表 + 下游 `Channel` 列表 + `pendingUpstreams` 计数。`run`：`open` → 循环 `receive` → EOB 则计数--（归零则向下游广播 EOB 并退出）→ 否则解包 value 经 OperatorChain 处理 → `finally close`。

`open`/`run`/`close` 全部在 Task 的 try/finally 内（修复阶段① open 在 try 外的隐患）。

### 4.6 `Partitioner`（execution）

```java
public interface Partitioner {
    int selectChannel(int numDownstream, Object recordOrKey, int upstreamIndex);
}
```

- `ForwardPartitioner`：返回 `upstreamIndex`（一对一，要求上下游并行度相同）。
- `HashPartitioner`：按 key 哈希取模。
- `RebalancePartitioner`：轮询（维护一个递增计数器）。

> 注：阶段② channel 选择基于 subtask 索引；具体签名在计划阶段定，但语义如上。

### 4.7 `ExecutionVertex` + `ExecutionEdge`（execution）

- `ExecutionVertex`：一个 subtask，含 `OperatorChain`（或 source）+ `subtaskIndex` + `parallelism`。
- `ExecutionEdge`：连接上游 vertex 组与下游 vertex 组，含 `Partitioner` + 上游/下游 vertex 列表。

### 4.8 `ExecutionGraph.from` 重构（execution）

从 `StreamGraph` 构建：

1. **拓扑展开**：每个 `Transformation` 按 `parallelism` 切成多个 `ExecutionVertex`（source 同样切，`CollectionSource` 按 parallelism 分片数据）。
2. **链化**：对每条 forward 边（同并行度、ForwardPartitioner），把上下游算子合并进同一 `OperatorChain`（同一 vertex）。
3. **分区边**：keyBy → `HashPartitioner`；rebalance → `RebalancePartitioner`；其余 forward → `ForwardPartitioner`。

> 阶段①的两个 `IllegalStateException` 分支（sink 数 ≠ 1、回溯未终止于 source）仍保留并补单测（final review flag）。阶段②仍维持**单 sink** 约束（与阶段①一致；多 sink / union 留后续阶段）。

### 4.9 `StreamExecutor` 重构（runtime）

```java
public void execute(ExecutionGraph graph) throws Exception {
    // 1. 为每条 ExecutionEdge 建 Channel
    // 2. 为每个 ExecutionVertex 建 Task（SourceTask / OperatorTask），接好输入/输出 Channel
    // 3. 启动所有 Task 线程
    // 4. join 等待全部结束（EOB 级联关闭后所有 Task 自然退出）
    // 5. 收集异常（任一 Task 失败则传播）
}
```

## 5. 反压

`Channel` 有界（容量常量，如 64），`send` = `BlockingQueue.put`（满则阻塞生产者线程）。下游慢 → 队列满 → 上游阻塞 → 反压沿管道向上传播。无额外机制。

## 6. 关闭语义（EOB 哨兵 + 引用计数对齐）

**EOB（EndOfBroadcast）**：一个 `StreamElement` 单例哨兵，表示"发送方不再发数据"。

传播规则：

1. **source 结束**：`SourceTask` 的 `source.run()` 返回后，向其**每个**下游 Channel 发送一个 EOB。
2. **fan-out**（rebalance/hash，一个上游 → 多下游）：上游向每个下游 Channel 各发一个 EOB（每个下游各一份）。
3. **fan-in**（多上游 → 一个下游）：下游 `OperatorTask` 初始化 `pendingUpstreams = 上游数`。每从任一输入 Channel `receive` 到一个 EOB，`pendingUpstreams--`。当 `pendingUpstreams == 0`：
   - 处理完已收到的数据，
   - 向自己的每个下游 Channel 发送 EOB，
   - 退出循环。
4. **算子链**：链内无 Channel，EOB 只在 Task 边界（Channel）传播。

**正确性**：引用计数保证一个下游只在所有上游都结束后才退出，不丢数据、不提前退。EOB 顺序与数据顺序一致（FIFO Channel），不会越过未处理数据。

## 7. 并发安全

- 每个 Task **单线程**访问自己的 OperatorChain/算子/状态，无跨 Task 共享可变状态。
- `Channel` = `BlockingQueue`（线程安全）。
- `CollectSink` 用 `synchronizedList`（多 sink subtask 并发写，已有）。
- `StreamGraph`/`ExecutionGraph` 构建期单线程，构建后只读（构建后不变）。
- `RebalancePartitioner` 的轮询计数器：每个上游 subtask 独立持有（Task 内单线程访问），无需同步。

## 8. API 扩展

- `DataStream<T> setParallelism(int)`：设置 `Transformation.parallelism`（默认 1）。
- `<K> DataStream<T> keyBy(KeySelector<T, K>)`：将后续边标记为 hash 分区。阶段②下游无 keyed state，仅按 key 路由（为阶段③铺路）。

> `KeySelector<T, K>` 新增函数式接口（`K getKey(T) throws Exception`）。

## 9. 测试策略

- 每个 TDD 任务配 JUnit5 单测。
- **通道/Task 单测**：用 `ListCollector` 式辅助 + 小容量 Channel 验证 send/receive/EOB。
- **引用计数对齐单测**：构造 fan-in（2 上游 → 1 下游），验证下游只在收到 2 个 EOB 后才退出且不丢数据。
- **分区器单测**：forward 一对一、hash 同 key 同通道、rebalance 轮询。
- **算子链单测**：同并行度 forward 链化后单 Task 多算子、链内不跨 Channel。
- **ExecutionGraph.from 两分支单测**（final review flag）：sink 数异常、回溯异常。
- **端到端验收**：多并行度示例（source parallelism=2 + rebalance + map + sink），验证并行处理 + CollectSink 汇总结果正确、无丢失无重复。

## 10. 增量构建阶段（任务预览，writing-plans 阶段细化成 step）

| # | 任务 | 关键产出 |
|---|---|---|
| 1 | `StreamElement` + `Record<T>` | 通道统一元素 |
| 2 | `Channel`（有界 + send/receive + EOB 哨兵常量） | 反压通道 |
| 3 | `ChannelWriter`（`Collector` 实现，写 Record 到 Channel） | 稳定边界适配 |
| 4 | `OperatorChain` + `ChainCollector`（链内 forward） | 算子链核心 |
| 5 | `Partitioner` 接口 + forward/hash/rebalance | 分区策略 |
| 6 | `Task`（`SourceTask` / `OperatorTask`，EOB 引用计数对齐，open/run/close try/finally） | 多线程执行单元 |
| 7 | `ExecutionVertex` + `ExecutionEdge` | 物理计划节点 |
| 8 | `ExecutionGraph.from` 重构（展开 + 链化 + 分区边）+ 两分支单测 | 物理计划构建 |
| 9 | `StreamExecutor` 重构（建 Channel/Task、启动、join） | 多线程调度 |
| 10 | API：`setParallelism` + `keyBy` + `KeySelector` | 用户接口 |
| 11 | list 封装加固（`StreamGraph`/`CollectSink` → `unmodifiableList`） | final review flag |
| 12 | 端到端验收：多并行度分区示例 + 文档 | 阶段②验收 |

> 任务数与边界在 writing-plans 阶段最终确定（可能合并/拆分）。

## 11. 风险与权衡

- **关闭语义的并发正确性**是阶段②最大风险：引用计数 + EOB 必须仔细实现并充分测试（fan-in 对齐、fan-out 广播）。写计划时 Task 任务的测试要覆盖这些边界。
- **算子链的链化逻辑**（ExecutionGraph.from）较复杂：判断可链化（同并行度 + forward + 类型匹配），计划阶段要给清晰的链化规则。
- **`CollectionSource` 并行分片**：source parallelism>1 时如何把 Iterable 数据分给多个 source subtask（按索引取模或分段）。计划阶段定（推荐按索引取模：元素 i → subtask i % parallelism）。
- **线程异常传播**：任一 Task 异常应让作业失败、其他 Task 停止。`StreamExecutor` 需收集并传播异常（计划阶段定机制，如 UncaughtExceptionHandler 或 Future）。
