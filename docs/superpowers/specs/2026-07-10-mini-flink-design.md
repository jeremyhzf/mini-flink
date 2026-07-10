# Mini-Flink 设计文档

- **日期**：2026-07-10
- **状态**：已通过 brainstorming，待编写实现计划
- **作者**：huzhengfa

## 1. 背景与目标

本项目是一个**简化版的 Apache Flink 流处理框架**，目的是**学习**——通过亲手实现，理解 Flink 区别于普通流处理框架的核心机制：有状态计算、事件时间与窗口、以及基于 barrier 的容错。

设计原则：

- **对照学习**：分层与命名贴近真实 Flink，学到的概念可直接迁移到 Flink 源码。
- **增量可见**：按端到端垂直切片推进，每个阶段都能跑、能测。
- **原理优先**：尽量用纯 JDK 实现，不引入依赖掩盖机制。
- **YAGNI**：只实现学习目标范围内的功能，不追求生产可用。

## 2. 学习目标（功能范围）

项目分三个递进层次，全部纳入范围：

1. **核心流处理模型**：DataStream API、逻辑 DAG、基本算子、source/sink。
2. **状态与窗口**：keyed state、滚动/滑动/会话窗口、event time/processing time、watermark。
3. **容错与并行**：并行执行（单进程多线程）、checkpoint barrier 对齐、状态后端持久化、从检查点恢复。

### 非目标（明确不做）

- 多进程/分布式部署（网络栈、跨进程 RPC）。
- 真实外部系统对接（Kafka、HDFS 等），仅用内置 source/sink。
- Flink SQL / Table API / CEP 复杂事件处理。
- 高级调度、动态扩缩容、增量 checkpoint、unaligned checkpoint。
- 生产级性能优化与监控。

## 3. 技术基线

| 项 | 选择 | 说明 |
|---|---|---|
| 语言 | Java | 与原版 Flink 一致，可对照源码 |
| 构建工具 | Maven | Flink 同款 |
| JDK | 17（LTS） | 用到 record、sealed、模式匹配 |
| 第三方依赖 | 尽量纯 JDK | 测试仅用 JUnit 5 |
| 状态后端 | 内存运行 + 文件快照 | 运行态在内存，checkpoint 序列化到本地文件 |
| 部署形态 | 单进程多线程模拟 | 一个 JVM 内多 Task 线程，内存队列交换数据 |

## 4. 分层架构

```
┌───────────────────────────────────────────────────┐
│  API 层     DataStream + 算子方法 + 函数接口          │  用户写作业
├───────────────────────────────────────────────────┤
│  StreamGraph 层   逻辑 DAG（StreamNode / StreamEdge）│  用户代码编译出的逻辑图
├───────────────────────────────────────────────────┤
│  ExecutionGraph 层  物理执行计划（含并行度 + 分区器）  │  逻辑节点切成 subtask
├───────────────────────────────────────────────────┤
│  Runtime 层   Task / Operator / 内存通道             │  线程与算子实际运行
└───────────────────────────────────────────────────┘
   横切关注点：State（状态）· Time（时间）· Window（窗口）· Checkpoint（容错）
```

### 4.1 API 层（`org.miniflink.api`）

用户构建和提交作业的入口。

- **`StreamExecutionEnvironment`**：作业入口，持有 transformation 列表；`execute(jobName)` 触发「编译 → 物理计划 → 运行」。
- **`DataStream<T>`**：不可变的流抽象，链式调用算子方法返回新的 `DataStream`。
- **算子方法**：`map`、`flatMap`、`filter`、`keyBy`、`window`、`reduce`、`union`、`sink`。
- **函数接口**（仿 Flink 函数式接口）：`MapFunction<T,O>`、`FlatMapFunction<T,O>`、`FilterFunction<T>`、`ReduceFunction<T>`、`KeySelector<T,K>`、`WindowFunction` 等。

### 4.2 StreamGraph 层（`org.miniflink.graph`）

用户代码编译出的逻辑 DAG。

- **`Transformation<T>`**：一次算子调用产生的一个逻辑变换节点。
- **`StreamNode`**：逻辑节点，记录算子类型、用户函数、并行度、keySelector、分区器。
- **`StreamEdge`**：逻辑边，连接上游输出与下游输入。
- **`StreamGraph`**：完整逻辑图，由 `StreamExecutionEnvironment` 在 `execute()` 时编译而成。

### 4.3 ExecutionGraph 层（`org.miniflink.execution`）

把逻辑图按并行度展开成可执行的物理计划。

- **`ExecutionVertex`**：一个 subtask（一个 StreamNode 按并行度切出的一份）。
- **`ExecutionEdge`**：subtask 间连接，携带分区策略。
- **分区器（Partitioner）**：
  - `ForwardPartitioner`：一对一（用于算子链 / 同并行度直连）。
  - `HashPartitioner`：按 key 哈希（keyBy 用）。
  - `RebalancePartitioner`：轮询分发。
- **`ExecutionGraph`**：物理执行计划，运行时据此为每个 `ExecutionVertex` 创建 `Task`。

### 4.4 Runtime 层（`org.miniflink.runtime`）

真正干活的部分。

- **`Task`**：一个 subtask 的执行单元，一个 `Thread`，持有输入通道、算子实例、输出，跑主循环。
- **`Operator`** 算子基类与实现：`SourceFunctionOperator`、`MapOperator`、`FilterOperator`、`FlatMapOperator`、`KeyedProcessOperator`、`WindowOperator`、`SinkOperator`。
- **`StreamElement`**：在通道里流动的统一元素，封装三种类型（见下）。
- **`Record<T>`**：数据记录，携带事件时间戳。
- **`Watermark`**：水位线，表示 event time 进度。
- **`CheckpointBarrier`**：checkpoint 屏障，随数据流传播。
- **`Input` / `Output`**：基于 `BlockingQueue<StreamElement>` 的内存通道，有界 → 天然反压。

### 4.5 State 横切层（`org.miniflink.state`）

- **Keyed State 接口**：`ValueState<T>`、`ListState<T>`、`MapState<K,V>`。
- **`StateBackend`**：状态后端接口，负责创建和快照状态。
- **实现**：`MemoryStateBackend`（内存持有 + checkpoint 时序列化到本地文件）。

### 4.6 Time 横切层（`org.miniflink.time`）

- **时间语义**：event time（由 watermark 驱动）、processing time（墙钟）。
- **`TimeService`**：向算子提供当前时间、注册定时器（窗口触发用）。

### 4.7 Window 横切层（`org.miniflink.window`）

- **`Window`**：窗口定义（时间区间）。
- **`WindowAssigner`**：把记录分配到一个或多个窗口。
  - `TumblingWindowAssigner`（滚动）、`SlidingWindowAssigner`（滑动）、`SessionWindowAssigner`（会话，按 gap 合并）。
- **`Trigger`**：决定窗口何时触发计算（基于 watermark / 定时器）。
- **`WindowOperator`**：窗口算子，维护每个窗口的状态，触发后输出聚合结果。

### 4.8 Checkpoint 横切层（`org.miniflink.checkpoint`）

- **`CheckpointCoordinator`**：周期性向 source 注入 `CheckpointBarrier`；收集所有算子 ack；管理已完成快照清单。
- **Barrier 对齐（aligned checkpoint）**：多输入算子收到某条边的 barrier 后阻塞该边，等所有上游 barrier 到齐再快照状态、放行后续数据，保证 exactly-once。
- **恢复**：作业失败时，从最近成功 checkpoint 恢复所有 keyed state，重启受影响 subtask。

## 5. 核心数据流

```
用户代码 (DataStream 链式调用)
        │  execute(jobName)
        ▼
   StreamGraph (逻辑 DAG)
        │  按并行度展开
        ▼
   ExecutionGraph (物理计划: ExecutionVertex + 分区器)
        │  为每个 subtask 创建 Task
        ▼
   Task 线程跑算子循环
        │  数据经 BlockingQueue 流动
        ▼
   watermark 随数据传播 → 触发窗口
   barrier 周期注入 → 快照状态
```

## 6. 并行执行模型

- 每个 **subtask = 一个 `Thread`**，运行一个 `Task`。
- Task 之间用**有界 `BlockingQueue<StreamElement>`** 连接；队列满时生产者阻塞 → **天然反压**。
- source 按 parallelism 切分（有界数据按段切，无界 socket 单源广播或按行分发）。
- keyBy 用 `HashPartitioner` 把同一 key 路由到同一 subtask，保证 keyed state 一致性。
- **算子链（Operator Chaining）**：同并行度、Forward 连接的相邻算子合并进同一线程，省去队列中转。作为后期可选优化，首版可不实现。

## 7. 错误处理与优雅关闭

- 算子抛异常 → 标记作业失败 → 停止所有 Task → 从最近成功 checkpoint 恢复状态 → 重启作业（简化版：整体重启，不做细粒度 failover region）。
- 有界 source 结束 + watermark 推进到 `+∞`（`Long.MAX_VALUE`）→ 各算子处理完缓冲数据后逐级关闭 → 作业优雅退出。
- 算子内不允许静默吞异常；向上抛出由作业层统一处理。

## 8. 测试策略

- 每个增量阶段配 **JUnit 5** 单元测试。
- 端到端测试模式：`CollectionSource`（输入一个 List）→ 处理 → `CollectSink`（输出收集进 List）→ 断言结果。
- **checkpoint 测试**：人为注入失败，验证恢复后输出结果满足**恰好一次（exactly-once）**语义——即结果集中每条记录出现且仅出现一次。
- 并行测试：固定并行度，验证 keyBy 后同 key 数据落在同一 subtask、聚合结果正确。

## 9. 项目结构

```
mini-flink/
├── pom.xml
├── src/main/java/org/miniflink/
│   ├── api/          # DataStream、函数接口、StreamExecutionEnvironment
│   ├── graph/        # StreamGraph（逻辑 DAG）
│   ├── execution/    # ExecutionGraph、分区器
│   ├── runtime/      # Task、Operator、StreamElement、Record、Watermark、Barrier、内存通道
│   ├── state/        # StateBackend、ValueState/ListState/MapState
│   ├── time/         # 时钟、时间语义、TimeService
│   ├── window/       # WindowAssigner、WindowOperator、Trigger
│   └── checkpoint/   # CheckpointCoordinator、快照管理
├── src/test/java/org/miniflink/...
└── docs/
    ├── superpowers/specs/   # 本设计文档
    └── examples/            # 每阶段可运行示例 + 原理学习笔记
```

## 10. 增量构建阶段（5 个垂直切片）

每个阶段完成后必须：可运行示例通过 + 对应单元测试通过。

| 阶段 | 目标 | 关键产出 | 可运行示例（验收） |
|---|---|---|---|
| **① 骨架** | source→map→sink，单线程串行跑通；API→StreamGraph→ExecutionGraph→Runtime 分层骨架 | `DataStream`、`StreamExecutionEnvironment`、四层最简实现 | 词频统计（无状态，单并行度） |
| **② 并行** | parallelism、多线程 Task、三种分区器、有界队列反压 | `ExecutionVertex` 切分、`Task` 多线程、`BlockingQueue` 通道 | 多并行度词频统计 |
| **③ 状态** | keyed state、keyBy 聚合、StateBackend | `ValueState`/`ListState`/`MapState`、`HashPartitioner` 落地 | 按 key 累加求和 |
| **④ 时间/窗口** | watermark、event time、滚动/滑动窗口、Trigger | `Watermark`、`WindowAssigner`、`WindowOperator`、`TimeService` | 每分钟窗口聚合（滚动）；滑动窗口示例 |
| **⑤ 容错** | checkpoint barrier 对齐、状态快照、从检查点恢复、会话窗口 | `CheckpointCoordinator`、barrier 对齐、文件快照、恢复；`SessionWindowAssigner` | 注入失败验证 exactly-once |

> 注：会话窗口（依赖 watermark + 状态 + 定时器）放在第 ⑤ 阶段，与前序能力耦合最自然。

## 11. 风险与权衡

- **checkpoint barrier 对齐**是本项目最复杂的部分；首版只做 aligned checkpoint，多输入算子的对齐逻辑需仔细实现并充分测试。
- **线程并发 bug**（数据竞争、死锁）是多线程运行时的主要风险；状态访问需保证线程安全（keyed state 按 key 隔离，Task 内单线程访问）。
- **算子链**若首版不做，会多一层队列开销，但不影响正确性与学习目标，可作为后期优化。
- **会话窗口合并逻辑**较繁琐，若第 ⑤ 阶段时间紧张，可降级为「固定 gap 的简化会话」。

## 12. 后续步骤

本设计经用户审查通过后，调用 `writing-plans` skill 将上述 5 个增量阶段细化为带任务分解、依赖关系和验收标准的实现计划。
