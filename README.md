# Mini-Flink

> 学习用的简化版 [Apache Flink](https://flink.apache.org/) 流处理框架 —— 用最少的代码理解 Flink 的核心机制：**有状态流处理、事件时间与窗口、基于 barrier 的容错**。

不是生产框架，而是一座"可拆可改"的学习脚手架：分层与命名贴近真实 Flink，单进程多线程模拟（每 subtask 一个 `Thread`，内存 `BlockingQueue` 交换数据），纯 JDK 实现，不引入依赖掩盖原理。

---

## 当前状态

采用 5 个递进的增量阶段构建，**全部完成**：

| 阶段 | 内容 | 状态 |
|---|---|---|
| **① 骨架** | 四层分层、`source → map/flatMap/filter → sink` 执行链 | ✅ |
| **② 并行** | 多线程 Task、有界队列反压、算子链、分区器（forward/hash/rebalance）、EOB 引用计数关闭 | ✅ |
| **③ 状态** | keyed state（`ValueState`/`ListState`/`MapState`）、`keyBy`、`StateBackend` 统一注入 | ✅ |
| **④ 时间/窗口** | event time、watermark、`TimerService`、滚动/滑动窗口、`WindowOperator` | ✅ |
| **⑤ 容错** | barrier 对齐（Chandy-Lamport）、状态快照、**自动 failover**（exactly-once）、window timer 持久化 | ✅ |

**102 个测试全绿**，6 个可运行示例覆盖全部能力。

## 快速开始

**环境**：JDK 17 + Maven。

```bash
# 编译
mvn compile

# 运行全部测试（102 个）
mvn test

# 运行某个示例（见下表）
mvn compile exec:java -Dexec.mainClass="org.miniflink.examples.WindowExample"
# 或
mvn -q compile && java -cp target/classes org.miniflink.examples.WindowExample
```

### 可运行示例

| 示例 | 演示 |
|---|---|
| `TextProcessingExample` | ① 骨架：source→map→filter→sink |
| `ParallelExample` | ② 并行：多 subtask + forward/rebalance 分区 |
| `WordCountExample` | ③ keyed state：keyBy + per-key 累加 |
| `WindowExample` | ④ 窗口：event time + watermark + 滚动窗口聚合 |
| `CheckpointExample` | ⑤ 容错：周期 checkpoint + 故障自动恢复（exactly-once） |
| `CustomSourceSinkExample` | 自定义扩展：`SourceFunction` + `SinkFunction` |

## 一个最小作业

```java
StreamExecutionEnvironment env = new StreamExecutionEnvironment();
CollectSink<String> sink = new CollectSink<>();

env.fromCollection(List.of("hello world", "hi there", "go"))
   .flatMap((line, out) -> { for (String w : line.split(" ")) out.collect(w); })  // 分词
   .filter(w -> w.length() > 2)        // 过滤短词
   .map(String::toUpperCase)           // 转大写
   .addSink(sink::add);

env.execute("text-processing");
System.out.println(sink.getResults()); // [HELLO, WORLD, THERE]
```

### 能力组合示例（窗口 + 容错）

```java
env.fromCollection(events)
   .assignTimestampsAndWatermarks(                     // ④ event time + watermark
       WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofMillis(0), e -> e.ts))
   .keyBy(e -> e.key)                                  // ③ keyBy 分区
   .window(TumblingEventTimeWindows.of(Duration.ofSeconds(1)))  // ④ 滚动窗口
   .reduce((a, b) -> merge(a, b))                      // ③ 窗口内增量聚合
   .addSink(sink::add);
env.execute("window-job");                             // ⑤ enableCheckpointing 可加容错
```

## 架构

四层纵向分层 + 横切关注点（对应真实 Flink）：

```
API 层          DataStream / KeyedStream / WindowedStream + 函数接口        用户写作业
StreamGraph 层  Transformation 线性链（逻辑 DAG）                            用户代码编译
ExecutionGraph  ExecutionVertex[subtask] + ExecutionEdge + 分区器            按并行度展开
Runtime 层      Task / Operator / OperatorChain / Channel / InputGate        线程与算子运行
                StateBackend / RuntimeContext / CheckpointCoordinator
   横切：time（watermark/定时器）· window（窗口分配/触发）
```

**数据流转**：`Record` / `Watermark` / `Barrier` / `EndOfBroadcast` 四种 `StreamElement` 经有界 `Channel` 流动；下游 `InputGate` 聚合多上游 + barrier 对齐；有界队列满则反压。

👉 **完整架构详解**（分层、核心抽象、三视角数据流转、checkpoint/failover 机制、并发模型）见 **[docs/architecture.md](docs/architecture.md)**。

## 项目结构

```
src/main/java/org/miniflink/
├── api/              # DataStream / StreamExecutionEnvironment / KeyedStream / WindowedStream + 函数接口
├── connector/        # 内置 CollectionSource / CollectSink
├── graph/            # Transformation 体系 + StreamGraph（逻辑 DAG）
├── execution/        # ExecutionGraph（物理计划）+ Forward/Hash/Rebalance 分区器
├── runtime/          # Task/Operator/OperatorChain/Channel/InputGate/Output
│                     #   Record/Watermark/Barrier/EndOfBroadcast（StreamElement）
│                     #   StateBackend/MemoryStateBackend + 3 种 state
│                     #   RuntimeContext / StreamExecutor（失败关闭 + failover 循环）
│                     #   CheckpointCoordinator/SubtaskSnapshot/Checkpoint（容错）
│   └── checkpoint/   # WindowOperatorState（算子级快照）
├── time/             # WatermarkStrategy / InternalTimerService / TimestampAssigner
├── window/           # Window/TimeWindow/WindowAssigner/Trigger/EventTimeTrigger
└── examples/         # 6 个可运行示例
```

## 文档

- **[架构文档](docs/architecture.md)** —— 分层设计 + 核心抽象 + 数据流转 + 关键机制（首读推荐）
- [整体设计 spec](docs/superpowers/specs/2026-07-10-mini-flink-design.md) —— 范围、架构、5 阶段规划
- 各阶段设计：`docs/superpowers/specs/2026-07-1*-mini-flink-stage*-design.md`
- 各阶段实现计划：`docs/superpowers/plans/2026-07-1*-mini-flink-stage*.md`
- 示例解读：`docs/examples/*.md`

## 技术栈

Java 17（records / 模式匹配）· Maven · JUnit 5 · 纯 JDK（不引入额外依赖，不掩盖原理）

## 已知简化（学习范围外，推迟）

多并行 watermark min 对齐、会话窗口、`allowedLateness`、`MapState.remove`、`InternalTimerService.currentWatermark` 持久化 —— 见架构文档 §7。
