# Mini-Flink

> 学习用的简化版 [Apache Flink](https://flink.apache.org/) 流处理框架 —— 用最少的代码理解 Flink 的核心机制：有状态流处理、事件时间与窗口、基于 barrier 的容错。

不是生产框架，而是一座"可拆可改"的学习脚手架：分层与命名贴近真实 Flink，每完成一个阶段都能跑通一个新例子。

---

## 当前状态

采用 5 个递进的增量阶段构建，**阶段①（骨架）已完成**：

| 阶段 | 内容 | 状态 |
|---|---|---|
| **① 骨架** | 四层分层、`source → map/flatMap/filter → sink` 单线程同步执行链 | ✅ 已完成 |
| ② 并行 | 多线程 Task、分区器（forward/hash/rebalance）、有界队列反压 | ⏳ 规划中 |
| ③ 状态 | keyed state、`keyBy` 聚合 | ⏳ 规划中 |
| ④ 时间/窗口 | watermark、event time、滚动/滑动/会话窗口 | ⏳ 规划中 |
| ⑤ 容错 | checkpoint barrier 对齐、状态快照与恢复 | ⏳ 规划中 |

## 快速开始

**环境**：JDK 17 + Maven。

```bash
# 编译
mvn compile

# 运行全部测试（12 个）
mvn test

# 运行验收示例（打印流处理结果）
mvn compile exec:java
```

示例输出：

```
输入        : ["hello world", "hi there", "go"]
处理结果    : [HELLO, WORLD, THERE]
预期        : [HELLO, WORLD, THERE]
```

`hello world hi there go` → 分词 → 过滤 `hi`/`go`（长度 ≤ 2）→ 转大写。

## 一个最小作业

```java
StreamExecutionEnvironment env = new StreamExecutionEnvironment();
CollectSink<String> sink = new CollectSink<>();

env.fromCollection(List.of("hello world", "hi there", "go"))
   .<String>flatMap((line, out) -> { for (String w : line.split(" ")) out.collect(w); })  // 分词
   .filter(w -> w.length() > 2)        // 过滤短词
   .map(String::toUpperCase)           // 转大写
   .addSink(sink::add);                // 收集结果

env.execute("text-processing");
System.out.println(sink.getResults()); // [HELLO, WORLD, THERE]
```

可运行的完整版本见 `src/main/java/org/miniflink/examples/TextProcessingExample.java`。

## 架构

四层分层（对应真实 Flink）+ 四个横切关注点：

```
┌───────────────────────────────────────────────────┐
│  API 层     DataStream + 算子方法 + 函数接口          │  用户写作业
├───────────────────────────────────────────────────┤
│  StreamGraph 层   逻辑 DAG（Transformation）         │  用户代码编译出的逻辑图
├───────────────────────────────────────────────────┤
│  ExecutionGraph 层  物理执行计划（含并行度 + 分区器）  │  逻辑节点切成 subtask
├───────────────────────────────────────────────────┤
│  Runtime 层   Task / Operator / 内存通道             │  线程与算子实际运行
└───────────────────────────────────────────────────┘
   横切：State · Time · Window · Checkpoint
```

阶段①的执行模型是**单线程同步链**：`source.run()` 产生的每条数据，经 `OperatorOutput` 同步流过算子链直达 sink。后续阶段②会把算子放进多线程 Task + 内存队列。

## 项目结构

```
src/main/java/org/miniflink/
├── api/              # DataStream、StreamExecutionEnvironment、函数接口
├── connector/        # 内置 CollectionSource / CollectSink
├── graph/            # Transformation 体系 + StreamGraph（逻辑 DAG）
├── execution/        # ExecutionGraph（物理计划：单线性链）
├── runtime/          # Collector/Operator/SourceOperator 接口、算子、OperatorOutput、StreamExecutor
└── examples/         # 可运行的验收示例
```

> `state` / `time` / `window` / `checkpoint` 包将在阶段 ③④⑤ 陆续加入。

## 文档

- [设计文档（spec）](docs/superpowers/specs/2026-07-10-mini-flink-design.md) —— 范围、架构、5 阶段规划
- [阶段①实现计划](docs/superpowers/plans/2026-07-10-mini-flink-stage1-skeleton.md) —— 8 个 TDD 任务
- [文本处理示例说明](docs/examples/text-processing.md) —— 端到端执行流程解读

## 技术栈

Java 17 · Maven · JUnit 5 · 纯 JDK（不引入额外依赖，不掩盖原理）
