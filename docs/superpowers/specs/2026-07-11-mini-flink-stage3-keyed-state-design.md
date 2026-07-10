# Mini-Flink 阶段③（keyed state + 聚合）设计补充

- **日期**：2026-07-11
- **状态**：已通过 brainstorming，待编写实现计划
- **关系**：细化 [整体设计](2026-07-10-mini-flink-design.md) 的阶段③部分（spec 第 4.5 节 keyed state）。

## 1. 背景与目标

阶段①（骨架）+ 阶段②（多线程并行）已完成在 `main`：四层分层、单线性链/多线程 Task、有界 Channel 反压、算子链、三分区器（含 keyBy hash 分区）、EOB 引用计数关闭。

阶段③在 keyBy 基础上加 **keyed state（per-key 状态）+ 聚合算子**，让框架能做有状态流处理（词频统计、累加聚合）。keyBy 已实现分区（同 key 路由到同一 subtask），阶段③让该 subtask 维护 per-key 状态。

### 设计原则（延续）

- **对照学习**：贴近真实 Flink（RuntimeContext、StateBackend、ValueState/KeyedStream）。
- **稳定边界**：`Collector` 接口不变。
- **YAGNI**：checkpoint/状态快照留阶段⑤；定时器/KeyedProcessFunction 超出阶段③范围。

## 2. 范围

### 包含

- 三种 keyed state：`ValueState<T>`、`ListState<T>`、`MapState<K,V>`。
- `StateBackend` 接口 + `MemoryStateBackend`（per-subtask 内存存储）。
- `RuntimeContext`（统一注入算子，提供 subtask 信息 + keyed state 访问）。
- `KeyedStream<K,T>`（keyBy 返回）+ `reduce(ReduceFunction)` + `sum`（便捷）。
- `ReduceOperator`（keyed 聚合算子，running reduce）。
- `Operator.open(Collector, RuntimeContext)` 签名统一。
- 验收：带计数的词频统计 WordCount。

### 不包含（留后续）

- `KeyedProcessFunction` + 定时器（与阶段④时间服务耦合）。
- 状态快照/checkpoint（阶段⑤）。
- 增量聚合 `AggregateFunction`（阶段③用 `ReduceFunction` 已够；AggregateFunction 留后续）。
- RocksDB 状态后端（仅内存）。

## 3. 已确认的关键决策

| 决策 | 选择 | 备注 |
|---|---|---|
| state 接口范围 | **ValueState + ListState + MapState**（完整三种） | spec 第 4.5 节既定 |
| state 注入方式 | **RuntimeContext 统一注入** | `Operator.open(Collector, RuntimeContext)`，所有算子适配 |
| keyBy 返回类型 | **KeyedStream<K,T>** | keyed 操作类型安全，reduce/sum 在 KeyedStream 上 |
| state 后端 | **MemoryStateBackend**（内存） | per-subtask；checkpoint 留阶段⑤ |
| reduce 语义 | **running reduce** | 每条输入输出当前累加结果（Flink 行为） |

## 4. 核心抽象

### 4.1 `StateBackend` + `MemoryStateBackend`（runtime）

```java
public interface StateBackend {
    <T> ValueState<T> getValueState(String name);
    <T> ListState<T> getListState(String name);
    <K, V> MapState<K, V> getMapState(String name);
}
```

`MemoryStateBackend` per-subtask：内部 `Map<stateName, Map<key, Object>>`。创建的 state 句柄共享 backend 的存储 + 通过 `RuntimeContext.currentKey` 寻址。

### 4.2 三种 state 句柄（runtime）

```java
public interface ValueState<T> { T value(); void update(T v); }
public interface ListState<T>  { Iterable<T> get(); void add(T v); void clear(); }
public interface MapState<K,V> { V get(K k); void put(K k, V v); Iterable<Map.Entry<K,V>> entries(); void clear(); }
```

实现（如 `ValueStateImpl`）持有 `StateBackend` + stateName + `RuntimeContext`；`value()`/`update()` 用 `ctx.getCurrentKey()` 寻址 backend。

### 4.3 `RuntimeContext`（runtime）

```java
public interface RuntimeContext {
    int getSubtaskIndex();
    int getParallelism();
    <K> K getCurrentKey();
    void setCurrentKey(Object key);
    StateBackend getStateBackend();          // 算子用它取 state 句柄
    KeySelector<?, ?> getKeySelector();      // keyed 算子用（普通算子可为 null）
}
```

`RuntimeContextImpl` 持有 subtaskIndex/parallelism/KeySelector/StateBackend/currentKey。

### 4.4 `KeyedStream`（api）

```java
public class KeyedStream<T, K> {
    private final DataStream<T> dataStream;
    private final KeySelector<T, K> keySelector;
    public DataStream<T> reduce(ReduceFunction<T> reduceFn) { ... }  // 建 hash+keySelector 的 reduce transformation
    // sum 是 reduce 的便捷委托（用户传求和函数，如 Integer::sum）；通用聚合用 reduce
    public DataStream<T> sum(ReduceFunction<T> sumFn) { return reduce(sumFn); }
}
```

`DataStream.keyBy(KeySelector)` 改为返回 `KeyedStream<K,T>`（内部仍设 hash partitioner）。

### 4.5 `ReduceFunction` + `ReduceOperator`

```java
@FunctionalInterface
public interface ReduceFunction<T> { T reduce(T a, T b) throws Exception; }
```

`ReduceOperator<IN>` implements `Operator<IN, IN>`：
- `open(out, ctx)`：保存 ctx，从 `ctx.getStateBackend().getValueState("reduce-acc")` 取累加器 state。
- `processElement(record)`：`currentKey = keySelector.getKey(record); ctx.setCurrentKey(currentKey); IN acc = valueState.value(); IN reduced = (acc==null) ? record : reduceFn.reduce(acc, record); valueState.update(reduced); out.collect(reduced);`

## 5. state 集成 + currentKey 机制

- `StreamExecutor` 为每个 vertex 建 per-subtask `RuntimeContextImpl`（新建 `MemoryStateBackend` + 从 `OneInputTransformation.getKeySelector()` 取 KeySelector + subtaskIndex + parallelism）。
- `Task` open 算子时传入 RuntimeContext。
- keyed 算子 processElement 时设 currentKey，state 句柄据此寻址。**同 key 恒落同一 subtask（keyBy hash 保证），故 per-subtask state 内的 per-key map 不会跨 subtask 分裂。**

## 6. Operator.open 签名统一（影响所有算子）

`Operator.open(Collector, RuntimeContext)` 取代 `open(Collector)`。适配范围：
- `MapOperator`/`FilterOperator`/`FlatMapOperator`/`SinkOperator`：open 加 ctx 参数，忽略。
- `SourceOperator.open(Collector, RuntimeContext)`：用 ctx.getSubtaskIndex()/getParallelism() 替换阶段②的 `open(Collector, int, int)`（RuntimeContext 统一承载）。
- `OperatorChain.open(Collector, RuntimeContext)`：透传 ctx 给链内算子。
- `SourceTask`/`OperatorTask`：open 时传 RuntimeContext。

> 这是一次大范围签名调整，但统一了上下文注入（也为阶段⑤ checkpoint 的 StateBackend 访问铺路）。各算子的全量回归保证阶段①②不破坏。

## 7. final review flag 闭合（阶段②遗留）

阶段② final review flag："算子 copy() 浅拷贝 + keyed state 后算子持有可变状态"。阶段③设计直接闭合：

- `ReduceOperator.copy()` 仍只共享无状态的 `ReduceFunction`。
- state（ValueState）在 `open()` 内从 per-subtask `RuntimeContext`/`StateBackend` 获取，每个 subtask 独立。
- 故多 subtask 间 state 隔离，copy 浅拷贝安全。✓

## 8. 测试 + 验收

- 每任务 TDD：StateBackend/三种 state/RuntimeContext/currentKey/KeyedStream/ReduceOperator 各自单测。
- currentKey 单测：设 key1 → state.value()==A；设 key2 → state.value()==null（隔离）。
- **验收 WordCount**：`source → flatMap(分词) → map(word → (word,1)) → keyBy(word) → reduce((a,b) → (a.word, a.count+b.count)) → sink`，多 key 并发累加正确。

## 9. 任务预览（writing-plans 细化）

| # | 任务 |
|---|---|
| 1 | `StateBackend` + 三种 state 接口 + `MemoryStateBackend` + state 句柄实现 + 单测 |
| 2 | `RuntimeContext` + `RuntimeContextImpl`（currentKey 机制）+ 单测 |
| 3 | `Operator.open(Collector, RuntimeContext)` 签名统一 + 所有算子/OperatorChain/Task/SourceOperator 适配 + 全量回归 |
| 4 | `ReduceFunction` + `ReduceOperator`（currentKey + ValueState 累加）+ 单测 |
| 5 | `KeyedStream`（keyBy 返回）+ `reduce`/`sum` API + 单测 |
| 6 | 验收：WordCount + 文档 |

## 10. 风险与权衡

- **Operator.open 签名统一**改动面大（所有算子 + OperatorChain + Task + SourceOperator）。任务 3 必须保证全量回归（阶段①② 48 测试）不破。
- **currentKey 寻址**：state 句柄依赖 `RuntimeContext.currentKey`，keyed 算子必须在每次 processElement 设 currentKey。普通算子不设 currentKey（其 RuntimeContext 的 KeySelector 为 null，但也不访问 keyed state）。单测覆盖 key 隔离。
- **reduce 输出量**：running reduce 每条输入输出一条，下游吞吐 = 输入吞吐（无聚合压缩）。学习项目可接受（Flink 行为一致）。
- **MapState.entries() 遍历**：在并发/大 key 空间下需注意，阶段③单 subtask 内单线程访问，安全。
