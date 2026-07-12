# 独立 state/checkpoint 顶层包设计

- 日期：2026-07-13
- 状态：已批准（待实现）
- 类型：包结构重组（纯移动，不改 API 与行为）

## 概述

将散落在 `org.miniflink.runtime` 根目录的状态后端类与快照数据结构类，独立成 `org.miniflink.state` 与 `org.miniflink.checkpoint` 两个顶层包。对齐 Flink 的 state/checkpoint 分层，并与 mini-flink 既有 `time`/`window` 顶层包约定一致。

## 背景与问题

当前 `org.miniflink.runtime` 根目录约 50 个类，混杂三类职责：

1. **状态后端体系**：`StateBackend`、`MemoryStateBackend`、`Value/List/MapState`(+`Impl`)、`OperatorState`
2. **快照/checkpoint**：`StateSnapshot`、`SubtaskSnapshot`、`Checkpoint`、`CheckpointCoordinator`、`SnapshotCallback`、`Barrier`，外加已存在的 `runtime/checkpoint/WindowOperatorState`
3. **执行核心**：`Operator*`、`Channel*`、`Task`、`StreamExecutor`、`RuntimeContext*` 等

状态后端、状态句柄、算子状态、快照数据结构、checkpoint 协调器都堆在 runtime 根目录，职责边界模糊，难以独立理解。

Flink 在 `flink-runtime` 模块下用 `runtime.state` 与 `runtime.checkpoint` 两个并列子包组织这些类。本次重构借鉴该分层，但采用顶层包形式（见 D1）。

## 目标

- 把 state 相关类聚合到 `org.miniflink.state` 顶层包
- 把 checkpoint 相关类聚合到 `org.miniflink.checkpoint` 顶层包
- 依赖方向清晰、无环
- 行为零变化（编译 + 现有测试全绿）

## 非目标（YAGNI）

- 不引入 `StateDescriptor`
- 不拆分面向用户的 state API（如 `api.state`）
- 不改任何接口定义或方法签名
- 不引入 `CheckpointTrigger` 接口抽象（消除反向依赖的潜在接口，留待未来 API 演进）

## 关键决策与理由

### D1：顶层两个包（`org.miniflink.state` + `org.miniflink.checkpoint`）

mini-flink 既有约定把子系统作为顶层包（`time`、`window`）。Flink 虽把 state/checkpoint 放 runtime 模块下，但 Flink 的 runtime 是独立 Maven 模块，而 mini-flink 是单模块、runtime 是一个包。遵循项目既有顶层包约定，一致性优先。

### D2：纯包重组

仅改 `package` 声明与 `import`，不改 API。最小、安全、可由编译 + 测试严格验证。API 演进（StateDescriptor 等）留待后续独立工作。

### D3：交界类归属

- **`StateSnapshot` → state**：`StateBackend.snapshot()` 返回它、`restore()` 入参它。若放 checkpoint 包，则 state→checkpoint 反向依赖，与 checkpoint→state 成环。依赖方向锁定。
- **`OperatorState`（接口）→ state**：算子状态属状态域，Flink 同样放 `runtime.state`。
- **`WindowOperatorState` → checkpoint**：算子快照数据，对齐 Flink 的 `OperatorSubtaskState` 在 checkpoint 包，与 `SubtaskSnapshot.operatorStates` 配合。
- **`Barrier` → runtime（留）**：`implements StreamElement`，与 `Record`/`Watermark`/`EndOfBroadcast` 同属数据流元素家族；被 6 个 runtime 核心类使用（`InputGate`/`OperatorTask`/`Output`/`SourceTask`/`StreamElement`/`Task`）；对齐 Flink 的 `CheckpointBarrier` 在 `runtime.io` 包。
- **`SnapshotCallback` → runtime（留）**：仅 `InputGate` 使用，是 IO 层与 task 间的回调契约。

### D4：CheckpointCoordinator 留 runtime（无反向依赖）

`CheckpointCoordinator` 持有 `List<SourceTask>` 并调 `requestCheckpoint()`。若移入 checkpoint 包，则 checkpoint→runtime 反向依赖，纯重组无法消除（需引入触发接口，属 API 演进）。将其留 runtime（与 `StreamExecutor` 同属执行期编排），checkpoint 包只含纯快照数据结构，依赖仅 state，三层单向无环。

### 依赖图

```
state ◀── checkpoint          （checkpoint 仅依赖 state）
  ▲           ▲
  └── runtime ┘               （runtime 依赖 state + checkpoint，无环）
```

- `state`：零外部依赖
- `checkpoint → state`：`SubtaskSnapshot`/`WindowOperatorState` 引用 `StateSnapshot`/`OperatorState`
- `runtime → state`、`runtime → checkpoint`：无反向依赖

## 包结构与边界

### `org.miniflink.state`（零外部依赖）

| 类 | 角色 |
|---|---|
| `StateBackend` | 状态后端接口 |
| `MemoryStateBackend` | 内存状态后端实现 |
| `ValueState` / `ValueStateImpl` | keyed value state |
| `ListState` / `ListStateImpl` | keyed list state |
| `MapState` / `MapStateImpl` | keyed map state |
| `OperatorState` | 算子状态标记接口 |
| `StateSnapshot` | keyed state 快照数据 |

> 10 个类相互引用全在同包，移动后**无需新增 import**（仅改 package 声明）。

### `org.miniflink.checkpoint`（依赖 state）

| 类 | 角色 |
|---|---|
| `Checkpoint` | 作业级快照（checkpointId + 各 subtask 快照） |
| `SubtaskSnapshot` | subtask 快照（keyed state + source offset + operator states） |
| `WindowOperatorState` | WindowOperator 算子快照（timers + windows） |

### `org.miniflink.runtime`（依赖 state + checkpoint，留）

执行编排（`StreamExecutor`、`CheckpointCoordinator`、`SourceTask`、`OperatorTask`、`OperatorChain`…）、数据流/IO（`StreamElement`、`Record`、`Watermark`、`Barrier`、`EndOfBroadcast`、`Channel`/`InputChannel`/`InputGate`、`Output`/`OutputCollector`、`SnapshotCallback`）、算子（`operator/*`）、上下文（`RuntimeContext*`/`SourceContext*`）。

## 迁移清单

### 移动 main 类（13）

**→ `org.miniflink.state`（10，仅改 package 声明，无需新增 import）**：
`StateBackend`、`MemoryStateBackend`、`ValueState`、`ValueStateImpl`、`ListState`、`ListStateImpl`、`MapState`、`MapStateImpl`、`OperatorState`、`StateSnapshot`

**→ `org.miniflink.checkpoint`（3）**：
- `Checkpoint`：改 package（`SubtaskSnapshot` 同包，无需新 import）
- `SubtaskSnapshot`：改 package + 新增 `import org.miniflink.state.StateSnapshot`、`import org.miniflink.state.OperatorState`
- `WindowOperatorState`：从 `org.miniflink.runtime.checkpoint` 升为 `org.miniflink.checkpoint`；`import org.miniflink.runtime.OperatorState` → `import org.miniflink.state.OperatorState`

### 改 import 的 runtime 类（9）

| 类 | 新增 import |
|---|---|
| `CheckpointCoordinator` | `checkpoint.Checkpoint`、`checkpoint.SubtaskSnapshot` |
| `SourceTask` | `state.StateSnapshot`、`checkpoint.SubtaskSnapshot` |
| `OperatorTask` | `state.StateSnapshot`、`state.OperatorState`、`checkpoint.SubtaskSnapshot` |
| `StreamExecutor` | `checkpoint.Checkpoint`、`checkpoint.SubtaskSnapshot` |
| `RuntimeContext` | `state.StateBackend` |
| `RuntimeContextImpl` | `state.StateBackend`、`state.MemoryStateBackend` |
| `Operator` | `state.OperatorState` |
| `OperatorChain` | `state.OperatorState` |
| `WindowOperator` | `checkpoint.WindowOperatorState`、`state.OperatorState` |

### 移动测试（4，镜像 main 包结构）

- → `state`：`StateSnapshotTest`、`MemoryStateBackendTest`
- → `checkpoint`：`SubtaskSnapshotTest`、`WindowOperatorStateTest`（从 `runtime/checkpoint` 升顶层）
- 留 `runtime`（仅改 import）：`CheckpointCoordinatorTest`、`BarrierTest`、`InputGateTest`、`FailoverRecoveryTest`、`FailureCloseTest`、`SourceContextCheckpointOffsetTest` 等
- 留 `window`（仅改 import）：`WindowFailoverTest`

### 文档同步

`docs/architecture.md`（提及包名/分层/StateBackend/CheckpointCoordinator 处更新为新结构）。

## 验证策略

1. `mvn compile` —— 所有 import 解析正确
2. `mvn test` —— 现有全部测试通过（行为零变化）
3. grep 确认：
   - runtime 根目录不再含 state/checkpoint 相关类
   - 无残留 `import org.miniflink.runtime.StateBackend` / `OperatorState` / `StateSnapshot` / `Checkpoint` / `SubtaskSnapshot` 等旧引用
   - 无残留 `org.miniflink.runtime.checkpoint` 旧包引用

## 决策摘要

| 决策点 | 选择 | 理由 |
|---|---|---|
| 包层级 | 顶层两个包 | 遵循项目 `time`/`window` 顶层包约定 |
| 工作范围 | 纯包重组 | 最小、可严格验证 |
| `OperatorState` 接口 | state | 对齐 Flink，算子状态属状态域 |
| `WindowOperatorState` | checkpoint | 算子快照数据，对齐 `OperatorSubtaskState` |
| `StateSnapshot` | state | `StateBackend` 契约，依赖方向锁定 |
| `CheckpointCoordinator` | runtime（留） | 避免 checkpoint→runtime 反向依赖 |
| `Barrier` / `SnapshotCallback` | runtime（留） | 数据流元素 / IO 回调，对齐 Flink io |
