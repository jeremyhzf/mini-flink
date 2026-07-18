# mini-flink 项目演进路径 — 设计文档

- 日期：2026-07-18
- 分支：`docs/evolution-path`（从 `main` @ `702cd22` 切出）
- 状态：待复核

## 1. 背景与目标

mini-flink 是学习用简化版 Flink，2026-07-10 ~ 07-13 共 4 天、90 个提交，按 **5 阶段 + 1 重构** 演进。现有 `docs/architecture.md`（30KB，偏**静态架构**）和 `docs/superpowers/{specs,plans}`（偏**单阶段设计与计划**），缺少一份能让人快速读懂"项目如何一步步长出来"的**演进脉络**文档，以及能在 git 层面**回溯每个阶段代码形态**的锚点。

本次工作产出两件互补的资产：

1. **高层路线图文档** `docs/evolution.md` — 一页纸级的演进导航地图
2. **Git 阶段 tag** — 5 个阶段 + 1 个重构锚点，`checkout` 即可回到该阶段代码

## 2. 范围

- **覆盖**：阶段①骨架 ~ 阶段⑤容错，外加 07-13 的 state/checkpoint 顶层包独立重构。
- **不覆盖**：单阶段的深入设计（已有 specs）、静态架构详解（已有 architecture.md）、示例运行步骤（已有 examples/）。本文只做**脉络串联**，并通过链接指向上述文档。

## 3. 关键设计决策

| 决策点 | 选择 | 理由 |
|---|---|---|
| 产物形式 | 文档 + Git tag | 读写（文档）与回溯（tag）两个角度都覆盖 |
| 文档详略 | 高层路线图（简） | 与 architecture.md 互补不重复；项目脉络信息密度最高 |
| tag 锚点策略 | **策略 A：阶段完成态** | tag 指向该阶段最后一个实质性提交，`checkout` 能看到该阶段**全部成果**（含验收示例），最贴合"路径"语义 |
| 分支 | `docs/evolution-path`，完成后由用户决定合并/PR/保留 | 隔离本次工作；tag 全局存在不绑分支 |
| WIP 处理 | 切分支前 `git stash`（已完成） | tag 与文档基于干净的 `main`，避免 WIP 干扰 |

## 4. Git tag 方案

tag 命名遵循 `stageN-<主题>`，附加一个重构 tag。所有锚点均位于 `main` 主线（`--first-parent`），已逐个核实：

| tag | 锚点 commit | 锚点依据（该阶段最后一个实质性提交）|
|---|---|---|
| `stage1-skeleton` | `72425a0` | ①最后提交（项目 README + 全部①成果：API/图/同步执行）|
| `stage2-parallel` | `59dc91a` | ②final review fix（setParallelism 校验 + 链化降级单测），②主线最后提交 |
| `stage3-keyed-state` | `819a839` | ③WordCount 可运行示例（keyed state 聚合）|
| `stage4-window` | `5ce4358` | ④窗口聚合可运行示例（event time + watermark）|
| `stage5-fault-tolerance` | `45261e8` | 架构文档（含"演进"章节），标志 5 阶段全部成型 |
| `refactor-packages` | `fd3b57e` | 重构完成（同步 state/checkpoint 顶层包结构 + 全量验证）|

> 说明：`42f91bd`（README 反映 5 阶段）不在主线 first-parent 上（位于侧支），故 stage5 改用主线的 `45261e8`。打 tag 后用 `git show <tag>` 逐个验证锚点 commit 正确。

tag 均为**轻量 tag**（lightweight），足以满足"回溯锚点"需求，无需附带说明信息。

## 5. 文档结构（`docs/evolution.md`）

定位：**演进脉络导航图**，与 `architecture.md`（静态架构）正交。约 1 屏/阶段。

### 5.1 顶部：总览 Mermaid 时间线
横向时间轴，5 阶段 + 重构依次排列，每个节点标注：阶段名 / 日期 / tag。一张图概览全貌。

### 5.2 每阶段一节（固定模板）
- **一句话目标**：该阶段解决什么核心问题
- **为何需要**：上一阶段结束时缺少什么能力
- **核心类清单**（5~8 个，实现时用 codegraph 核对现状）
- **代表提交**（1~2 个，带 hash + 一句话）
- **阶段前后能力对比**：一句话说明"做之前 vs 做之后"
- **回溯锚点**：`git checkout stageN-xxx`

各阶段预期核心类（实现时核对）：

| 阶段 | 预期核心类 |
|---|---|
| ① 骨架 | `DataStream`、`StreamExecutionEnvironment`、`Transformation`、`StreamGraph`、`ExecutionGraph`、`MapOperator`、`Collector` |
| ② 并行 | `Channel`、`OperatorChain`、`Task`/`SourceTask`/`OperatorTask`、`Partitioner`、`ExecutionVertex`、`StreamExecutor`（多线程）|
| ③ keyed state | `StateBackend`、`ValueState`/`ListState`/`MapState`、`RuntimeContext`、`KeyedStream`、`ReduceOperator` |
| ④ 时间与窗口 | `Record`(事件时间)、`Watermark`、`WatermarkStrategy`、`TimerService`、`WindowAssigner`、`Trigger`、`WindowOperator` |
| ⑤ 容错 | `Barrier`、`InputGate`/`InputChannel`、`Checkpoint`/`StateSnapshot`、`CheckpointCoordinator`、failover 循环 |
| 重构 | `org.miniflink.state`、`org.miniflink.checkpoint` 顶层包 |

### 5.3 末尾：文档导航关系
明确指向并区分：
- `architecture.md` → 静态分层架构与核心抽象
- `docs/superpowers/specs/` → 每阶段设计细节
- `docs/superpowers/plans/` → 每阶段实现计划
- `docs/examples/` → 各阶段可运行示例

## 6. 执行步骤

1. `git stash`（✅ 已完成）→ `git checkout -b docs/evolution-path`（✅ 已完成）
2. 用 codegraph 核对每阶段核心类现状（已在本次探索中部分完成）
3. 写 `docs/evolution.md` → commit
4. 逐个打 tag（`stage1-skeleton`…`refactor-packages`），每个打完 `git show <tag>` 验证锚点
5. push 分支与 tag
6. 完成后提示用户决定合并/PR/保留；恢复 WIP：`git stash pop`（切回含 WIP 的上下文时）

## 7. 验收标准

- [ ] `docs/evolution.md` 存在，含 Mermaid 时间线 + 5 阶段节 + 重构节 + 文档导航
- [ ] 每阶段核心类清单经 codegraph 核对与当前/历史代码一致
- [ ] 6 个 tag 均已创建，`git tag -l` 可见
- [ ] 每个 tag 的 `git show <tag> --stat` 锚点 commit 与本表一致
- [ ] `git checkout stage1-skeleton` 等可正常切到对应历史形态
- [ ] 文档与 architecture.md 无内容重复，仅有导航链接

## 8. 非目标

- 不重写或合并已有 specs/plans/architecture.md
- 不修改任何源码（纯文档 + git 元数据）
- 不做交互式可视化（已明确选择 Markdown + Mermaid）
- 不逐 commit 解读（高层路线图，非 commit 级详述）
