# 独立 state/checkpoint 顶层包 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把 `org.miniflink.runtime` 根目录的 state 类与 checkpoint 类抽到 `org.miniflink.state` / `org.miniflink.checkpoint` 两个顶层包，行为零变化。

**Architecture:** 纯包重组——仅改 `package` 声明与 `import`，不改任何 API、签名、注释内容与运行时行为。三层单向依赖：`state ◀ checkpoint ◀ runtime`，无环。

**Tech Stack:** Java 17（用到 record）、Maven、JUnit 5。

## Global Constraints

- **纯包重组**：不改任何接口定义、方法签名、方法体逻辑、注释文字。只改 `package` 声明与 `import` 语句。
- **不引入新抽象**：不引入 `StateDescriptor`、不拆 `api.state`、不引入 `CheckpointTrigger`。
- **验证标准**：每个 Task 结束时 `mvn compile` + `mvn test` 全绿（现有全部测试通过），行为零变化。
- **工作分支**：`refactor/state-checkpoint-package`（已建并已提交 spec）。
- **环境**：macOS，`sed` 原地改写需用 `sed -i ''`。
- **移动文件用 `git mv`**（保留历史）；改 `package` 用 `sed` 批量；改 `import` 用 Edit 逐文件精确替换。
- **注释保留**：如 `StateSnapshot.java` 注释里的 "MemoryStateBackend 三类 store" 是文字描述，移动后仍准确，不改。

## 文件结构

**新建目录**：
- `src/main/java/org/miniflink/state/`
- `src/main/java/org/miniflink/checkpoint/`
- `src/test/java/org/miniflink/state/`
- `src/test/java/org/miniflink/checkpoint/`

**移动 main（13）→ 见 Task 1 / Task 2。移动 test（4）→ 见 Task 1 / Task 2。改 import 的 main（9）+ test（4）→ 见各 Task。**

---

### Task 1: 抽出 `org.miniflink.state` 包

**Files:**
- Move (main, 10): `StateBackend`、`MemoryStateBackend`、`ValueState`、`ValueStateImpl`、`ListState`、`ListStateImpl`、`MapState`、`MapStateImpl`、`OperatorState`、`StateSnapshot` → `src/main/java/org/miniflink/state/`
- Move (test, 2): `StateSnapshotTest`、`MemoryStateBackendTest` → `src/test/java/org/miniflink/state/`
- Modify import (main): `RuntimeContext`、`RuntimeContextImpl`、`Operator`、`OperatorChain`、`SourceTask`、`OperatorTask`、`WindowOperator`、`SubtaskSnapshot`、`WindowOperatorState`
- Modify import (test): `RuntimeContextTest`、`SubtaskSnapshotTest`、`WindowOperatorStateTest`、`WindowFailoverTest`

**Interfaces:**
- Produces: 顶层包 `org.miniflink.state`（10 个类），被后续 runtime/checkpoint 引用。
- 依赖方向产出：`SubtaskSnapshot` / `WindowOperatorState`（本 Task 暂留原位）开始 `import org.miniflink.state.*`，为 Task 2 移入 checkpoint 包做准备。

- [ ] **Step 1: 建 state main 目录并 git mv 10 个类**

```bash
mkdir -p src/main/java/org/miniflink/state
for f in StateBackend MemoryStateBackend ValueState ValueStateImpl ListState ListStateImpl MapState MapStateImpl OperatorState StateSnapshot; do
  git mv "src/main/java/org/miniflink/runtime/$f.java" "src/main/java/org/miniflink/state/$f.java"
done
```

- [ ] **Step 2: 批量改这 10 个类的 package 声明（runtime → state）**

```bash
sed -i '' 's/^package org\.miniflink\.runtime;/package org.miniflink.state;/' src/main/java/org/miniflink/state/*.java
```

验证：
```bash
head -1 src/main/java/org/miniflink/state/StateBackend.java
# 期望：package org.miniflink.state;
head -1 src/main/java/org/miniflink/state/MemoryStateBackend.java
# 期望：package org.miniflink.state;
```

> 这 10 个类相互引用（如 `MemoryStateBackend`→`ValueStateImpl`，`StateBackend`→`StateSnapshot`），移动后同在 `state` 包，无需补 import。

- [ ] **Step 3: 改 main 引用方 import（逐文件 Edit，加入下列确切的 import 行）**

对每个文件，在其 import 块（`package` 声明之后、首个 `class`/`interface` 之前）插入指定 import。**插入的 import 内容必须逐字如下**：

| 文件 | 新增 import 行 |
|---|---|
| `runtime/RuntimeContext.java` | `import org.miniflink.state.StateBackend;` |
| `runtime/RuntimeContextImpl.java` | `import org.miniflink.state.StateBackend;`<br>`import org.miniflink.state.MemoryStateBackend;` |
| `runtime/Operator.java` | `import org.miniflink.state.OperatorState;` |
| `runtime/OperatorChain.java` | `import org.miniflink.state.OperatorState;` |
| `runtime/SourceTask.java` | `import org.miniflink.state.StateSnapshot;` |
| `runtime/OperatorTask.java` | `import org.miniflink.state.StateSnapshot;`<br>`import org.miniflink.state.OperatorState;` |

`WindowOperator.java` 是**替换**现有 import（非新增）：
| 原 import 行 | 新 import 行 |
|---|---|
| `import org.miniflink.runtime.MapState;` | `import org.miniflink.state.MapState;` |
| `import org.miniflink.runtime.OperatorState;` | `import org.miniflink.state.OperatorState;` |

> `WindowOperator` 中的 `import org.miniflink.runtime.checkpoint.WindowOperatorState;` 本 Task **不动**（Task 2 改）。

`SubtaskSnapshot.java`（本 Task 暂留 `runtime`，Task 2 才移 checkpoint）—— 新增：
```
import org.miniflink.state.StateSnapshot;
import org.miniflink.state.OperatorState;
```

`WindowOperatorState.java`（本 Task 暂留 `runtime/checkpoint`，Task 2 才移）—— 替换：
| 原 import 行 | 新 import 行 |
|---|---|
| `import org.miniflink.runtime.OperatorState;` | `import org.miniflink.state.OperatorState;` |

- [ ] **Step 4: 建 state test 目录并 git mv 2 个测试**

```bash
mkdir -p src/test/java/org/miniflink/state
git mv src/test/java/org/miniflink/runtime/StateSnapshotTest.java src/test/java/org/miniflink/state/StateSnapshotTest.java
git mv src/test/java/org/miniflink/runtime/MemoryStateBackendTest.java src/test/java/org/miniflink/state/MemoryStateBackendTest.java
sed -i '' 's/^package org\.miniflink\.runtime;/package org.miniflink.state;/' src/test/java/org/miniflink/state/*.java
```

> 这两个测试引用的类（`StateSnapshot`/`MemoryStateBackend`/`ValueState`/`MapState`/`ListState`）已全部移入 `state` 包，同包无需 import。

- [ ] **Step 5: 改其余 test 引用方 import**

| 文件 | 改动 |
|---|---|
| `runtime/RuntimeContextTest.java` | 新增 `import org.miniflink.state.ValueState;` |
| `runtime/SubtaskSnapshotTest.java`（暂留 runtime，Task 2 移） | 新增 `import org.miniflink.state.ValueState;` |
| `runtime/checkpoint/WindowOperatorStateTest.java`（暂留，Task 2 移） | 新增 `import org.miniflink.state.OperatorState;` |
| `window/WindowFailoverTest.java` | `import org.miniflink.runtime.StateSnapshot;` → `import org.miniflink.state.StateSnapshot;` |

> `WindowFailoverTest` 中的 `import org.miniflink.runtime.checkpoint.WindowOperatorState;` 本 Task 不动（Task 2 改）。

- [ ] **Step 6: 编译验证**

Run: `mvn -q compile`
Expected: BUILD SUCCESS（无编译错误）。若报"cannot find symbol"，按报错文件补对应 `import org.miniflink.state.<类名>;`（类名见上表）。

- [ ] **Step 7: 测试验证**

Run: `mvn -q test`
Expected: BUILD SUCCESS，全部现有测试通过（与重组前同等数量通过）。

- [ ] **Step 8: Commit**

```bash
git add -A
git commit -m "refactor(state): 抽出 org.miniflink.state 顶层包（10 类 + 引用方 import）

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

### Task 2: 抽出 `org.miniflink.checkpoint` 包

**Files:**
- Move (main, 3): `Checkpoint`、`SubtaskSnapshot`（从 `runtime`）、`WindowOperatorState`（从 `runtime/checkpoint`）→ `src/main/java/org/miniflink/checkpoint/`
- Move (test, 2): `SubtaskSnapshotTest`（从 `runtime`）、`WindowOperatorStateTest`（从 `runtime/checkpoint`）→ `src/test/java/org/miniflink/checkpoint/`
- Modify import (main): `CheckpointCoordinator`、`SourceTask`、`OperatorTask`、`StreamExecutor`、`WindowOperator`
- Modify import (test): `CheckpointCoordinatorTest`、`WindowFailoverTest`

**Interfaces:**
- Produces: 顶层包 `org.miniflink.checkpoint`（3 个类），仅依赖 `state`，被 `runtime` 引用。
- Consumes: Task 1 产出的 `org.miniflink.state`（`SubtaskSnapshot`/`WindowOperatorState` 已在 Task 1 加好 `import org.miniflink.state.*`）。

- [ ] **Step 1: 建 checkpoint main 目录并 git mv 3 个类**

```bash
mkdir -p src/main/java/org/miniflink/checkpoint
git mv src/main/java/org/miniflink/runtime/Checkpoint.java        src/main/java/org/miniflink/checkpoint/Checkpoint.java
git mv src/main/java/org/miniflink/runtime/SubtaskSnapshot.java   src/main/java/org/miniflink/checkpoint/SubtaskSnapshot.java
git mv src/main/java/org/miniflink/runtime/checkpoint/WindowOperatorState.java src/main/java/org/miniflink/checkpoint/WindowOperatorState.java
```

- [ ] **Step 2: 改这 3 个类的 package 声明**

`Checkpoint.java` 与 `SubtaskSnapshot.java` 来自 `runtime` 根包：
```bash
sed -i '' 's/^package org\.miniflink\.runtime;/package org.miniflink.checkpoint;/' src/main/java/org/miniflink/checkpoint/Checkpoint.java src/main/java/org/miniflink/checkpoint/SubtaskSnapshot.java
```

`WindowOperatorState.java` 来自 `runtime.checkpoint` 子包，单独改：
```bash
sed -i '' 's/^package org\.miniflink\.runtime\.checkpoint;/package org.miniflink.checkpoint;/' src/main/java/org/miniflink/checkpoint/WindowOperatorState.java
```

验证：
```bash
head -1 src/main/java/org/miniflink/checkpoint/Checkpoint.java            # package org.miniflink.checkpoint;
head -1 src/main/java/org/miniflink/checkpoint/SubtaskSnapshot.java       # package org.miniflink.checkpoint;
head -1 src/main/java/org/miniflink/checkpoint/WindowOperatorState.java   # package org.miniflink.checkpoint;
```

> `SubtaskSnapshot` 的 `import org.miniflink.state.StateSnapshot/OperatorState` 已在 Task 1 加好；`WindowOperatorState` 的 `import org.miniflink.state.OperatorState` 已在 Task 1 改好。无需再动。

- [ ] **Step 3: 改 main 引用方 import**

| 文件 | 新增 import 行 |
|---|---|
| `runtime/CheckpointCoordinator.java` | `import org.miniflink.checkpoint.Checkpoint;`<br>`import org.miniflink.checkpoint.SubtaskSnapshot;` |
| `runtime/SourceTask.java` | `import org.miniflink.checkpoint.SubtaskSnapshot;` |
| `runtime/OperatorTask.java` | `import org.miniflink.checkpoint.SubtaskSnapshot;` |
| `runtime/StreamExecutor.java` | `import org.miniflink.checkpoint.Checkpoint;`<br>`import org.miniflink.checkpoint.SubtaskSnapshot;` |

`WindowOperator.java` —— 替换现有 import：
| 原 import 行 | 新 import 行 |
|---|---|
| `import org.miniflink.runtime.checkpoint.WindowOperatorState;` | `import org.miniflink.checkpoint.WindowOperatorState;` |

- [ ] **Step 4: 建 checkpoint test 目录并 git mv 2 个测试 + 改 package**

```bash
mkdir -p src/test/java/org/miniflink/checkpoint
git mv src/test/java/org/miniflink/runtime/SubtaskSnapshotTest.java        src/test/java/org/miniflink/checkpoint/SubtaskSnapshotTest.java
git mv src/test/java/org/miniflink/runtime/checkpoint/WindowOperatorStateTest.java src/test/java/org/miniflink/checkpoint/WindowOperatorStateTest.java
sed -i '' 's/^package org\.miniflink\.runtime;/package org.miniflink.checkpoint;/' src/test/java/org/miniflink/checkpoint/SubtaskSnapshotTest.java
sed -i '' 's/^package org\.miniflink\.runtime\.checkpoint;/package org.miniflink.checkpoint;/' src/test/java/org/miniflink/checkpoint/WindowOperatorStateTest.java
```

> `SubtaskSnapshotTest` 的 `import org.miniflink.state.ValueState`（Task 1 已加）与 `import org.miniflink.runtime.operator.MapOperator`（不动）保留；`WindowOperatorStateTest` 的 `import org.miniflink.state.OperatorState`（Task 1 已加）保留。

- [ ] **Step 5: 改其余 test 引用方 import**

| 文件 | 改动 |
|---|---|
| `runtime/CheckpointCoordinatorTest.java` | 新增 `import org.miniflink.checkpoint.Checkpoint;`<br>新增 `import org.miniflink.checkpoint.SubtaskSnapshot;` |
| `window/WindowFailoverTest.java` | `import org.miniflink.runtime.checkpoint.WindowOperatorState;` → `import org.miniflink.checkpoint.WindowOperatorState;` |

- [ ] **Step 6: 编译验证**

Run: `mvn -q compile`
Expected: BUILD SUCCESS。

- [ ] **Step 7: 测试验证**

Run: `mvn -q test`
Expected: BUILD SUCCESS，全部现有测试通过。

- [ ] **Step 8: 删除已空的旧 checkpoint 子包目录（若有）**

```bash
# runtime/checkpoint 子包下的类已全部移走，目录应已空；git mv 会处理，确认无残留
ls src/main/java/org/miniflink/runtime/checkpoint 2>/dev/null && echo "仍有残留" || echo "目录已清（正常）"
ls src/test/java/org/miniflink/runtime/checkpoint 2>/dev/null && echo "仍有残留" || echo "目录已清（正常）"
```

- [ ] **Step 9: Commit**

```bash
git add -A
git commit -m "refactor(checkpoint): 抽出 org.miniflink.checkpoint 顶层包（3 类 + 引用方 import）

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

### Task 3: 文档同步 + 全量验证

**Files:**
- Modify: `docs/architecture.md`（涉及包名/分层/StateBackend/CheckpointCoordinator 的段落）

**Interfaces:** 无（文档与验证）。

- [ ] **Step 1: 更新 `docs/architecture.md`**

读 `docs/architecture.md`，找到所有提及包结构、分层、`StateBackend`、`CheckpointCoordinator`、`StateSnapshot`、`SubtaskSnapshot`、`WindowOperatorState` 所属包的段落，按以下新结构更新描述：

- `org.miniflink.state`：状态后端与状态句柄（`StateBackend`/`MemoryStateBackend`/`Value|List|MapState`(+Impl)/`OperatorState`/`StateSnapshot`），零外部依赖。
- `org.miniflink.checkpoint`：快照数据结构（`Checkpoint`/`SubtaskSnapshot`/`WindowOperatorState`），仅依赖 state。
- `org.miniflink.runtime`：执行核心（含 `CheckpointCoordinator`、`Barrier`、`SnapshotCallback`），依赖 state + checkpoint。

附上三层单向依赖图：
```
state ◀── checkpoint
  ▲           ▲
  └── runtime ┘
```

> 只改包归属描述，不改机制说明。若文档某处仅描述机制未提包名，不动。

- [ ] **Step 2: grep 确认无残留旧引用**

```bash
# main + test 中不应再有指向旧位置的 import / FQN（注释中的历史性文字描述可忽略）
grep -rn -E 'org\.miniflink\.runtime\.(StateBackend|MemoryStateBackend|ValueState|ValueStateImpl|ListState|ListStateImpl|MapState|MapStateImpl|OperatorState|StateSnapshot|Checkpoint|SubtaskSnapshot)\b' src || echo "✓ 无残留 state/checkpoint 旧引用"
grep -rn 'org\.miniflink\.runtime\.checkpoint' src || echo "✓ 无残留 runtime.checkpoint 旧子包引用"
```
Expected: 两条均打印 `✓ 无残留...`（grep 无匹配时返回非零，触发 echo）。

- [ ] **Step 3: 全量编译 + 测试**

Run: `mvn -q clean test`
Expected: BUILD SUCCESS，全部测试通过（与重组前同等数量，行为零变化）。

- [ ] **Step 4: 确认包结构落地**

```bash
echo "=== state 包 ==="; ls src/main/java/org/miniflink/state/
echo "=== checkpoint 包 ==="; ls src/main/java/org/miniflink/checkpoint/
echo "=== runtime 根（应不再含 state/checkpoint 类）==="; ls src/main/java/org/miniflink/runtime/*.java | xargs -n1 basename
```
Expected: `state/` 10 个类；`checkpoint/` 3 个类；`runtime/` 根目录不再含 `StateBackend.java`/`Checkpoint.java`/`SubtaskSnapshot.java` 等。

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "docs(architecture): 同步 state/checkpoint 顶层包结构 + 全量验证

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

## Self-Review

**1. Spec coverage**：spec 的迁移清单（state 10 类、checkpoint 3 类、9 main 改 import、4 测试移动、architecture.md 同步、grep 验证）→ Task 1/2/3 全覆盖。✓

**2. Placeholder scan**：无 TBD/TODO；每个 import 改动给出确切 import 行或精确的"原→新"替换；命令均完整。✓

**3. Type 一致性**：`import org.miniflink.state.X` 与 `import org.miniflink.checkpoint.X` 的类名与移动类一致；Task 1 为 `SubtaskSnapshot`/`WindowOperatorState` 预加的 state import，与 Task 2 移动后的包声明不冲突。✓

**4. 依赖顺序**：Task 1（state，最底层）→ Task 2（checkpoint，依赖 state）→ Task 3（文档+验证）。每个 Task 结束编译通过。`SubtaskSnapshot`/`WindowOperatorState` 在 Task 1 暂留原位但加好 state import，Task 2 再移动，中间状态可编译。✓
