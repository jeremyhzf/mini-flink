# Mini-Flink 阶段③（keyed state + 聚合）实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在 keyBy 基础上加 keyed state（ValueState/ListState/MapState）+ RuntimeContext 统一注入 + KeyedStream + ReduceOperator，实现有状态流处理（WordCount）。

**Architecture:** 引入 `StateBackend`（内存，per-subtask）+ 三种 state 句柄（经 currentKey 寻址）+ `RuntimeContext`（统一注入算子，承载 subtask 信息 + state 访问）。`Operator.open` 签名统一为 `(Collector, RuntimeContext)`。`keyBy` 返回 `KeyedStream`，其上 `reduce` 用 per-key ValueState 做 running reduce。

**Tech Stack:** Java 17、Maven、JUnit 5、纯 JDK。

## Global Constraints

- Java 17（`maven.compiler.release=17`），Maven 构建。
- 包根 `org.miniflink`；依赖仅 JUnit 5（test scope），其余纯 JDK。
- 所有代码注释、commit message 使用中文。
- 每个任务结束必须 commit；TDD（先失败测试 → 实现 → 通过）。
- `Collector<T>` 接口**不可改动**（稳定边界）。
- 阶段③仍维持**单线性链 + 单 sink**。

---

## File Structure（阶段③新增/修改）

```
src/main/java/org/miniflink/
├── api/
│   ├── function/ReduceFunction.java     # 新增
│   ├── KeyedStream.java                 # 新增（keyBy 返回）
│   └── DataStream.java                  # 修改：keyBy 返回 KeyedStream
├── runtime/
│   ├── StateBackend.java                # 新增（接口）
│   ├── MemoryStateBackend.java          # 新增（内存存储 + 建句柄）
│   ├── ValueState.java                  # 新增（接口）
│   ├── ListState.java                   # 新增（接口）
│   ├── MapState.java                    # 新增（接口）
│   ├── ValueStateImpl.java              # 新增（句柄，绑 backend + currentKey）
│   ├── ListStateImpl.java               # 新增
│   ├── MapStateImpl.java                # 新增
│   ├── RuntimeContext.java              # 新增（接口）
│   ├── RuntimeContextImpl.java          # 新增
│   ├── Operator.java                    # 修改：open(Collector, RuntimeContext)
│   ├── OperatorChain.java               # 修改：open(Collector, RuntimeContext)
│   ├── SourceOperator.java              # 修改：open(Collector, RuntimeContext) 替换 (Collector,int,int)
│   ├── SourceTask.java                  # 修改：构造加 RuntimeContext
│   ├── OperatorTask.java                # 修改：构造加 RuntimeContext
│   ├── StreamExecutor.java              # 修改：建 per-subtask RuntimeContextImpl + 传 Task
│   ├── operator/MapOperator.java        # 修改：open 加 ctx
│   ├── operator/FilterOperator.java     # 修改：open 加 ctx
│   ├── operator/FlatMapOperator.java    # 修改：open 加 ctx
│   ├── operator/SinkOperator.java       # 修改：open 加 ctx
│   ├── operator/SourceOperatorImpl.java # 修改：open 用 ctx 取 subtaskIndex/parallelism
│   └── operator/ReduceOperator.java     # 新增（keyed 聚合算子）
└── src/test/java/org/miniflink/...      # 各任务单测
```

### 跨任务类型契约（权威）

```java
// runtime 新增
interface StateBackend {
    <T> ValueState<T> getValueState(String name);
    <T> ListState<T> getListState(String name);
    <K, V> MapState<K, V> getMapState(String name);
    void setCurrentKey(Object key);
}
interface ValueState<T> { T value(); void update(T v); }
interface ListState<T>  { Iterable<T> get(); void add(T v); void clear(); }
interface MapState<K,V> { V get(K k); void put(K k, V v); Iterable<Map.Entry<K,V>> entries(); void clear(); }

interface RuntimeContext {
    int getSubtaskIndex(); int getParallelism();
    Object getCurrentKey(); void setCurrentKey(Object key);   // 转发 StateBackend.setCurrentKey
    StateBackend getStateBackend();
    KeySelector<?, ?> getKeySelector();   // keyed 算子非 null，普通算子可 null
}

// runtime 修改（open 签名统一）
interface Operator<IN, OUT> { void open(Collector<OUT> out, RuntimeContext ctx); void processElement(IN) throws Exception; void close(); Operator<IN,OUT> copy(); }
interface SourceOperator<OUT> { void open(Collector<OUT> out, RuntimeContext ctx); void run() throws Exception; void close(); SourceOperator<OUT> copy(); }
class OperatorChain<IN,OUT> { void open(Collector<OUT> output, RuntimeContext ctx); ... }

// api 新增
@FunctionalInterface interface ReduceFunction<T> { T reduce(T a, T b) throws Exception; }
class KeyedStream<T, K> { DataStream<T> reduce(ReduceFunction<T> reduceFn); DataStream<T> sum(ReduceFunction<T> sumFn); }
```

---

## Task 1: StateBackend + 三种 state + MemoryStateBackend + 句柄实现

**Files:**
- Create: `src/main/java/org/miniflink/runtime/StateBackend.java`
- Create: `src/main/java/org/miniflink/runtime/ValueState.java`
- Create: `src/main/java/org/miniflink/runtime/ListState.java`
- Create: `src/main/java/org/miniflink/runtime/MapState.java`
- Create: `src/main/java/org/miniflink/runtime/MemoryStateBackend.java`
- Create: `src/main/java/org/miniflink/runtime/ValueStateImpl.java`
- Create: `src/main/java/org/miniflink/runtime/ListStateImpl.java`
- Create: `src/main/java/org/miniflink/runtime/MapStateImpl.java`
- Test: `src/test/java/org/miniflink/runtime/MemoryStateBackendTest.java`

**Interfaces:**
- Produces: `StateBackend.getValueState/getListState/getMapState(name)` + `setCurrentKey(key)`；`MemoryStateBackend`；`ValueState/ListState/MapState` 接口 + `*Impl`（句柄绑 backend，经 `backend.currentKey()` 寻址）。

- [ ] **Step 1: 写失败测试 `MemoryStateBackendTest`**

```java
package org.miniflink.runtime;

import org.junit.jupiter.api.Test;
import java.util.Map;
import static org.junit.jupiter.api.Assertions.*;

class MemoryStateBackendTest {

    @Test
    void ValueState按currentKey隔离存取() {
        MemoryStateBackend backend = new MemoryStateBackend();
        ValueState<Integer> state = backend.getValueState("acc");

        backend.setCurrentKey("a");
        assertNull(state.value());
        state.update(1);
        assertEquals(1, state.value());

        backend.setCurrentKey("b");      // 切 key，a 的值不受影响
        assertNull(state.value());
        state.update(2);
        assertEquals(2, state.value());

        backend.setCurrentKey("a");      // 回到 a，值仍在
        assertEquals(1, state.value());
    }

    @Test
    void ListState按key累加() {
        MemoryStateBackend backend = new MemoryStateBackend();
        ListState<String> state = backend.getListState("words");

        backend.setCurrentKey("a");
        state.add("x"); state.add("y");
        assertIterableEquals(java.util.List.of("x", "y"), state.get());

        backend.setCurrentKey("b");
        assertNull(state.get());          // 新 key 无数据
    }

    @Test
    void MapState按key存键值() {
        MemoryStateBackend backend = new MemoryStateBackend();
        MapState<String, Integer> state = backend.getMapState("counts");

        backend.setCurrentKey("a");
        state.put("k1", 1); state.put("k2", 2);
        assertEquals(1, state.get("k1"));

        backend.setCurrentKey("b");
        assertNull(state.get("k1"));      // 隔离
    }
}
```

- [ ] **Step 2: 运行测试验证失败**

Run: `mvn -q -Dtest=MemoryStateBackendTest test`
Expected: 编译失败 —— `StateBackend`/`ValueState`/`MemoryStateBackend` 等不存在。

- [ ] **Step 3: 创建 `StateBackend` 接口**

```java
package org.miniflink.runtime;

/** 状态后端：创建 keyed state 句柄；currentKey 由 RuntimeContext 设置。 */
public interface StateBackend {
    <T> ValueState<T> getValueState(String name);
    <T> ListState<T> getListState(String name);
    <K, V> MapState<K, V> getMapState(String name);
    void setCurrentKey(Object key);
}
```

- [ ] **Step 4: 创建三种 state 接口**

`ValueState.java`：
```java
package org.miniflink.runtime;
/** 单值状态（绑定当前 key）。 */
public interface ValueState<T> {
    T value();
    void update(T v);
}
```

`ListState.java`：
```java
package org.miniflink.runtime;
/** 列表状态（绑定当前 key）。 */
public interface ListState<T> {
    Iterable<T> get();
    void add(T v);
    void clear();
}
```

`MapState.java`：
```java
package org.miniflink.runtime;
import java.util.Map;
/** 映射状态（绑定当前 key）。 */
public interface MapState<K, V> {
    V get(K key);
    void put(K key, V value);
    Iterable<Map.Entry<K, V>> entries();
    void clear();
}
```

- [ ] **Step 5: 创建 `MemoryStateBackend`**

```java
package org.miniflink.runtime;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 内存状态后端（per-subtask）：三类存储 name -> (currentKey -> 值)。
 * 句柄实现经 currentKey() 寻址。currentKey 由 RuntimeContext 通过 setCurrentKey 设置。
 */
public class MemoryStateBackend implements StateBackend {
    private Object currentKey;
    private final Map<String, Map<Object, Object>> valueStore = new HashMap<>();
    private final Map<String, Map<Object, List<Object>>> listStore = new HashMap<>();
    private final Map<String, Map<Object, Map<Object, Object>>> mapStore = new HashMap<>();

    @Override
    public void setCurrentKey(Object key) {
        this.currentKey = key;
    }

    Object currentKey() {
        return currentKey;
    }

    // ---- ValueState 存储 ----
    Object getValue(String name, Object key) {
        Map<Object, Object> m = valueStore.get(name);
        return m == null ? null : m.get(key);
    }
    void putValue(String name, Object key, Object value) {
        valueStore.computeIfAbsent(name, k -> new HashMap<>()).put(key, value);
    }

    // ---- ListState 存储 ----
    List<Object> getOrCreateList(String name, Object key) {
        return listStore.computeIfAbsent(name, k -> new HashMap<>())
                .computeIfAbsent(key, k -> new ArrayList<>());
    }

    // ---- MapState 存储 ----
    Map<Object, Object> getOrCreateMap(String name, Object key) {
        return mapStore.computeIfAbsent(name, k -> new HashMap<>())
                .computeIfAbsent(key, k -> new HashMap<>());
    }

    @Override
    public <T> ValueState<T> getValueState(String name) {
        return new ValueStateImpl<>(this, name);
    }

    @Override
    public <T> ListState<T> getListState(String name) {
        return new ListStateImpl<>(this, name);
    }

    @Override
    public <K, V> MapState<K, V> getMapState(String name) {
        return new MapStateImpl<>(this, name);
    }
}
```

- [ ] **Step 6: 创建 `ValueStateImpl`**

```java
package org.miniflink.runtime;

/** ValueState 句柄：经 backend.currentKey() 寻址 backend 的 per-key 存储。 */
public class ValueStateImpl<T> implements ValueState<T> {
    private final MemoryStateBackend backend;
    private final String name;

    public ValueStateImpl(MemoryStateBackend backend, String name) {
        this.backend = backend;
        this.name = name;
    }

    @Override
    public T value() {
        Object key = backend.currentKey();
        return (T) backend.getValue(name, key);
    }

    @Override
    public void update(T v) {
        backend.putValue(name, backend.currentKey(), v);
    }
}
```

- [ ] **Step 7: 创建 `ListStateImpl`**

```java
package org.miniflink.runtime;

import java.util.List;

/** ListState 句柄：操作 backend 中当前 key 的 List。 */
public class ListStateImpl<T> implements ListState<T> {
    private final MemoryStateBackend backend;
    private final String name;

    public ListStateImpl(MemoryStateBackend backend, String name) {
        this.backend = backend;
        this.name = name;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Iterable<T> get() {
        List<Object> list = (List<Object>) backend.getOrCreateList(name, backend.currentKey());
        return (Iterable<T>) list;
    }

    @Override
    public void add(T v) {
        backend.getOrCreateList(name, backend.currentKey()).add(v);
    }

    @Override
    public void clear() {
        backend.getOrCreateList(name, backend.currentKey()).clear();
    }
}
```

- [ ] **Step 8: 创建 `MapStateImpl`**

```java
package org.miniflink.runtime;

import java.util.Map;

/** MapState 句柄：操作 backend 中当前 key 的 Map。 */
public class MapStateImpl<K, V> implements MapState<K, V> {
    private final MemoryStateBackend backend;
    private final String name;

    public MapStateImpl(MemoryStateBackend backend, String name) {
        this.backend = backend;
        this.name = name;
    }

    @Override
    @SuppressWarnings("unchecked")
    public V get(K key) {
        Map<Object, Object> m = backend.getOrCreateMap(name, backend.currentKey());
        return (V) m.get(key);
    }

    @Override
    public void put(K key, V value) {
        backend.getOrCreateMap(name, backend.currentKey()).put(key, value);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Iterable<Map.Entry<K, V>> entries() {
        Map<Object, Object> m = backend.getOrCreateMap(name, backend.currentKey());
        return (Iterable<Map.Entry<K, V>>) (Iterable<?>) m.entrySet();
    }

    @Override
    public void clear() {
        backend.getOrCreateMap(name, backend.currentKey()).clear();
    }
}
```

- [ ] **Step 9: 运行测试验证通过**

Run: `mvn -q -Dtest=MemoryStateBackendTest test`
Expected: `BUILD SUCCESS`，`Tests run: 3, Failures: 0`

- [ ] **Step 10: 提交**

```bash
git add src/main/java/org/miniflink/runtime/StateBackend.java \
        src/main/java/org/miniflink/runtime/ValueState.java \
        src/main/java/org/miniflink/runtime/ListState.java \
        src/main/java/org/miniflink/runtime/MapState.java \
        src/main/java/org/miniflink/runtime/MemoryStateBackend.java \
        src/main/java/org/miniflink/runtime/ValueStateImpl.java \
        src/main/java/org/miniflink/runtime/ListStateImpl.java \
        src/main/java/org/miniflink/runtime/MapStateImpl.java \
        src/test/java/org/miniflink/runtime/MemoryStateBackendTest.java
git commit -m "feat(runtime): 添加 StateBackend/三种 state/MemoryStateBackend 与句柄

Co-Authored-By: Claude <noreply@anthropic.com>"
```

## Task 2: RuntimeContext + RuntimeContextImpl

**Files:**
- Create: `src/main/java/org/miniflink/runtime/RuntimeContext.java`
- Create: `src/main/java/org/miniflink/runtime/RuntimeContextImpl.java`
- Test: `src/test/java/org/miniflink/runtime/RuntimeContextTest.java`

**Interfaces:**
- Consumes: `StateBackend`/`MemoryStateBackend`/`ValueState`（Task 1）、`KeySelector`（阶段②）。
- Produces: `RuntimeContext.getSubtaskIndex()/getParallelism()/getCurrentKey()/setCurrentKey()/getStateBackend()/getKeySelector()`；`RuntimeContextImpl(int subtaskIndex, int parallelism, KeySelector)`。

- [ ] **Step 1: 写失败测试 `RuntimeContextTest`**

```java
package org.miniflink.runtime;

import org.junit.jupiter.api.Test;
import org.miniflink.api.function.KeySelector;

import static org.junit.jupiter.api.Assertions.*;

class RuntimeContextTest {

    @Test
    void setCurrentKey转发backend且key隔离() {
        RuntimeContextImpl ctx = new RuntimeContextImpl(0, 1, null);
        ValueState<Integer> state = ctx.getStateBackend().getValueState("acc");

        ctx.setCurrentKey("a");
        state.update(1);
        ctx.setCurrentKey("b");
        assertNull(state.value());      // b 隔离
        state.update(2);
        ctx.setCurrentKey("a");
        assertEquals(1, state.value()); // a 仍在
    }

    @Test
    void 持有subtaskIndex与keySelector() {
        KeySelector<String, String> ks = s -> s;
        RuntimeContextImpl ctx = new RuntimeContextImpl(1, 2, ks);
        assertEquals(1, ctx.getSubtaskIndex());
        assertEquals(2, ctx.getParallelism());
        assertSame(ks, ctx.getKeySelector());
    }
}
```

- [ ] **Step 2: 运行测试验证失败**

Run: `mvn -q -Dtest=RuntimeContextTest test`
Expected: 编译失败 —— `RuntimeContext`/`RuntimeContextImpl` 不存在。

- [ ] **Step 3: 创建 `RuntimeContext` 接口**

```java
package org.miniflink.runtime;

import org.miniflink.api.function.KeySelector;

/** 算子运行时上下文（per-subtask）：承载并行位置 + keyed state 访问 + currentKey。 */
public interface RuntimeContext {
    int getSubtaskIndex();
    int getParallelism();
    Object getCurrentKey();
    void setCurrentKey(Object key);       // 转发 StateBackend.setCurrentKey
    StateBackend getStateBackend();
    KeySelector<?, ?> getKeySelector();   // keyed 算子非 null，普通算子可 null
}
```

- [ ] **Step 4: 创建 `RuntimeContextImpl`**

```java
package org.miniflink.runtime;

import org.miniflink.api.function.KeySelector;

/** RuntimeContext 默认实现：内置一个 MemoryStateBackend，setCurrentKey 转发到它。 */
public class RuntimeContextImpl implements RuntimeContext {
    private final int subtaskIndex;
    private final int parallelism;
    private final KeySelector<?, ?> keySelector;
    private final MemoryStateBackend backend = new MemoryStateBackend();

    public RuntimeContextImpl(int subtaskIndex, int parallelism, KeySelector<?, ?> keySelector) {
        this.subtaskIndex = subtaskIndex;
        this.parallelism = parallelism;
        this.keySelector = keySelector;
    }

    @Override
    public int getSubtaskIndex() {
        return subtaskIndex;
    }

    @Override
    public int getParallelism() {
        return parallelism;
    }

    @Override
    public Object getCurrentKey() {
        return backend.currentKey();
    }

    @Override
    public void setCurrentKey(Object key) {
        backend.setCurrentKey(key);
    }

    @Override
    public StateBackend getStateBackend() {
        return backend;
    }

    @Override
    public KeySelector<?, ?> getKeySelector() {
        return keySelector;
    }
}
```

- [ ] **Step 5: 运行测试验证通过**

Run: `mvn -q -Dtest=RuntimeContextTest test`
Expected: `BUILD SUCCESS`，`Tests run: 2, Failures: 0`

- [ ] **Step 6: 提交**

```bash
git add src/main/java/org/miniflink/runtime/RuntimeContext.java \
        src/main/java/org/miniflink/runtime/RuntimeContextImpl.java \
        src/test/java/org/miniflink/runtime/RuntimeContextTest.java
git commit -m "feat(runtime): 添加 RuntimeContext 与 RuntimeContextImpl

Co-Authored-By: Claude <noreply@anthropic.com>"
```

## Task 3: Operator.open 签名统一为 (Collector, RuntimeContext)

> 重构任务：把 `open(Collector)` / `SourceOperator.open(Collector,int,int)` 统一为 `open(Collector, RuntimeContext)`，并由 StreamExecutor 为每个 subtask 建 RuntimeContextImpl。无新行为，验证 = 阶段①② 全量 48 测试不回归。

**Files:**
- Modify: `src/main/java/org/miniflink/runtime/Operator.java`
- Modify: `src/main/java/org/miniflink/runtime/SourceOperator.java`
- Modify: `src/main/java/org/miniflink/runtime/OperatorChain.java`
- Modify: `src/main/java/org/miniflink/runtime/operator/MapOperator.java`
- Modify: `src/main/java/org/miniflink/runtime/operator/FilterOperator.java`
- Modify: `src/main/java/org/miniflink/runtime/operator/FlatMapOperator.java`
- Modify: `src/main/java/org/miniflink/runtime/operator/SinkOperator.java`
- Modify: `src/main/java/org/miniflink/runtime/operator/SourceOperatorImpl.java`
- Modify: `src/main/java/org/miniflink/runtime/SourceTask.java`
- Modify: `src/main/java/org/miniflink/runtime/OperatorTask.java`
- Modify: `src/main/java/org/miniflink/runtime/StreamExecutor.java`

**Interfaces:**
- Consumes: `RuntimeContext`/`RuntimeContextImpl`（Task 2）。
- Produces: `Operator.open(Collector, RuntimeContext)`、`SourceOperator.open(Collector, RuntimeContext)`、`OperatorChain.open(Collector, RuntimeContext)`、`SourceTask(..., RuntimeContext)`、`OperatorTask(..., RuntimeContext)`。

- [ ] **Step 1: 修改 `Operator` 接口**

把 `void open(Collector<OUT> out);` 改为 `void open(Collector<OUT> out, RuntimeContext ctx);`（加 `import org.miniflink.runtime.RuntimeContext;` 不需要——同包）。

- [ ] **Step 2: 修改 `SourceOperator` 接口**

把 `void open(Collector<OUT> out, int subtaskIndex, int parallelism);` 改为：
```java
    void open(Collector<OUT> out, RuntimeContext ctx);
```

- [ ] **Step 3: 修改四个处理算子的 open（加 ctx，忽略）**

`MapOperator.open`：
```java
    @Override
    public void open(Collector<OUT> out, RuntimeContext ctx) {
        this.out = out;
    }
```
`FilterOperator.open`：
```java
    @Override
    public void open(Collector<IN> out, RuntimeContext ctx) {
        this.out = out;
    }
```
`FlatMapOperator.open`：
```java
    @Override
    public void open(Collector<OUT> out, RuntimeContext ctx) {
        this.out = out;
    }
```
`SinkOperator.open`：
```java
    @Override
    public void open(Collector<Void> out, RuntimeContext ctx) {
        // sink 无下游输出，ctx 暂不使用
    }
```

- [ ] **Step 4: 修改 `SourceOperatorImpl.open`（用 ctx 取并行位置）**

把 `open(Collector<OUT> out, int subtaskIndex, int parallelism)` 改为：
```java
    @Override
    public void open(Collector<OUT> out, RuntimeContext ctx) {
        this.ctx = new SourceContextImpl<>(out, ctx.getSubtaskIndex(), ctx.getParallelism());
    }
```

- [ ] **Step 5: 修改 `OperatorChain.open`（透传 ctx 给链内算子）**

把 `open(Collector<OUT> output)` 改为：
```java
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void open(Collector<OUT> output, RuntimeContext ctx) {
        Collector current = output;
        for (int i = operators.size() - 1; i >= 0; i--) {
            Operator op = operators.get(i);
            op.open(current, ctx);
            current = new ChainCollector(op);
        }
    }
```

- [ ] **Step 6: 修改 `SourceTask`（构造收 ctx，run 用它）**

整体替换 `src/main/java/org/miniflink/runtime/SourceTask.java`：
```java
package org.miniflink.runtime;

import java.util.List;

/** source 执行单元：open source（注入 RuntimeContext）→ run → 正常结束广播 EOB。 */
public class SourceTask implements Task {
    private final SourceOperator<?> sourceOperator;
    private final List<Output> outputs;
    private final RuntimeContext ctx;

    public SourceTask(SourceOperator<?> sourceOperator, List<Output> outputs, RuntimeContext ctx) {
        this.sourceOperator = sourceOperator;
        this.outputs = outputs;
        this.ctx = ctx;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Override
    public void run() {
        OutputCollector out = new OutputCollector(outputs, ctx.getSubtaskIndex());
        try {
            sourceOperator.open((Collector) out, ctx);
            sourceOperator.run();
            broadcastEob(outputs, ctx.getSubtaskIndex());
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            throw new RuntimeException("SourceTask 执行异常", e);
        } finally {
            sourceOperator.close();
        }
    }
}
```

- [ ] **Step 7: 修改 `OperatorTask`（构造收 ctx，run 用它）**

整体替换 `src/main/java/org/miniflink/runtime/OperatorTask.java`：
```java
package org.miniflink.runtime;

import java.util.List;

/** 处理算子执行单元：open chain → 循环读 Channel → Record 经 chain 处理 → EOB 计数；归零广播 EOB。 */
public class OperatorTask implements Task {
    private final OperatorChain<?, ?> chain;
    private final Channel input;
    private final int pendingUpstreams;
    private final List<Output> outputs;
    private final RuntimeContext ctx;

    public OperatorTask(OperatorChain<?, ?> chain, Channel input, int pendingUpstreams,
                        List<Output> outputs, RuntimeContext ctx) {
        this.chain = chain;
        this.input = input;
        this.pendingUpstreams = pendingUpstreams;
        this.outputs = outputs;
        this.ctx = ctx;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Override
    public void run() {
        Collector outCollector = outputs.isEmpty() ? new NoopCollector<>() : new OutputCollector(outputs, ctx.getSubtaskIndex());
        try {
            chain.open((Collector) outCollector, ctx);
            @SuppressWarnings("rawtypes")
            OperatorChain rawChain = chain;
            int remaining = pendingUpstreams;
            while (remaining > 0) {
                StreamElement e = input.receive();
                if (e == EndOfBroadcast.INSTANCE) {
                    remaining--;
                } else if (e instanceof Record<?> r) {
                    rawChain.processElement(r.value());
                }
            }
            broadcastEob(outputs, ctx.getSubtaskIndex());
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            throw new RuntimeException("OperatorTask 执行异常", e);
        } finally {
            chain.close();
        }
    }
}
```

- [ ] **Step 8: 修改 `StreamExecutor`（建 per-subtask RuntimeContextImpl + 传 Task）**

在 `execute` 的"为每个 vertex 建 Task"循环中，替换为：
```java
        // 2. 为每个 vertex 建 RuntimeContext + Task
        List<Task> tasks = new ArrayList<>();
        for (ExecutionVertex v : graph.getVertices()) {
            RuntimeContext ctx = new RuntimeContextImpl(
                    v.getSubtaskIndex(), v.getParallelism(), findInputKeySelector(v, graph.getEdges()));
            List<Output> outputs = buildOutputs(v, graph.getEdges(), inputChannelOf);
            if (v.isSource()) {
                tasks.add(new SourceTask(v.getSourceOperator(), outputs, ctx));
            } else {
                Channel input = inputChannelOf.get(v);
                int pending = countUpstreams(v, graph.getEdges());
                tasks.add(new OperatorTask(new OperatorChain<>(v.getOperators()), input, pending, outputs, ctx));
            }
        }
```
并在类中新增私有方法：
```java
    /** 取 vertex 入边的 keySelector（keyed 算子非 null；单线性链每 vertex 最多一条入边）。 */
    private org.miniflink.api.function.KeySelector<?, ?> findInputKeySelector(
            ExecutionVertex v, List<ExecutionEdge> edges) {
        for (ExecutionEdge edge : edges) {
            if (edge.getTargets().contains(v)) {
                return edge.getKeySelector();
            }
        }
        return null;
    }
```

- [ ] **Step 9: 全量回归**

Run: `mvn -q test`
Expected: `BUILD SUCCESS`，阶段①② 共 48 个测试全部通过（签名变更后所有 open 调用点已适配，SourceParallelismTest 仍通过——ctx 含 subtaskIndex/parallelism）。

- [ ] **Step 10: 提交**

```bash
git add -A
git commit -m "refactor(runtime): Operator.open 统一为 (Collector, RuntimeContext)，StreamExecutor 建 per-subtask 上下文

Co-Authored-By: Claude <noreply@anthropic.com>"
```

## Task 4: ReduceFunction + ReduceOperator

**Files:**
- Create: `src/main/java/org/miniflink/api/function/ReduceFunction.java`
- Create: `src/main/java/org/miniflink/runtime/operator/ReduceOperator.java`
- Test: `src/test/java/org/miniflink/runtime/operator/ReduceOperatorTest.java`

**Interfaces:**
- Consumes: `Operator.open(Collector, RuntimeContext)`（Task 3）、`RuntimeContext`/`ValueState`（Task 1-2）、`KeySelector`（阶段②）、`ListCollector`（阶段① test）。
- Produces: `ReduceFunction<T>.reduce(T,T)`；`ReduceOperator<IN>(ReduceFunction)`，`copy()`。

- [ ] **Step 1: 写失败测试 `ReduceOperatorTest`**

```java
package org.miniflink.runtime.operator;

import org.junit.jupiter.api.Test;
import org.miniflink.api.function.KeySelector;
import org.miniflink.api.function.ReduceFunction;
import org.miniflink.runtime.ListCollector;
import org.miniflink.runtime.RuntimeContextImpl;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ReduceOperatorTest {

    @Test
    void 按key累加并输出running结果() throws Exception {
        // keySelector: x -> x % 2（按奇偶分组）；reduce: 求和
        ReduceOperator<Integer> op = new ReduceOperator<>((ReduceFunction<Integer>) (a, b) -> a + b);
        KeySelector<Integer, Integer> ks = x -> x % 2;
        RuntimeContextImpl ctx = new RuntimeContextImpl(0, 1, ks);
        ListCollector<Integer> out = new ListCollector<>();
        op.open(out, ctx);

        op.processElement(1); // key=1, acc=1, 输出 1
        op.processElement(3); // key=1, acc=1+3=4, 输出 4
        op.processElement(2); // key=0, acc=2, 输出 2
        op.processElement(4); // key=0, acc=2+4=6, 输出 6

        assertEquals(List.of(1, 4, 2, 6), out.getResult());
    }
}
```

- [ ] **Step 2: 运行测试验证失败**

Run: `mvn -q -Dtest=ReduceOperatorTest test`
Expected: 编译失败 —— `ReduceFunction`/`ReduceOperator` 不存在。

- [ ] **Step 3: 创建 `ReduceFunction`**

```java
package org.miniflink.api.function;

/** 两两合并函数（keyed reduce 用）。 */
@FunctionalInterface
public interface ReduceFunction<T> {
    T reduce(T a, T b) throws Exception;
}
```

- [ ] **Step 4: 创建 `ReduceOperator`**

```java
package org.miniflink.runtime.operator;

import org.miniflink.api.function.KeySelector;
import org.miniflink.api.function.ReduceFunction;
import org.miniflink.runtime.Collector;
import org.miniflink.runtime.Operator;
import org.miniflink.runtime.RuntimeContext;
import org.miniflink.runtime.ValueState;

/**
 * keyed 聚合算子：open 时从 RuntimeContext 取 ValueState（累加器）；
 * processElement 设 currentKey，reduce(当前累加, 输入) 更新 state 并输出 running 结果。
 */
public class ReduceOperator<IN> implements Operator<IN, IN> {
    private final ReduceFunction<IN> reduceFn;
    private Collector<IN> out;
    private RuntimeContext ctx;
    private ValueState<IN> acc;
    private KeySelector<IN, ?> keySelector;

    public ReduceOperator(ReduceFunction<IN> reduceFn) {
        this.reduceFn = reduceFn;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Override
    public void open(Collector<IN> out, RuntimeContext ctx) {
        this.out = out;
        this.ctx = ctx;
        this.acc = ctx.getStateBackend().getValueState("reduce-acc");
        this.keySelector = (KeySelector<IN, ?>) ctx.getKeySelector();
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Override
    public void processElement(IN record) throws Exception {
        Object key = keySelector.getKey(record);
        ctx.setCurrentKey(key);
        IN current = acc.value();
        IN reduced = (current == null) ? record : reduceFn.reduce(current, record);
        acc.update(reduced);
        out.collect(reduced);
    }

    @Override
    public void close() {
        // 阶段③无需操作
    }

    @Override
    public ReduceOperator<IN> copy() {
        // 共享无状态的 ReduceFunction；acc 在 open 时从 per-subtask RuntimeContext 获取
        return new ReduceOperator<>(reduceFn);
    }
}
```

- [ ] **Step 5: 运行测试验证通过**

Run: `mvn -q -Dtest=ReduceOperatorTest test`
Expected: `BUILD SUCCESS`，`Tests run: 1, Failures: 0`

- [ ] **Step 6: 提交**

```bash
git add src/main/java/org/miniflink/api/function/ReduceFunction.java \
        src/main/java/org/miniflink/runtime/operator/ReduceOperator.java \
        src/test/java/org/miniflink/runtime/operator/ReduceOperatorTest.java
git commit -m "feat(runtime): 添加 ReduceFunction 与 keyed ReduceOperator

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

## Task 5: KeyedStream + keyBy 返回 KeyedStream

**Files:**
- Create: `src/main/java/org/miniflink/api/KeyedStream.java`
- Modify: `src/main/java/org/miniflink/api/DataStream.java`（keyBy 返回 KeyedStream；加 package-private `keyedTransform`）
- Modify: `src/test/java/org/miniflink/api/DataStreamKeyByTest.java`（适配 KeyedStream）
- Test: `src/test/java/org/miniflink/api/KeyedStreamTest.java`

**Interfaces:**
- Consumes: `ReduceOperator`/`ReduceFunction`（Task 4）、`OneInputTransformation`/`HashPartitioner`（阶段②）。
- Produces: `KeyedStream<K,T>.reduce(ReduceFunction)`/`sum(ReduceFunction)`；`DataStream.keyBy` 返回 `KeyedStream<K,T>`。

- [ ] **Step 1: 写失败测试 `KeyedStreamTest`**

```java
package org.miniflink.api;

import org.junit.jupiter.api.Test;
import org.miniflink.api.function.KeySelector;
import org.miniflink.api.function.ReduceFunction;
import org.miniflink.execution.HashPartitioner;
import org.miniflink.graph.OneInputTransformation;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class KeyedStreamTest {

    @Test
    void keyBy返回KeyedStream且reduce建hash分区transformation() {
        StreamExecutionEnvironment env = new StreamExecutionEnvironment();
        KeyedStream<Integer, Integer> keyed = env.fromCollection(List.of(1, 2, 3))
                .keyBy((KeySelector<Integer, Integer>) x -> x);
        DataStream<Integer> reduced = keyed.reduce((ReduceFunction<Integer>) (a, b) -> a + b);

        OneInputTransformation<?, ?> tx = (OneInputTransformation<?, ?>) reduced.getTransformation();
        assertEquals("reduce", tx.getName());
        assertInstanceOf(HashPartitioner.class, tx.getPartitioner());
        assertNotNull(tx.getKeySelector());
    }
}
```

- [ ] **Step 2: 运行测试验证失败**

Run: `mvn -q -Dtest=KeyedStreamTest test`
Expected: 编译失败 —— `KeyedStream` 不存在 / `keyBy` 返回类型不匹配。

- [ ] **Step 3: 创建 `KeyedStream`**

```java
package org.miniflink.api;

import org.miniflink.api.function.KeySelector;
import org.miniflink.api.function.ReduceFunction;
import org.miniflink.execution.HashPartitioner;
import org.miniflink.runtime.Operator;
import org.miniflink.runtime.operator.ReduceOperator;

/**
 * keyBy 返回的流：携带 keySelector，提供 keyed 聚合操作。
 * reduce/sum 建一个 hash 分区的 OneInputTransformation（reduce 算子）。
 */
public class KeyedStream<T, K> {
    private final DataStream<T> dataStream;
    private final KeySelector<T, K> keySelector;

    public KeyedStream(DataStream<T> dataStream, KeySelector<T, K> keySelector) {
        this.dataStream = dataStream;
        this.keySelector = keySelector;
    }

    public DataStream<T> reduce(ReduceFunction<T> reduceFn) {
        ReduceOperator<T> op = new ReduceOperator<>(reduceFn);
        return dataStream.keyedTransform("reduce", op, new HashPartitioner(), keySelector);
    }

    /** 便捷：直接委托 reduce（如 sum(Integer::sum)）。 */
    public DataStream<T> sum(ReduceFunction<T> sumFn) {
        return reduce(sumFn);
    }

    public KeySelector<T, K> getKeySelector() {
        return keySelector;
    }
}
```

- [ ] **Step 4: 修改 `DataStream`（keyBy 返回 KeyedStream + 加 keyedTransform）**

在 `DataStream.java` 中：

(a) 把 `keyBy` 改为返回 `KeyedStream`：
```java
    public <K> KeyedStream<T, K> keyBy(KeySelector<T, K> keySelector) {
        return new KeyedStream<>(this, keySelector);
    }
```

(b) 新增 package-private 方法 `keyedTransform`（供 KeyedStream.reduce 建 hash+keySelector transformation）：
```java
    /** 供 KeyedStream 使用：建一个带指定分区器与 keySelector 的 transformation。 */
    <O> DataStream<O> keyedTransform(String name, Operator<T, O> operator,
                                     org.miniflink.execution.Partitioner partitioner,
                                     KeySelector<T, ?> keySelector) {
        OneInputTransformation<T, O> tx = new OneInputTransformation<>(
                env.getNewNodeId(), name, transformation, operator, partitioner, keySelector);
        env.addTransformation(tx);
        return new DataStream<>(env, tx);
    }
```

> 注：阶段②的 `keyBy` 用 `nextPartitioner`/`nextKeySelector` 字段驱动 `transform`。阶段③ `keyBy` 改返回 `KeyedStream`，原 `nextPartitioner`/`nextKeySelector` 字段可保留（不再被 keyBy 路径使用），不影响现有 `transform`。若编译告警未使用，可保留——后续任务不动。

- [ ] **Step 5: 修改 `DataStreamKeyByTest`（适配 KeyedStream）**

阶段②的 `DataStreamKeyByTest` 用 `keyBy(...).map(...)`，阶段③ `keyBy` 返回 `KeyedStream`（无 map）。把该测试整体替换为：

```java
package org.miniflink.api;

import org.junit.jupiter.api.Test;
import org.miniflink.api.function.KeySelector;
import org.miniflink.api.function.ReduceFunction;
import org.miniflink.execution.ForwardPartitioner;
import org.miniflink.execution.HashPartitioner;
import org.miniflink.graph.OneInputTransformation;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class DataStreamKeyByTest {

    @Test
    void keyBy后reduce使用hash分区() {
        StreamExecutionEnvironment env = new StreamExecutionEnvironment();
        DataStream<Integer> reduced = env.fromCollection(List.of(1, 2, 3))
                .keyBy((KeySelector<Integer, Integer>) x -> x)
                .reduce((ReduceFunction<Integer>) (a, b) -> a + b);
        OneInputTransformation<?, ?> tx = (OneInputTransformation<?, ?>) reduced.getTransformation();
        assertInstanceOf(HashPartitioner.class, tx.getPartitioner());
        assertNotNull(tx.getKeySelector());
    }

    @Test
    void reduce后的普通算子恢复forward() {
        StreamExecutionEnvironment env = new StreamExecutionEnvironment();
        DataStream<Integer> s = env.fromCollection(List.of(1, 2, 3))
                .keyBy((KeySelector<Integer, Integer>) x -> x)
                .reduce((ReduceFunction<Integer>) (a, b) -> a + b);
        DataStream<Integer> mapped = s.map(x -> x);
        OneInputTransformation<?, ?> tx = (OneInputTransformation<?, ?>) mapped.getTransformation();
        assertInstanceOf(ForwardPartitioner.class, tx.getPartitioner());
    }

    @Test
    void setParallelism应设到transformation() {
        StreamExecutionEnvironment env = new StreamExecutionEnvironment();
        DataStream<Integer> src = env.fromCollection(List.of(1, 2, 3)).setParallelism(2);
        assertEquals(2, src.getTransformation().getParallelism());
    }
}
```

- [ ] **Step 6: 运行测试验证通过 + 全量回归**

Run: `mvn -q -Dtest=KeyedStreamTest,DataStreamKeyByTest test`
Expected: `BUILD SUCCESS`，`Tests run: 4, Failures: 0`

Run: `mvn -q test`
Expected: 全量通过（阶段①② 48 + 阶段③已加，无回归）。

- [ ] **Step 7: 提交**

```bash
git add src/main/java/org/miniflink/api/KeyedStream.java \
        src/main/java/org/miniflink/api/DataStream.java \
        src/test/java/org/miniflink/api/KeyedStreamTest.java \
        src/test/java/org/miniflink/api/DataStreamKeyByTest.java
git commit -m "feat(api): 添加 KeyedStream，keyBy 返回 KeyedStream + reduce/sum

Co-Authored-By: Claude <noreply@anthropic.com>"
```

## Task 6: 阶段③验收 — WordCount（带计数的词频统计）

**说明**：spec 第①阶段验收示例 deferred 到阶段③的"带计数 WordCount"——现在 keyed state 就绪，终于能做。

**Files:**
- Test: `src/test/java/org/miniflink/examples/WordCountExampleTest.java`
- Create: `docs/examples/wordcount.md`

**Interfaces:** Consumes 全部阶段③ API（Task 1-5）。

- [ ] **Step 1: 写验收测试 `WordCountExampleTest`**

```java
package org.miniflink.examples;

import org.junit.jupiter.api.Test;
import org.miniflink.api.StreamExecutionEnvironment;
import org.miniflink.api.function.KeySelector;
import org.miniflink.api.function.ReduceFunction;
import org.miniflink.connector.CollectSink;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * 阶段③验收：带计数的词频统计 WordCount。
 * source → flatMap(分词) → map(word→WC(word,1)) → keyBy(word) → reduce(计数累加) → sink。
 */
class WordCountExampleTest {

    record WC(String word, int count) { }

    @Test
    void 词频统计正确累加() throws Exception {
        StreamExecutionEnvironment env = new StreamExecutionEnvironment();
        CollectSink<WC> sink = new CollectSink<>();

        env.fromCollection(List.of("hello world hello", "world flink"))
           .<String>flatMap((line, out) -> { for (String w : line.split(" ")) out.collect(w); })
           .map(w -> new WC(w, 1))
           .keyBy((KeySelector<WC, String>) wc -> wc.word)
           .reduce((ReduceFunction<WC>) (a, b) -> new WC(a.word, a.count + b.count))
           .addSink(sink::add);

        env.execute("wordcount");

        // reduce 输出 running 结果（每输入一条）；按 word 取最大 count（=最终值）
        Map<String, Integer> result = new HashMap<>();
        for (WC wc : sink.getResults()) {
            result.merge(wc.word, wc.count, Math::max);
        }
        assertEquals(2, result.get("hello"));
        assertEquals(2, result.get("world"));
        assertEquals(1, result.get("flink"));
    }
}
```

- [ ] **Step 2: 运行验收测试**

Run: `mvn -q -Dtest=WordCountExampleTest test`
Expected: `BUILD SUCCESS`，`Tests run: 1, Failures: 0`

- [ ] **Step 3: 创建示例文档 `docs/examples/wordcount.md`**

````markdown
# 阶段③示例：WordCount（带计数的词频统计）

演示 keyed state + 聚合（阶段③核心能力）。

## 作业逻辑

```java
env.fromCollection(List.of("hello world hello", "world flink"))
   .flatMap((line, out) -> { for (String w : line.split(" ")) out.collect(w); })  // 分词
   .map(w -> new WC(w, 1))                          // → (word, 1)
   .keyBy(wc -> wc.word)                            // 按 word 分区
   .reduce((a, b) -> new WC(a.word, a.count + b.count))  // per-key 计数累加
   .addSink(sink::add);
```

## 工作机制

- `keyBy(word)` 用 HashPartitioner 把同 word 路由到同一 subtask（KeyedStream）。
- `ReduceOperator` 持 per-key `ValueState`（累加器）：每条输入设 currentKey，`reduce(累加, 输入)` 更新 state 并输出 running 结果。
- 同 key 恒落同一 subtask → per-subtask state 内 per-key map 不会跨 subtask 分裂。
- `reduce` 输出 running（每输入一条输出当前累加），sink 收多条，最终值 = 每 key 最大 count。
````

- [ ] **Step 4: 全量测试最终回归**

Run: `mvn -q test`
Expected: `BUILD SUCCESS`，全量通过（阶段①②③）。

- [ ] **Step 5: 提交**

```bash
git add src/test/java/org/miniflink/examples/WordCountExampleTest.java \
        docs/examples/wordcount.md
git commit -m "docs(examples): 添加阶段③ WordCount 验收示例（keyed state 聚合）

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

## Self-Review

### 1. Spec 覆盖（对照 `2026-07-11-mini-flink-stage3-keyed-state-design.md`）

| spec 节 | 覆盖任务 |
|---|---|
| StateBackend + MemoryStateBackend + 三种 state + 句柄 | Task 1 |
| RuntimeContext + currentKey 机制 | Task 2 |
| Operator.open(Collector, RuntimeContext) 统一注入 | Task 3 |
| ReduceFunction + ReduceOperator（running reduce） | Task 4 |
| KeyedStream + reduce/sum | Task 5 |
| WordCount 验收 | Task 6 |

全覆盖。spec §7 final review flag（copy 浅拷贝）由 ReduceOperator.copy 共享无状态 ReduceFunction + state 在 open 内 per-subtask 获取，闭合。

### 2. Placeholder 扫描

无 TBD/TODO/"适当处理"等占位。Task 3 的 `git add -A` 是合理的（重构多文件）；Task 5 注明阶段② nextPartitioner 字段保留不阻塞编译。

### 3. 类型一致性

跨任务签名一致：`StateBackend.getValueState(name)`、`RuntimeContext.setStateKey/getStateBackend/getKeySelector`、`Operator.open(Collector, RuntimeContext)`、`SourceOperator.open(Collector, RuntimeContext)`、`OperatorChain.open(Collector, RuntimeContext)`、`KeyedStream.reduce/sum`、`ReduceOperator(ReduceFunction)`。Task 3 改的所有调用点（OperatorChain/SourceTask/OperatorTask/StreamExecutor）与新签名匹配。

### 已知简化（Self-Review 接受）

- `MemoryStateBackend` 的 currentKey 由 RuntimeContext 转发设置（非线程安全 per-key 访问——但每个 Task 单线程，安全）。
- `KeyedStream.sum` 直接委托 reduce（用户传求和函数），非按字段自动求和（阶段③无 Tuple）。
- `ReduceOperator` 输出 running reduce（每输入一条），下游收到多条；WordCount 验收取每 key 最大值。
- 阶段② `DataStream.nextPartitioner/nextKeySelector` 字段在阶段③ keyBy 改 KeyedStream 后不再被 keyBy 路径使用，保留（不影响 transform；若需可后续清理）。
