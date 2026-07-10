# Mini-Flink 阶段①（骨架）实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 搭建 mini-flink 的四层分层骨架，让 `source → map/flatMap/filter → sink` 在单线程同步链上端到端跑通。

**Architecture:** API 层（`DataStream` / `StreamExecutionEnvironment`）→ StreamGraph 层（逻辑 DAG `Transformation`）→ ExecutionGraph 层（线性算子链）→ Runtime 层（`Operator` / `SourceOperator` / `Collector` / 同步执行器）。阶段①用**同步链式调用**模型（source 产一条数据立即沿算子链流到 sink），后续阶段再把算子放进多线程 + 内存队列。

**Tech Stack:** Java 17、Maven、JUnit 5，其余纯 JDK。

## Global Constraints

- Java 17（`maven.compiler.release=17`），Maven 构建。
- 包根 `org.miniflink`；依赖仅 JUnit 5（test scope），其余用纯 JDK。
- 所有代码注释、文档、commit message 使用中文。
- 每个任务结束必须 commit；TDD（先写失败测试 → 实现 → 通过）。
- 阶段①固定单并行度、单线性链、同步执行；不实现 keyBy/状态/窗口/watermark/checkpoint（后续阶段）。

---

## File Structure（阶段①产出文件及职责）

```
mini-flink/
├── pom.xml                                    # Maven 构建，Java 17 + JUnit5
├── src/main/java/org/miniflink/
│   ├── api/
│   │   ├── StreamExecutionEnvironment.java    # 作业入口：fromCollection / execute
│   │   ├── DataStream.java                    # 流抽象：map/flatMap/filter/addSink
│   │   └── function/
│   │       ├── MapFunction.java               # 函数式接口
│   │       ├── FlatMapFunction.java           # 函数式接口（用 Collector 输出）
│   │       ├── FilterFunction.java            # 函数式接口
│   │       ├── SinkFunction.java              # 函数式接口
│   │       └── SourceFunction.java            # 函数式接口（用 SourceContext 输出）
│   ├── connector/
│   │   ├── CollectionSource.java              # 内置 source：从 Iterable 读
│   │   └── CollectSink.java                   # 内置 sink：结果收进 List
│   ├── graph/
│   │   ├── Transformation.java                # 逻辑节点抽象基类
│   │   ├── SourceTransformation.java          # source 节点（持 SourceOperator）
│   │   ├── OneInputTransformation.java        # 单输入节点（持 Operator + 上游）
│   │   └── StreamGraph.java                   # 逻辑图（transformation 集合 + sink 列表）
│   ├── execution/
│   │   └── ExecutionGraph.java                # 物理计划：单线性链 source + List<Operator>
│   └── runtime/
│       ├── Collector.java                     # 数据输出抽象（collect + close）
│       ├── Operator.java                      # 处理算子接口（open/processElement/close）
│       ├── SourceOperator.java                # source 算子接口（open/run/close）
│       ├── SourceContext.java                 # source 发数据接口（collect）
│       ├── SourceContextImpl.java             # SourceContext 实现，转发到 Collector
│       ├── OperatorOutput.java                # Collector 实现：同步调用下游 processElement
│       ├── NoopCollector.java                 # Collector 实现：丢弃（链尾/sink 用）
│       ├── StreamExecutor.java                # 执行器：组装链 → open → run → close
│       └── operator/
│           ├── MapOperator.java               # 包装 MapFunction
│           ├── FlatMapOperator.java           # 包装 FlatMapFunction
│           ├── FilterOperator.java            # 包装 FilterFunction
│           ├── SinkOperator.java              # 包装 SinkFunction（OUT = Void）
│           └── SourceOperatorImpl.java        # 包装 SourceFunction
├── src/test/java/org/miniflink/
│   ├── runtime/
│   │   └── ListCollector.java                 # 测试辅助 Collector（收集进 List）
│   ├── SmokeTest.java                         # Task1 构建验证
│   ├── runtime/operator/{Map,Filter,FlatMap,Sink,Source}OperatorTest.java
│   ├── graph/StreamGraphTest.java
│   ├── api/DataStreamApiTest.java
│   ├── execution/EndToEndExecutionTest.java
│   └── examples/TextProcessingExampleTest.java
└── docs/examples/
    └── text-processing.md                     # 阶段①可运行示例说明
```

### 跨任务类型契约（权威，后续任务必须严格匹配这些签名）

```java
// runtime
interface Collector<T>          { void collect(T record); void close(); }
interface Operator<IN, OUT>     { void open(Collector<OUT> out); void processElement(IN record) throws Exception; void close(); }
interface SourceOperator<OUT>   { void open(Collector<OUT> out); void run() throws Exception; void close(); }
interface SourceContext<T>      { void collect(T record); }

// api.function
@FunctionalInterface interface MapFunction<T, O>      { O map(T value) throws Exception; }
@FunctionalInterface interface FlatMapFunction<T, O>  { void flatMap(T value, Collector<O> out) throws Exception; }
@FunctionalInterface interface FilterFunction<T>      { boolean filter(T value) throws Exception; }
@FunctionalInterface interface SinkFunction<T>        { void invoke(T value) throws Exception; }
interface SourceFunction<T>                           { void run(SourceContext<T> ctx) throws Exception; }

// graph
abstract class Transformation<T> { int getId(); String getName(); int getParallelism(); void setParallelism(int); }
class SourceTransformation<T>    extends Transformation<T>  { SourceOperator<T> getOperator(); }
class OneInputTransformation<IN,OUT> extends Transformation<OUT> { Transformation<IN> getInput(); Operator<IN,OUT> getOperator(); }

// execution / runtime
class ExecutionGraph { static ExecutionGraph from(StreamGraph); SourceOperator<?> getSource(); List<Operator<?,?>> getOperators(); }
class StreamExecutor { void execute(ExecutionGraph) throws Exception; }

// api
class DataStream<T> { <O> DataStream<O> map(MapFunction<T,O>); <O> DataStream<O> flatMap(FlatMapFunction<T,O>); DataStream<T> filter(FilterFunction<T>); void addSink(SinkFunction<T>); Transformation<T> getTransformation(); }
class StreamExecutionEnvironment { <T> DataStream<T> fromCollection(Iterable<T>); void execute(String) throws Exception; int getNewNodeId(); }
```

---

## Task 1: Maven 脚手架与构建验证

**Files:**
- Create: `pom.xml`
- Create: `src/test/java/org/miniflink/SmokeTest.java`

**Interfaces:** Produces 可运行的 Maven 项目（`mvn test` 成功）。

- [ ] **Step 1: 创建 `pom.xml`**

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>
    <groupId>org.miniflink</groupId>
    <artifactId>mini-flink</artifactId>
    <version>0.1.0-SNAPSHOT</version>
    <packaging>jar</packaging>

    <properties>
        <maven.compiler.release>17</maven.compiler.release>
        <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
        <junit.version>5.10.2</junit.version>
    </properties>

    <dependencies>
        <dependency>
            <groupId>org.junit.jupiter</groupId>
            <artifactId>junit-jupiter</artifactId>
            <version>${junit.version}</version>
            <scope>test</scope>
        </dependency>
    </dependencies>

    <build>
        <plugins>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-surefire-plugin</artifactId>
                <version>3.2.5</version>
            </plugin>
        </plugins>
    </build>
</project>
```

- [ ] **Step 2: 创建 smoke 测试**

```java
package org.miniflink;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SmokeTest {
    @Test
    void 构建链路可用() {
        assertTrue(true);
    }
}
```

- [ ] **Step 3: 运行测试验证构建可用**

Run: `mvn -q test`
Expected: 输出 `BUILD SUCCESS`，`Tests run: 1, Failures: 0`

- [ ] **Step 4: 提交**

```bash
git add pom.xml src/test/java/org/miniflink/SmokeTest.java
git commit -m "chore: 初始化 Maven 脚手架（Java 17 + JUnit5）"
```

---

## Task 2: Collector + Operator 接口与 MapOperator

**Files:**
- Create: `src/main/java/org/miniflink/runtime/Collector.java`
- Create: `src/main/java/org/miniflink/runtime/Operator.java`
- Create: `src/main/java/org/miniflink/api/function/MapFunction.java`
- Create: `src/main/java/org/miniflink/runtime/operator/MapOperator.java`
- Create: `src/test/java/org/miniflink/runtime/ListCollector.java`
- Test: `src/test/java/org/miniflink/runtime/operator/MapOperatorTest.java`

**Interfaces:**
- Produces: `Collector<T>`（`collect`/`close`）、`Operator<IN,OUT>`（`open`/`processElement`/`close`）、`MapOperator<IN,OUT>` 构造签名 `new MapOperator<>(MapFunction<IN,OUT>)`、测试辅助 `ListCollector<T>`。

- [ ] **Step 1: 创建测试辅助 `ListCollector`**

```java
package org.miniflink.runtime;

import java.util.ArrayList;
import java.util.List;

/** 测试辅助：把 collect 的元素收集进 List。 */
public class ListCollector<T> implements Collector<T> {
    private final List<T> collected = new ArrayList<>();

    @Override
    public void collect(T record) {
        collected.add(record);
    }

    @Override
    public void close() {
        // 阶段①无需操作
    }

    public List<T> getResult() {
        return collected;
    }
}
```

- [ ] **Step 2: 写失败测试 `MapOperatorTest`**

```java
package org.miniflink.runtime.operator;

import org.junit.jupiter.api.Test;
import org.miniflink.api.function.MapFunction;
import org.miniflink.runtime.ListCollector;

import static org.junit.jupiter.api.Assertions.assertEquals;

class MapOperatorTest {

    @Test
    void 应将输入映射为输出并收集到下游() throws Exception {
        // 输入 1,2,3 → 输出 2,4,6（乘 2）
        MapOperator<Integer, Integer> op = new MapOperator<>((MapFunction<Integer, Integer>) x -> x * 2);
        ListCollector<Integer> downstream = new ListCollector<>();
        op.open(downstream);

        op.processElement(1);
        op.processElement(2);
        op.processElement(3);

        assertEquals(java.util.List.of(2, 4, 6), downstream.getResult());
    }
}
```

- [ ] **Step 3: 运行测试验证它失败**

Run: `mvn -q -Dtest=MapOperatorTest test`
Expected: 编译失败 —— `Collector` / `Operator` / `MapFunction` / `MapOperator` 不存在。

- [ ] **Step 4: 创建 `Collector` 接口**

```java
package org.miniflink.runtime;

/** 数据输出抽象：算子向下游发送数据，source 向链路发送数据。 */
public interface Collector<T> {
    void collect(T record);
    void close();
}
```

- [ ] **Step 5: 创建 `Operator` 接口**

```java
package org.miniflink.runtime;

/** 处理算子接口：接收一条输入，处理后向下游输出。 */
public interface Operator<IN, OUT> {
    void open(Collector<OUT> out);
    void processElement(IN record) throws Exception;
    void close();
}
```

- [ ] **Step 6: 创建 `MapFunction` 接口**

```java
package org.miniflink.api.function;

/** 一对一转换：每条输入产生一条输出。 */
@FunctionalInterface
public interface MapFunction<T, O> {
    O map(T value) throws Exception;
}
```

- [ ] **Step 7: 创建 `MapOperator` 实现**

```java
package org.miniflink.runtime.operator;

import org.miniflink.api.function.MapFunction;
import org.miniflink.runtime.Collector;
import org.miniflink.runtime.Operator;

/** 包装 MapFunction 的算子。 */
public class MapOperator<IN, OUT> implements Operator<IN, OUT> {
    private final MapFunction<IN, OUT> mapFunction;
    private Collector<OUT> out;

    public MapOperator(MapFunction<IN, OUT> mapFunction) {
        this.mapFunction = mapFunction;
    }

    @Override
    public void open(Collector<OUT> out) {
        this.out = out;
    }

    @Override
    public void processElement(IN record) throws Exception {
        OUT result = mapFunction.map(record);
        out.collect(result);
    }

    @Override
    public void close() {
        // 阶段①无需操作
    }
}
```

- [ ] **Step 8: 运行测试验证通过**

Run: `mvn -q -Dtest=MapOperatorTest test`
Expected: `BUILD SUCCESS`，`Tests run: 1, Failures: 0`

- [ ] **Step 9: 提交**

```bash
git add src/main/java/org/miniflink/runtime/Collector.java \
        src/main/java/org/miniflink/runtime/Operator.java \
        src/main/java/org/miniflink/api/function/MapFunction.java \
        src/main/java/org/miniflink/runtime/operator/MapOperator.java \
        src/test/java/org/miniflink/runtime/ListCollector.java \
        src/test/java/org/miniflink/runtime/operator/MapOperatorTest.java
git commit -m "feat(runtime): 添加 Collector/Operator 接口与 MapOperator"
```

---

## Task 3: FilterOperator 与 FlatMapOperator

**Files:**
- Create: `src/main/java/org/miniflink/api/function/FilterFunction.java`
- Create: `src/main/java/org/miniflink/api/function/FlatMapFunction.java`
- Create: `src/main/java/org/miniflink/runtime/operator/FilterOperator.java`
- Create: `src/main/java/org/miniflink/runtime/operator/FlatMapOperator.java`
- Test: `src/test/java/org/miniflink/runtime/operator/FilterOperatorTest.java`
- Test: `src/test/java/org/miniflink/runtime/operator/FlatMapOperatorTest.java`

**Interfaces:**
- Consumes: `Operator<IN,OUT>`、`Collector<T>`、`ListCollector<T>`（来自 Task 2）。
- Produces: `FilterOperator<IN>` 构造签名 `new FilterOperator<>(FilterFunction<IN>)`、`FlatMapOperator<IN,OUT>` 构造签名 `new FlatMapOperator<>(FlatMapFunction<IN,OUT>)`。

- [ ] **Step 1: 写失败测试 `FilterOperatorTest`**

```java
package org.miniflink.runtime.operator;

import org.junit.jupiter.api.Test;
import org.miniflink.api.function.FilterFunction;
import org.miniflink.runtime.ListCollector;

import static org.junit.jupiter.api.Assertions.assertEquals;

class FilterOperatorTest {

    @Test
    void 仅转发满足过滤条件的元素() throws Exception {
        FilterOperator<Integer> op = new FilterOperator<>((FilterFunction<Integer>) x -> x % 2 == 0);
        ListCollector<Integer> downstream = new ListCollector<>();
        op.open(downstream);

        op.processElement(1);
        op.processElement(2);
        op.processElement(3);
        op.processElement(4);

        assertEquals(java.util.List.of(2, 4), downstream.getResult());
    }
}
```

- [ ] **Step 2: 写失败测试 `FlatMapOperatorTest`**

```java
package org.miniflink.runtime.operator;

import org.junit.jupiter.api.Test;
import org.miniflink.api.function.FlatMapFunction;
import org.miniflink.runtime.ListCollector;

import static org.junit.jupiter.api.Assertions.assertEquals;

class FlatMapOperatorTest {

    @Test
    void 一条输入可产生零或多条输出() throws Exception {
        // 把句子拆成单词
        FlatMapOperator<String, String> op = new FlatMapOperator<>(
                (FlatMapFunction<String, String>) (line, out) -> {
                    for (String word : line.split(" ")) {
                        out.collect(word);
                    }
                });
        ListCollector<String> downstream = new ListCollector<>();
        op.open(downstream);

        op.processElement("hello world");
        op.processElement("mini flink");

        assertEquals(java.util.List.of("hello", "world", "mini", "flink"), downstream.getResult());
    }
}
```

- [ ] **Step 3: 运行测试验证它们失败**

Run: `mvn -q -Dtest=FilterOperatorTest,FlatMapOperatorTest test`
Expected: 编译失败 —— `FilterFunction` / `FlatMapFunction` / `FilterOperator` / `FlatMapOperator` 不存在。

- [ ] **Step 4: 创建 `FilterFunction` 接口**

```java
package org.miniflink.api.function;

/** 过滤：返回 true 的元素被保留，false 被丢弃。 */
@FunctionalInterface
public interface FilterFunction<T> {
    boolean filter(T value) throws Exception;
}
```

- [ ] **Step 5: 创建 `FlatMapFunction` 接口**

```java
package org.miniflink.api.function;

import org.miniflink.runtime.Collector;

/** 一对多转换：一条输入通过 Collector 发出零或多条输出。 */
@FunctionalInterface
public interface FlatMapFunction<T, O> {
    void flatMap(T value, Collector<O> out) throws Exception;
}
```

- [ ] **Step 6: 创建 `FilterOperator` 实现**

```java
package org.miniflink.runtime.operator;

import org.miniflink.api.function.FilterFunction;
import org.miniflink.runtime.Collector;
import org.miniflink.runtime.Operator;

/** 包装 FilterFunction 的算子：仅转发满足条件的元素。 */
public class FilterOperator<IN> implements Operator<IN, IN> {
    private final FilterFunction<IN> filterFunction;
    private Collector<IN> out;

    public FilterOperator(FilterFunction<IN> filterFunction) {
        this.filterFunction = filterFunction;
    }

    @Override
    public void open(Collector<IN> out) {
        this.out = out;
    }

    @Override
    public void processElement(IN record) throws Exception {
        if (filterFunction.filter(record)) {
            out.collect(record);
        }
    }

    @Override
    public void close() {
        // 阶段①无需操作
    }
}
```

- [ ] **Step 7: 创建 `FlatMapOperator` 实现**

```java
package org.miniflink.runtime.operator;

import org.miniflink.api.function.FlatMapFunction;
import org.miniflink.runtime.Collector;
import org.miniflink.runtime.Operator;

/** 包装 FlatMapFunction 的算子：把下游 Collector 交给用户函数发出多条结果。 */
public class FlatMapOperator<IN, OUT> implements Operator<IN, OUT> {
    private final FlatMapFunction<IN, OUT> flatMapFunction;
    private Collector<OUT> out;

    public FlatMapOperator(FlatMapFunction<IN, OUT> flatMapFunction) {
        this.flatMapFunction = flatMapFunction;
    }

    @Override
    public void open(Collector<OUT> out) {
        this.out = out;
    }

    @Override
    public void processElement(IN record) throws Exception {
        flatMapFunction.flatMap(record, out);
    }

    @Override
    public void close() {
        // 阶段①无需操作
    }
}
```

- [ ] **Step 8: 运行测试验证通过**

Run: `mvn -q -Dtest=FilterOperatorTest,FlatMapOperatorTest test`
Expected: `BUILD SUCCESS`，`Tests run: 2, Failures: 0`

- [ ] **Step 9: 提交**

```bash
git add src/main/java/org/miniflink/api/function/FilterFunction.java \
        src/main/java/org/miniflink/api/function/FlatMapFunction.java \
        src/main/java/org/miniflink/runtime/operator/FilterOperator.java \
        src/main/java/org/miniflink/runtime/operator/FlatMapOperator.java \
        src/test/java/org/miniflink/runtime/operator/FilterOperatorTest.java \
        src/test/java/org/miniflink/runtime/operator/FlatMapOperatorTest.java
git commit -m "feat(runtime): 添加 FilterOperator 与 FlatMapOperator"
```

---

## Task 4: Source 与 Sink 链路（含内置 CollectionSource）

**Files:**
- Create: `src/main/java/org/miniflink/api/function/SinkFunction.java`
- Create: `src/main/java/org/miniflink/api/function/SourceFunction.java`
- Create: `src/main/java/org/miniflink/runtime/SourceOperator.java`
- Create: `src/main/java/org/miniflink/runtime/SourceContext.java`
- Create: `src/main/java/org/miniflink/runtime/SourceContextImpl.java`
- Create: `src/main/java/org/miniflink/runtime/operator/SinkOperator.java`
- Create: `src/main/java/org/miniflink/runtime/operator/SourceOperatorImpl.java`
- Create: `src/main/java/org/miniflink/connector/CollectionSource.java`
- Test: `src/test/java/org/miniflink/runtime/operator/SinkOperatorTest.java`
- Test: `src/test/java/org/miniflink/runtime/operator/SourceOperatorImplTest.java`

**Interfaces:**
- Consumes: `Collector<T>`（Task 2）、`ListCollector<T>`（Task 2）。
- Produces: `SinkOperator<IN>` 构造 `new SinkOperator<>(SinkFunction<IN>)`；`SourceOperatorImpl<OUT>` 构造 `new SourceOperatorImpl<>(SourceFunction<OUT>)`；`SourceOperator<OUT>` 接口（`open(Collector<OUT>)` / `run()` / `close()`）；`CollectionSource<T>` 构造 `new CollectionSource<>(Iterable<T>)`。

- [ ] **Step 1: 写失败测试 `SinkOperatorTest`**

```java
package org.miniflink.runtime.operator;

import org.junit.jupiter.api.Test;
import org.miniflink.api.function.SinkFunction;
import org.miniflink.runtime.ListCollector;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SinkOperatorTest {

    @Test
    void 应把每个元素交给SinkFunction() throws Exception {
        List<String> sinked = new ArrayList<>();
        SinkOperator<String> op = new SinkOperator<>((SinkFunction<String>) sinked::add);
        op.open(new ListCollector<>()); // sink 不输出，给一个占位 Collector

        op.processElement("a");
        op.processElement("b");

        assertEquals(List.of("a", "b"), sinked);
    }
}
```

- [ ] **Step 2: 写失败测试 `SourceOperatorImplTest`**

```java
package org.miniflink.runtime.operator;

import org.junit.jupiter.api.Test;
import org.miniflink.connector.CollectionSource;
import org.miniflink.runtime.ListCollector;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SourceOperatorImplTest {

    @Test
    void run应把源数据全部输出到下游() throws Exception {
        SourceOperatorImpl<String> op = new SourceOperatorImpl<>(new CollectionSource<>(List.of("x", "y", "z")));
        ListCollector<String> downstream = new ListCollector<>();
        op.open(downstream);

        op.run();

        assertEquals(List.of("x", "y", "z"), downstream.getResult());
    }
}
```

- [ ] **Step 3: 运行测试验证它们失败**

Run: `mvn -q -Dtest=SinkOperatorTest,SourceOperatorImplTest test`
Expected: 编译失败 —— `SinkFunction` / `SourceFunction` / `SourceOperator` / `SourceContext` / `SourceContextImpl` / `SinkOperator` / `SourceOperatorImpl` / `CollectionSource` 不存在。

- [ ] **Step 4: 创建 `SinkFunction` 接口**

```java
package org.miniflink.api.function;

/** 输出端：每条到达的元素被消费（如打印、写文件）。 */
@FunctionalInterface
public interface SinkFunction<T> {
    void invoke(T value) throws Exception;
}
```

- [ ] **Step 5: 创建 `SourceFunction` 接口**

```java
package org.miniflink.api.function;

import org.miniflink.runtime.SourceContext;

/** 数据源：通过 SourceContext 向链路发出数据。 */
public interface SourceFunction<T> {
    void run(SourceContext<T> ctx) throws Exception;
}
```

- [ ] **Step 6: 创建 `SourceContext` 接口**

```java
package org.miniflink.runtime;

/** source 发数据用的上下文（只有 collect，不暴露 close）。 */
public interface SourceContext<T> {
    void collect(T record);
}
```

- [ ] **Step 7: 创建 `SourceContextImpl` 实现**

```java
package org.miniflink.runtime;

/** 把 SourceContext.collect 转发到下游 Collector。 */
public class SourceContextImpl<T> implements SourceContext<T> {
    private final Collector<T> out;

    public SourceContextImpl(Collector<T> out) {
        this.out = out;
    }

    @Override
    public void collect(T record) {
        out.collect(record);
    }
}
```

- [ ] **Step 8: 创建 `SourceOperator` 接口**

```java
package org.miniflink.runtime;

/** source 算子接口：open 设置输出，run 产生数据，close 释放资源。 */
public interface SourceOperator<OUT> {
    void open(Collector<OUT> out);
    void run() throws Exception;
    void close();
}
```

- [ ] **Step 9: 创建 `SinkOperator` 实现**

```java
package org.miniflink.runtime.operator;

import org.miniflink.api.function.SinkFunction;
import org.miniflink.runtime.Collector;
import org.miniflink.runtime.Operator;

/** 包装 SinkFunction 的算子：到达元素交给 sink 消费，无下游输出（OUT = Void）。 */
public class SinkOperator<IN> implements Operator<IN, Void> {
    private final SinkFunction<IN> sinkFunction;

    public SinkOperator(SinkFunction<IN> sinkFunction) {
        this.sinkFunction = sinkFunction;
    }

    @Override
    public void open(Collector<Void> out) {
        // sink 无下游输出
    }

    @Override
    public void processElement(IN record) throws Exception {
        sinkFunction.invoke(record);
    }

    @Override
    public void close() {
        // 阶段①无需操作
    }
}
```

- [ ] **Step 10: 创建 `SourceOperatorImpl` 实现**

```java
package org.miniflink.runtime.operator;

import org.miniflink.api.function.SourceFunction;
import org.miniflink.runtime.Collector;
import org.miniflink.runtime.SourceContext;
import org.miniflink.runtime.SourceContextImpl;
import org.miniflink.runtime.SourceOperator;

/** 包装 SourceFunction 的 source 算子：run 时把 SourceContext 交给用户函数。 */
public class SourceOperatorImpl<OUT> implements SourceOperator<OUT> {
    private final SourceFunction<OUT> sourceFunction;
    private SourceContext<OUT> ctx;

    public SourceOperatorImpl(SourceFunction<OUT> sourceFunction) {
        this.sourceFunction = sourceFunction;
    }

    @Override
    public void open(Collector<OUT> out) {
        this.ctx = new SourceContextImpl<>(out);
    }

    @Override
    public void run() throws Exception {
        sourceFunction.run(ctx);
    }

    @Override
    public void close() {
        // 阶段①无需操作
    }
}
```

- [ ] **Step 11: 创建 `CollectionSource` 内置 source**

```java
package org.miniflink.connector;

import org.miniflink.api.function.SourceFunction;
import org.miniflink.runtime.SourceContext;

/** 内置 source：从 Iterable 顺序读取数据。 */
public class CollectionSource<T> implements SourceFunction<T> {
    private final Iterable<T> data;

    public CollectionSource(Iterable<T> data) {
        this.data = data;
    }

    @Override
    public void run(SourceContext<T> ctx) {
        for (T item : data) {
            ctx.collect(item);
        }
    }
}
```

- [ ] **Step 12: 运行测试验证通过**

Run: `mvn -q -Dtest=SinkOperatorTest,SourceOperatorImplTest test`
Expected: `BUILD SUCCESS`，`Tests run: 2, Failures: 0`

- [ ] **Step 13: 提交**

```bash
git add src/main/java/org/miniflink/api/function/SinkFunction.java \
        src/main/java/org/miniflink/api/function/SourceFunction.java \
        src/main/java/org/miniflink/runtime/SourceOperator.java \
        src/main/java/org/miniflink/runtime/SourceContext.java \
        src/main/java/org/miniflink/runtime/SourceContextImpl.java \
        src/main/java/org/miniflink/runtime/operator/SinkOperator.java \
        src/main/java/org/miniflink/runtime/operator/SourceOperatorImpl.java \
        src/main/java/org/miniflink/connector/CollectionSource.java \
        src/test/java/org/miniflink/runtime/operator/SinkOperatorTest.java \
        src/test/java/org/miniflink/runtime/operator/SourceOperatorImplTest.java
git commit -m "feat(runtime): 添加 source/sink 链路与内置 CollectionSource"
```

---

## Task 5: Transformation 体系与 StreamGraph

**Files:**
- Create: `src/main/java/org/miniflink/graph/Transformation.java`
- Create: `src/main/java/org/miniflink/graph/SourceTransformation.java`
- Create: `src/main/java/org/miniflink/graph/OneInputTransformation.java`
- Create: `src/main/java/org/miniflink/graph/StreamGraph.java`
- Test: `src/test/java/org/miniflink/graph/StreamGraphTest.java`

**Interfaces:**
- Consumes: `Operator<IN,OUT>`、`SourceOperator<OUT>`（Task 2/4）。
- Produces: `Transformation<T>` 基类（`getId`/`getName`/`getParallelism`/`setParallelism`）；`SourceTransformation<T>` 构造 `new SourceTransformation<>(int id, String name, SourceOperator<T>)`、`getOperator()`；`OneInputTransformation<IN,OUT>` 构造 `new OneInputTransformation<>(int id, String name, Transformation<IN> input, Operator<IN,OUT>)`、`getInput()`/`getOperator()`；`StreamGraph` 方法 `addTransformation`/`addSink`/`getSinks`/`getTransformations`。

- [ ] **Step 1: 写失败测试 `StreamGraphTest`**

```java
package org.miniflink.graph;

import org.junit.jupiter.api.Test;
import org.miniflink.connector.CollectionSource;
import org.miniflink.runtime.operator.MapOperator;
import org.miniflink.runtime.operator.SinkOperator;
import org.miniflink.runtime.operator.SourceOperatorImpl;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class StreamGraphTest {

    @Test
    void 从sink回溯应能得到完整单线性链() {
        SourceTransformation<String> source = new SourceTransformation<>(
                1, "source", new SourceOperatorImpl<>(new CollectionSource<>(List.of("a"))));
        OneInputTransformation<String, String> map = new OneInputTransformation<>(
                2, "map", source, new MapOperator<>(x -> x + "!"));
        OneInputTransformation<String, Void> sink = new OneInputTransformation<>(
                3, "sink", map, new SinkOperator<>(v -> {}));

        StreamGraph graph = new StreamGraph();
        graph.addTransformation(source);
        graph.addTransformation(map);
        graph.addSink(sink);

        assertEquals(1, graph.getSinks().size());
        assertSame(sink, graph.getSinks().get(0));
        // sink 的 input 是 map，map 的 input 是 source
        assertSame(map, ((OneInputTransformation<?, ?>) graph.getSinks().get(0)).getInput());
    }
}
```

- [ ] **Step 2: 运行测试验证它失败**

Run: `mvn -q -Dtest=StreamGraphTest test`
Expected: 编译失败 —— `Transformation` / `SourceTransformation` / `OneInputTransformation` / `StreamGraph` 不存在。

- [ ] **Step 3: 创建 `Transformation` 抽象基类**

```java
package org.miniflink.graph;

/** 逻辑 DAG 节点的抽象基类。 */
public abstract class Transformation<T> {
    private final int id;
    private final String name;
    private int parallelism = 1;

    protected Transformation(int id, String name) {
        this.id = id;
        this.name = name;
    }

    public int getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public int getParallelism() {
        return parallelism;
    }

    public void setParallelism(int parallelism) {
        this.parallelism = parallelism;
    }
}
```

- [ ] **Step 4: 创建 `SourceTransformation`**

```java
package org.miniflink.graph;

import org.miniflink.runtime.SourceOperator;

/** source 节点：持有 SourceOperator。 */
public class SourceTransformation<T> extends Transformation<T> {
    private final SourceOperator<T> operator;

    public SourceTransformation(int id, String name, SourceOperator<T> operator) {
        super(id, name);
        this.operator = operator;
    }

    public SourceOperator<T> getOperator() {
        return operator;
    }
}
```

- [ ] **Step 5: 创建 `OneInputTransformation`**

```java
package org.miniflink.graph;

import org.miniflink.runtime.Operator;

/** 单输入节点：持有一个处理算子及其上游 input。 */
public class OneInputTransformation<IN, OUT> extends Transformation<OUT> {
    private final Transformation<IN> input;
    private final Operator<IN, OUT> operator;

    public OneInputTransformation(int id, String name, Transformation<IN> input, Operator<IN, OUT> operator) {
        super(id, name);
        this.input = input;
        this.operator = operator;
    }

    public Transformation<IN> getInput() {
        return input;
    }

    public Operator<IN, OUT> getOperator() {
        return operator;
    }
}
```

- [ ] **Step 6: 创建 `StreamGraph`**

```java
package org.miniflink.graph;

import java.util.ArrayList;
import java.util.List;

/** 逻辑图：收集所有 transformation 与 sink 节点。 */
public class StreamGraph {
    private final List<Transformation<?>> transformations = new ArrayList<>();
    private final List<Transformation<?>> sinks = new ArrayList<>();

    public void addTransformation(Transformation<?> t) {
        transformations.add(t);
    }

    public void addSink(Transformation<?> sink) {
        sinks.add(sink);
    }

    public List<Transformation<?>> getTransformations() {
        return transformations;
    }

    public List<Transformation<?>> getSinks() {
        return sinks;
    }
}
```

- [ ] **Step 7: 运行测试验证通过**

Run: `mvn -q -Dtest=StreamGraphTest test`
Expected: `BUILD SUCCESS`，`Tests run: 1, Failures: 0`

- [ ] **Step 8: 提交**

```bash
git add src/main/java/org/miniflink/graph/ \
        src/test/java/org/miniflink/graph/StreamGraphTest.java
git commit -m "feat(graph): 添加 Transformation 体系与 StreamGraph"
```

---

## Task 6: DataStream 与 StreamExecutionEnvironment API

**Files:**
- Create: `src/main/java/org/miniflink/api/DataStream.java`
- Create: `src/main/java/org/miniflink/api/StreamExecutionEnvironment.java`
- Test: `src/test/java/org/miniflink/api/DataStreamApiTest.java`

**Interfaces:**
- Consumes: 全部函数接口与算子（Task 2–4）、`Transformation` 体系（Task 5）、`CollectionSource`（Task 4）。
- Produces: `DataStream<T>`（`map`/`flatMap`/`filter`/`addSink`/`getTransformation`）；`StreamExecutionEnvironment`（`fromCollection`/`execute`/`getNewNodeId`）。
- 注：本任务**不实现 `execute` 的运行逻辑**（委托给 Task 7 的 `ExecutionGraph` + `StreamExecutor`）；`execute` 体内在 Task 7 补全。此处先写一个抛 `UnsupportedOperationException` 的占位，Task 7 替换。

- [ ] **Step 1: 写失败测试 `DataStreamApiTest`**

```java
package org.miniflink.api;

import org.junit.jupiter.api.Test;
import org.miniflink.graph.OneInputTransformation;
import org.miniflink.graph.SourceTransformation;
import org.miniflink.graph.Transformation;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class DataStreamApiTest {

    @Test
    void 链式调用应构建正确的逻辑链结构() {
        StreamExecutionEnvironment env = new StreamExecutionEnvironment();
        DataStream<String> source = env.fromCollection(List.of("a", "b"));
        DataStream<Integer> mapped = source.map(String::length);
        mapped.addSink(v -> {});

        Transformation<?> sinkTx = source.getTransformation(); // 占位，下面用真实断言
        // 从 source 节点开始，逐步校验链：source → map → sink
        SourceTransformation<?> srcNode = (SourceTransformation<?>) source.getTransformation();
        OneInputTransformation<?, ?> mapNode = (OneInputTransformation<?, ?>) mapped.getTransformation();
        assertSame(srcNode, mapNode.getInput(), "map 的上游应为 source");
        assertEquals("map", mapNode.getName());
    }

    @Test
    void filter应复用同一上游并返回新流() {
        StreamExecutionEnvironment env = new StreamExecutionEnvironment();
        DataStream<Integer> source = env.fromCollection(List.of(1, 2, 3));
        DataStream<Integer> filtered = source.filter(x -> x > 1);

        OneInputTransformation<?, ?> fNode = (OneInputTransformation<?, ?>) filtered.getTransformation();
        assertSame(source.getTransformation(), fNode.getInput());
        assertEquals("filter", fNode.getName());
    }
}
```

- [ ] **Step 2: 运行测试验证它们失败**

Run: `mvn -q -Dtest=DataStreamApiTest test`
Expected: 编译失败 —— `DataStream` / `StreamExecutionEnvironment` 不存在。

- [ ] **Step 3: 创建 `DataStream`**

```java
package org.miniflink.api;

import org.miniflink.api.function.FilterFunction;
import org.miniflink.api.function.FlatMapFunction;
import org.miniflink.api.function.MapFunction;
import org.miniflink.api.function.SinkFunction;
import org.miniflink.graph.OneInputTransformation;
import org.miniflink.graph.Transformation;
import org.miniflink.runtime.Operator;
import org.miniflink.runtime.operator.FilterOperator;
import org.miniflink.runtime.operator.FlatMapOperator;
import org.miniflink.runtime.operator.MapOperator;
import org.miniflink.runtime.operator.SinkOperator;

/** 流抽象：链式调用算子方法，内部累积逻辑 transformation。 */
public class DataStream<T> {
    private final StreamExecutionEnvironment env;
    private final Transformation<T> transformation;

    public DataStream(StreamExecutionEnvironment env, Transformation<T> transformation) {
        this.env = env;
        this.transformation = transformation;
    }

    public Transformation<T> getTransformation() {
        return transformation;
    }

    public <O> DataStream<O> map(MapFunction<T, O> mapper) {
        return transform("map", new MapOperator<>(mapper));
    }

    public <O> DataStream<O> flatMap(FlatMapFunction<T, O> flatMapper) {
        return transform("flatMap", new FlatMapOperator<>(flatMapper));
    }

    public DataStream<T> filter(FilterFunction<T> filter) {
        return transform("filter", new FilterOperator<>(filter));
    }

    public void addSink(SinkFunction<T> sinkFunction) {
        OneInputTransformation<T, Void> sink = new OneInputTransformation<>(
                env.getNewNodeId(), "sink", transformation, new SinkOperator<>(sinkFunction));
        env.addSink(sink);
    }

    /** 通用单输入变换：创建 transformation、注册到 env、返回新的 DataStream。 */
    private <O> DataStream<O> transform(String name, Operator<T, O> operator) {
        OneInputTransformation<T, O> tx = new OneInputTransformation<>(
                env.getNewNodeId(), name, transformation, operator);
        env.addTransformation(tx);
        return new DataStream<>(env, tx);
    }
}
```

- [ ] **Step 4: 创建 `StreamExecutionEnvironment`（execute 暂为占位）**

```java
package org.miniflink.api;

import org.miniflink.connector.CollectionSource;
import org.miniflink.graph.SourceTransformation;
import org.miniflink.graph.StreamGraph;
import org.miniflink.graph.Transformation;
import org.miniflink.runtime.operator.SourceOperatorImpl;

import java.util.concurrent.atomic.AtomicInteger;

/** 作业入口：构建 DAG 并触发执行。 */
public class StreamExecutionEnvironment {
    private final AtomicInteger idCounter = new AtomicInteger(0);
    private final StreamGraph streamGraph = new StreamGraph();

    public int getNewNodeId() {
        return idCounter.incrementAndGet();
    }

    public <T> DataStream<T> fromCollection(Iterable<T> data) {
        SourceTransformation<T> source = new SourceTransformation<>(
                getNewNodeId(), "source", new SourceOperatorImpl<>(new CollectionSource<>(data)));
        streamGraph.addTransformation(source);
        return new DataStream<>(this, source);
    }

    public void addTransformation(Transformation<?> t) {
        streamGraph.addTransformation(t);
    }

    public void addSink(Transformation<?> sink) {
        streamGraph.addSink(sink);
    }

    StreamGraph getStreamGraph() {
        return streamGraph;
    }

    /** 编译逻辑图并执行。运行逻辑在 Task 7（ExecutionGraph + StreamExecutor）补全。 */
    public void execute(String jobName) throws Exception {
        throw new UnsupportedOperationException("execute 将在 Task 7 实现");
    }
}
```

- [ ] **Step 5: 运行测试验证通过**

Run: `mvn -q -Dtest=DataStreamApiTest test`
Expected: `BUILD SUCCESS`，`Tests run: 2, Failures: 0`

- [ ] **Step 6: 提交**

```bash
git add src/main/java/org/miniflink/api/DataStream.java \
        src/main/java/org/miniflink/api/StreamExecutionEnvironment.java \
        src/test/java/org/miniflink/api/DataStreamApiTest.java
git commit -m "feat(api): 添加 DataStream 与 StreamExecutionEnvironment（execute 待 Task7）"
```

---

## Task 7: ExecutionGraph + 同步执行器（端到端跑通）

**Files:**
- Create: `src/main/java/org/miniflink/execution/ExecutionGraph.java`
- Create: `src/main/java/org/miniflink/runtime/NoopCollector.java`
- Create: `src/main/java/org/miniflink/runtime/OperatorOutput.java`
- Create: `src/main/java/org/miniflink/runtime/StreamExecutor.java`
- Create: `src/main/java/org/miniflink/connector/CollectSink.java`
- Modify: `src/main/java/org/miniflink/api/StreamExecutionEnvironment.java`（替换 `execute` 占位）
- Test: `src/test/java/org/miniflink/execution/EndToEndExecutionTest.java`

**Interfaces:**
- Consumes: `StreamGraph`、`Transformation` 体系（Task 5）、`Operator`/`SourceOperator`/`Collector`（Task 2–4）。
- Produces: `ExecutionGraph.from(StreamGraph)`、`ExecutionGraph.getSource()`/`getOperators()`；`StreamExecutor.execute(ExecutionGraph)`；`CollectSink<T>`（构造 `new CollectSink<>()`，字段 `List<T> results`）。

- [ ] **Step 1: 写失败测试 `EndToEndExecutionTest`**

```java
package org.miniflink.execution;

import org.junit.jupiter.api.Test;
import org.miniflink.api.StreamExecutionEnvironment;
import org.miniflink.connector.CollectSink;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class EndToEndExecutionTest {

    @Test
    void source_map_filter_sink应端到端跑通() throws Exception {
        StreamExecutionEnvironment env = new StreamExecutionEnvironment();
        CollectSink<Integer> sink = new CollectSink<>();

        env.fromCollection(List.of(1, 2, 3, 4, 5))
           .map(x -> x * 10)
           .filter(x -> x > 20)
           .addSink(sink::add);

        env.execute("demo-job");

        assertEquals(List.of(30, 40, 50), sink.getResults());
    }

    @Test
    void flatMap链应端到端跑通() throws Exception {
        StreamExecutionEnvironment env = new StreamExecutionEnvironment();
        CollectSink<String> sink = new CollectSink<>();

        env.fromCollection(List.of("a b", "c"))
           .flatMap((line, out) -> { for (String w : line.split(" ")) out.collect(w); })
           .map(String::toUpperCase)
           .addSink(sink::add);

        env.execute("flatmap-job");

        assertEquals(List.of("A", "B", "C"), sink.getResults());
    }
}
```

- [ ] **Step 2: 运行测试验证它们失败**

Run: `mvn -q -Dtest=EndToEndExecutionTest test`
Expected: 失败 —— `execute` 抛 `UnsupportedOperationException`（或编译失败，因 `CollectSink` 不存在）。

- [ ] **Step 3: 创建 `CollectSink` 内置 sink**

```java
package org.miniflink.connector;

import org.miniflink.api.function.SinkFunction;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** 内置 sink：把到达的元素收集进 List，供测试断言。 */
public class CollectSink<T> implements SinkFunction<T> {
    private final List<T> results = Collections.synchronizedList(new ArrayList<>());

    @Override
    public void invoke(T value) {
        results.add(value);
    }

    public List<T> getResults() {
        return results;
    }
}
```

- [ ] **Step 4: 创建 `NoopCollector`**

```java
package org.miniflink.runtime;

/** 丢弃所有元素的 Collector，用作链尾（sink 算子的下游）。 */
public class NoopCollector<T> implements Collector<T> {
    @Override
    public void collect(T record) {
        // 丢弃
    }

    @Override
    public void close() {
        // 无操作
    }
}
```

- [ ] **Step 5: 创建 `OperatorOutput`（同步链式连接）**

```java
package org.miniflink.runtime;

/** Collector 实现：collect 时同步调用下游算子的 processElement（阶段①同步链）。 */
public class OperatorOutput<T> implements Collector<T> {
    private final Operator<T, ?> next;

    public OperatorOutput(Operator<T, ?> next) {
        this.next = next;
    }

    @Override
    public void collect(T record) {
        try {
            next.processElement(record);
        } catch (Exception e) {
            // 阶段①把算子异常包装成 RuntimeException 向上传播
            throw new RuntimeException("算子执行异常", e);
        }
    }

    @Override
    public void close() {
        // 下游算子的 close 由执行器统一调用
    }
}
```

- [ ] **Step 6: 创建 `ExecutionGraph`**

```java
package org.miniflink.execution;

import org.miniflink.graph.OneInputTransformation;
import org.miniflink.graph.SourceTransformation;
import org.miniflink.graph.StreamGraph;
import org.miniflink.graph.Transformation;
import org.miniflink.runtime.Operator;
import org.miniflink.runtime.SourceOperator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 物理执行计划。阶段①：单线性链（一个 sink），从 sink 回溯到 source，
 * 产出 source + 正向算子序列（source 之后：op1, op2, ..., sink）。
 */
public class ExecutionGraph {
    private final SourceOperator<?> source;
    private final List<Operator<?, ?>> operators;

    public ExecutionGraph(SourceOperator<?> source, List<Operator<?, ?>> operators) {
        this.source = source;
        this.operators = operators;
    }

    public SourceOperator<?> getSource() {
        return source;
    }

    public List<Operator<?, ?>> getOperators() {
        return operators;
    }

    /** 从 StreamGraph 构建单线性链。 */
    public static ExecutionGraph from(StreamGraph streamGraph) {
        List<Transformation<?>> sinks = streamGraph.getSinks();
        if (sinks.size() != 1) {
            throw new IllegalStateException("阶段①仅支持单个 sink，当前 sinks=" + sinks.size());
        }

        List<Operator<?, ?>> chain = new ArrayList<>();
        Transformation<?> current = sinks.get(0);
        while (current instanceof OneInputTransformation<?, ?> oneInput) {
            chain.add(oneInput.getOperator());
            current = oneInput.getInput();
        }
        if (!(current instanceof SourceTransformation<?> sourceTx)) {
            throw new IllegalStateException("链回溯未终止于 source 节点");
        }

        // chain 当前顺序：[sink 算子, ..., 第一个算子]，反转后正向
        Collections.reverse(chain);
        return new ExecutionGraph(sourceTx.getOperator(), chain);
    }
}
```

- [ ] **Step 7: 创建 `StreamExecutor`**

```java
package org.miniflink.runtime;

import org.miniflink.execution.ExecutionGraph;

import java.util.List;

/**
 * 阶段①同步执行器：把 source 与算子链用 OperatorOutput 串成同步链，
 * open 全部算子后调用 source.run() 触发数据流动，最后统一 close。
 *
 * 注：为避免泛型噪声，链组装使用 raw type + 受检转换；阶段②引入多线程时会重构为 Task。
 */
public class StreamExecutor {

    public void execute(ExecutionGraph graph) throws Exception {
        SourceOperator<?> source = graph.getSource();
        List<Operator<?, ?>> operators = graph.getOperators();
        int n = operators.size();

        // 为每个算子准备它的输出 Collector：
        // 最后一个算子（sink）→ NoopCollector；其余 → OperatorOutput(下一个算子)
        @SuppressWarnings("rawtypes")
        Collector[] outputs = new Collector[n];
        for (int i = n - 1; i >= 0; i--) {
            if (i == n - 1) {
                outputs[i] = new NoopCollector<>();
            } else {
                outputs[i] = new OperatorOutput<>(operators.get(i + 1));
            }
        }

        // open 所有算子（按正序）
        for (int i = 0; i < n; i++) {
            @SuppressWarnings({"rawtypes", "unchecked"})
            Operator op = operators.get(i);
            op.open(outputs[i]);
        }

        // source 的输出连到第一个算子（链为空时丢弃）
        @SuppressWarnings("rawtypes")
        Collector sourceOut = (n == 0) ? new NoopCollector<>() : new OperatorOutput(operators.get(0));
        @SuppressWarnings("unchecked")
        Collector<Object> typedSourceOut = (Collector<Object>) sourceOut;
        source.open(typedSourceOut);

        try {
            source.run();
        } finally {
            // 关闭：source、所有算子、所有输出
            source.close();
            for (Operator<?, ?> op : operators) {
                op.close();
            }
            for (int i = 0; i < n; i++) {
                outputs[i].close();
            }
            typedSourceOut.close();
        }
    }
}
```

- [ ] **Step 8: 修改 `StreamExecutionEnvironment.execute`，接入编译与执行**

将 `StreamExecutionEnvironment.java` 中 `execute` 方法替换为：

```java
    public void execute(String jobName) throws Exception {
        ExecutionGraph execGraph = ExecutionGraph.from(streamGraph);
        new StreamExecutor().execute(execGraph);
    }
```

并在文件顶部补充 import：

```java
import org.miniflink.execution.ExecutionGraph;
import org.miniflink.runtime.StreamExecutor;
```

- [ ] **Step 9: 运行端到端测试验证通过**

Run: `mvn -q -Dtest=EndToEndExecutionTest test`
Expected: `BUILD SUCCESS`，`Tests run: 2, Failures: 0`

- [ ] **Step 10: 运行全部测试确保无回归**

Run: `mvn -q test`
Expected: `BUILD SUCCESS`，所有测试通过。

- [ ] **Step 11: 提交**

```bash
git add src/main/java/org/miniflink/execution/ExecutionGraph.java \
        src/main/java/org/miniflink/runtime/NoopCollector.java \
        src/main/java/org/miniflink/runtime/OperatorOutput.java \
        src/main/java/org/miniflink/runtime/StreamExecutor.java \
        src/main/java/org/miniflink/connector/CollectSink.java \
        src/main/java/org/miniflink/api/StreamExecutionEnvironment.java \
        src/test/java/org/miniflink/execution/EndToEndExecutionTest.java
git commit -m "feat(execution): 添加 ExecutionGraph 与同步 StreamExecutor，端到端跑通"
```

---

## Task 8: 阶段①验收示例（文本处理）与文档

**说明（与 spec 的偏差）：** spec 第①阶段验收示例写作「词频统计」，但词频统计需要**计数聚合**，而阶段①没有 keyed state / reduce（阶段③才有）。因此阶段①验收示例改为**纯转换链**（source → flatMap 分词 → filter 过滤 → map 转大写 → sink），完整演示阶段①全部算子能力；带计数的 WordCount 留到阶段③。spec 的验收示例描述需相应更新（见文末「Spec 偏差记录」）。

**Files:**
- Create: `src/test/java/org/miniflink/examples/TextProcessingExampleTest.java`（示例以测试形式固化，可直接 `mvn test` 跑）
- Create: `docs/examples/text-processing.md`

**Interfaces:** Consumes 全部 API（Task 6/7）。

- [ ] **Step 1: 写示例测试（即验收用例）**

```java
package org.miniflink.examples;

import org.junit.jupiter.api.Test;
import org.miniflink.api.StreamExecutionEnvironment;
import org.miniflink.connector.CollectSink;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * 阶段①验收示例：文本处理流水线。
 * 演示 source → flatMap(分词) → filter(过滤短词) → map(转大写) → sink 的完整链路。
 */
class TextProcessingExampleTest {

    @Test
    void 文本处理流水线应产出预期结果() throws Exception {
        StreamExecutionEnvironment env = new StreamExecutionEnvironment();
        CollectSink<String> sink = new CollectSink<>();

        env.fromCollection(List.of("hello world", "hi there", "go"))
           .flatMap((line, out) -> { for (String w : line.split(" ")) out.collect(w); })
           .filter(w -> w.length() > 2)          // 过滤长度 <=2 的词
           .map(String::toUpperCase)
           .addSink(sink::add);

        env.execute("text-processing");

        // hello(5) world(5) hi(2,过滤) there(5) go(2,过滤)
        assertEquals(List.of("HELLO", "WORLD", "THERE"), sink.getResults());
    }
}
```

- [ ] **Step 2: 运行验收示例**

Run: `mvn -q -Dtest=TextProcessingExampleTest test`
Expected: `BUILD SUCCESS`，`Tests run: 1, Failures: 0`

- [ ] **Step 3: 创建示例文档 `docs/examples/text-processing.md`**

````markdown
# 阶段①示例：文本处理流水线

本示例演示 mini-flink 阶段①（骨架）的端到端能力。

## 作业逻辑

```java
StreamExecutionEnvironment env = new StreamExecutionEnvironment();
CollectSink<String> sink = new CollectSink<>();

env.fromCollection(List.of("hello world", "hi there", "go"))
   .flatMap((line, out) -> { for (String w : line.split(" ")) out.collect(w); })  // 分词
   .filter(w -> w.length() > 2)                                                    // 过滤短词
   .map(String::toUpperCase)                                                       // 转大写
   .addSink(sink::add);                                                            // 收集结果

env.execute("text-processing");
// 结果：[HELLO, WORLD, THERE]
```

## 执行流程（对应四层）

1. **API 层**：`DataStream` 链式调用累积 `Transformation`。
2. **StreamGraph 层**：`execute` 时逻辑 DAG 就绪（source → flatMap → filter → map → sink）。
3. **ExecutionGraph 层**：从 sink 回溯成单线性链 `source + [flatMap, filter, map, sink]`。
4. **Runtime 层**：`StreamExecutor` 用 `OperatorOutput` 串成同步链，`source.run()` 触发数据同步流过到 sink。

## 阶段①已实现 / 未实现

- 已实现：source / map / flatMap / filter / sink、单线性链、同步执行。
- 未实现（后续阶段）：并行多线程、keyBy 与 keyed state、窗口与 watermark、checkpoint。
````

- [ ] **Step 4: 运行全部测试做最终回归**

Run: `mvn -q test`
Expected: `BUILD SUCCESS`，全部测试通过（约 10 个测试方法）。

- [ ] **Step 5: 提交**

```bash
git add src/test/java/org/miniflink/examples/TextProcessingExampleTest.java \
        docs/examples/text-processing.md
git commit -m "docs(examples): 添加阶段①文本处理验收示例与说明"
```

---

## Self-Review 结论

**1. Spec 覆盖：** spec 第①阶段要求「source→map→sink 单线程串行跑通；四层骨架」——Task 1–7 覆盖四层与端到端执行，Task 8 提供验收示例。flatMap/filter 虽非 spec 第①阶段最小集，但属「核心流处理模型」（spec 第2节），随骨架一并实现合理。✅

**2. Placeholder 扫描：** 无 TBD/TODO/「适当处理异常」等占位。`execute` 的占位（Task 6）是刻意的两段式实现，Task 7 明确替换，非遗留占位。✅

**3. 类型一致性：** 全部跨任务签名已在「跨任务类型契约」与各任务 Interfaces 块锁定并交叉核对：`Collector.collect/close`、`Operator.open/processElement/close`、`SourceOperator.open/run/close`、算子构造签名、`Transformation` 子类 getter、`ExecutionGraph.from`、`StreamExecutor.execute`、`DataStream`/`StreamExecutionEnvironment` 方法名——一致。✅

## Spec 偏差记录（需回填 spec）

- **第①阶段验收示例**：spec 写「词频统计（无状态，单并行度）」。因阶段①无状态聚合算子，本计划改为「文本处理流水线（纯转换链）」。**建议**在执行完阶段①后，更新 `docs/superpowers/specs/2026-07-10-mini-flink-design.md` 第10阶段表格第①行的验收示例描述，并注明「带计数的 WordCount 在阶段③实现」。
