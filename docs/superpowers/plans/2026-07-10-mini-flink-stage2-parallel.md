# Mini-Flink 阶段②（并行执行）实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把阶段①的单线程同步链改造为多线程管道——每个 subtask 一个线程，subtask 间用有界 `Channel` 连接（天然反压），支持 `parallelism` 展开与分区（forward/hash/rebalance）、算子链，以及 EOB 哨兵 + 引用计数的级联关闭。

**Architecture:** 引入 `StreamElement`/`Record`/`EndOfBroadcast` 作为通道元素；`Channel`（有界 `BlockingQueue`）连接 `Task`；`OperatorChain` 把同并行度 forward 算子合并进同一 Task（链内直接调用、链间走 Channel）；`ExecutionGraph.from` 按 parallelism 展开为 `ExecutionVertex` + 链化 + 标分区边；`StreamExecutor` 建 Channel/Task、启动线程、join 等待。`Collector` 接口保持不变（稳定边界），其实现换成 `ChannelWriter`/`OutputCollector`。

**Tech Stack:** Java 17、Maven、JUnit 5、纯 JDK（`java.util.concurrent`）。

## Global Constraints

- Java 17（`maven.compiler.release=17`），Maven 构建。
- 包根 `org.miniflink`；依赖仅 JUnit 5（test scope），其余纯 JDK。
- 所有代码注释、文档、commit message 使用中文。
- 每个任务结束必须 commit；TDD（先写失败测试 → 实现 → 通过）。
- `Collector<T>` 接口**不可改动**（稳定边界）；新通道行为通过新实现类提供。
- 阶段②仍维持**单线性链 + 单 sink**（与阶段①一致；多 sink/union 留后续）。

---

## File Structure（阶段②新增/修改）

```
src/main/java/org/miniflink/
├── api/function/KeySelector.java          # 新增（keyBy 用）
├── api/DataStream.java                     # 修改：setParallelism + keyBy + 输出分区器字段
├── connector/CollectionSource.java         # 修改：并行分片（按 subtaskIndex 取模）
├── connector/CollectSink.java              # 修改：getResults 返回 unmodifiableList
├── graph/OneInputTransformation.java       # 修改：加 partitioner + keySelector 字段
├── graph/StreamGraph.java                  # 修改：getter 返回 unmodifiableList
├── execution/Partitioner.java              # 新增（接口）
├── execution/ForwardPartitioner.java       # 新增
├── execution/HashPartitioner.java          # 新增
├── execution/RebalancePartitioner.java     # 新增
├── execution/ExecutionVertex.java          # 新增
├── execution/ExecutionEdge.java            # 新增
├── execution/ExecutionGraph.java           # 重构：from 按 parallelism 展开 + 链化 + 分区边
└── runtime/
    ├── StreamElement.java                  # 新增（接口）
    ├── Record.java                         # 新增（record<T>）
    ├── EndOfBroadcast.java                 # 新增（EOB 单例哨兵）
    ├── Channel.java                        # 新增（有界 BlockingQueue 包装）
    ├── ChannelWriter.java                  # 新增（Collector 实现，写 Record 到 Channel）
    ├── ChainCollector.java                 # 新增（链内 forward，调下游算子）
    ├── OperatorChain.java                  # 新增（链化算子序列）
    ├── Output.java                         # 新增（fan-out 目标：下游 Channel + 分区器 + keySelector）
    ├── OutputCollector.java                # 新增（Collector 实现，按分区器路由到多 Channel）
    ├── Task.java                           # 新增（Runnable 接口）
    ├── SourceTask.java                     # 新增
    ├── OperatorTask.java                   # 新增（EOB 引用计数对齐）
    ├── SourceContext.java                  # 修改：加 getSubtaskIndex/getParallelism
    ├── SourceContextImpl.java              # 修改：持有 subtaskIndex + parallelism
    ├── SourceOperator.java                 # 修改：open 加 subtaskIndex + parallelism 参数
    ├── operator/SourceOperatorImpl.java    # 修改：open 传 index/parallelism 给 SourceContextImpl
    ├── StreamExecutor.java                 # 重构：多线程 Task + join
    └── OperatorOutput.java                 # 删除（被 ChainCollector/ChannelWriter 取代）
```

### 跨任务类型契约（权威，后续任务严格匹配）

```java
// runtime（新增）
interface StreamElement { }
record Record<T>(T value) implements StreamElement { }
final class EndOfBroadcast implements StreamElement { public static final EndOfBroadcast INSTANCE; }

class Channel {
    static final int DEFAULT_CAPACITY = 64;
    Channel(int capacity); Channel();
    void send(StreamElement e) throws InterruptedException;   // put，满则阻塞
    StreamElement receive() throws InterruptedException;       // take，空则阻塞
}

class ChannelWriter<T> implements Collector<T> { ChannelWriter(Channel); }   // collect→Record→send
class ChainCollector<T> implements Collector<T> { ChainCollector(Operator<T,?>); }  // collect→下游 processElement

class OperatorChain<IN,OUT> {
    OperatorChain(List<Operator<?,?>> operators);
    void open(Collector<OUT> output);
    void processElement(IN record) throws Exception;
    void close();
}

class Output {  // 一个 fan-out 目标
    Output(List<Channel> downstreamChannels, Partitioner partitioner, KeySelector<?,?> keySelector);
    void route(Object value, int upstreamIndex) throws Exception;
    List<Channel> getDownstreamChannels();
}
class OutputCollector<T> implements Collector<T> { OutputCollector(List<Output> outputs, int upstreamIndex); }

interface Task extends Runnable { }
class SourceTask implements Task { SourceTask(SourceOperator<?> sourceOperator, List<Output> outputs, int sourceIndex); }
class OperatorTask implements Task { OperatorTask(OperatorChain<?,?> chain, Channel input, int pendingUpstreams, List<Output> outputs, int subtaskIndex); }

// execution（新增/修改）
interface Partitioner { int selectChannel(int numDownstream, Object key, int upstreamIndex); }
class ForwardPartitioner implements Partitioner { }     // 返回 upstreamIndex
class HashPartitioner implements Partitioner { }        // floorMod(key.hashCode(), numDownstream)
class RebalancePartitioner implements Partitioner { }   // AtomicInteger 轮询

class ExecutionVertex {
    ExecutionVertex(int id, int subtaskIndex, int parallelism, List<Operator<?,?>> operators, SourceOperator<?> sourceOperator);
    boolean isSource(); int getSubtaskIndex(); int getParallelism();
    List<Operator<?,?>> getOperators(); SourceOperator<?> getSourceOperator();
}
class ExecutionEdge {
    ExecutionEdge(List<ExecutionVertex> sources, List<ExecutionVertex> targets, Partitioner partitioner);
}
class ExecutionGraph {
    ExecutionGraph(List<ExecutionVertex> vertices, List<ExecutionEdge> edges);
    static ExecutionGraph from(StreamGraph streamGraph);
    List<ExecutionVertex> getVertices(); List<ExecutionEdge> getEdges();
}

// runtime 修改（SourceOperator / SourceContext 加并行信息）
interface SourceOperator<OUT> { void open(Collector<OUT> out, int subtaskIndex, int parallelism); void run() throws Exception; void close(); }
interface SourceContext<T> { void collect(T record); int getSubtaskIndex(); int getParallelism(); }

// api/function 新增
@FunctionalInterface interface KeySelector<T, K> { K getKey(T value) throws Exception; }

// graph 修改
class OneInputTransformation<IN,OUT> extends Transformation<OUT> {
    OneInputTransformation(int id, String name, Transformation<IN> input, Operator<IN,OUT> operator);
    Partitioner getPartitioner(); KeySelector<?,?> getKeySelector();
}
```

---

## Task 1: StreamElement + Record + EndOfBroadcast

**Files:**
- Create: `src/main/java/org/miniflink/runtime/StreamElement.java`
- Create: `src/main/java/org/miniflink/runtime/Record.java`
- Create: `src/main/java/org/miniflink/runtime/EndOfBroadcast.java`
- Test: `src/test/java/org/miniflink/runtime/StreamElementTest.java`

**Interfaces:**
- Produces: `StreamElement`（接口）、`Record<T>(T value)`、`EndOfBroadcast.INSTANCE`（单例）。

- [ ] **Step 1: 写失败测试 `StreamElementTest`**

```java
package org.miniflink.runtime;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class StreamElementTest {

    @Test
    void record携带value且实现StreamElement() {
        Record<String> r = new Record<>("hello");
        assertInstanceOf(StreamElement.class, r);
        assertEquals("hello", r.value());
    }

    @Test
    void EndOfBroadcast是单例且实现StreamElement() {
        EndOfBroadcast a = EndOfBroadcast.INSTANCE;
        EndOfBroadcast b = EndOfBroadcast.INSTANCE;
        assertSame(a, b);
        assertInstanceOf(StreamElement.class, a);
    }
}
```

- [ ] **Step 2: 运行测试验证失败**

Run: `mvn -q -Dtest=StreamElementTest test`
Expected: 编译失败 —— `StreamElement`/`Record`/`EndOfBroadcast` 不存在。

- [ ] **Step 3: 创建 `StreamElement`**

```java
package org.miniflink.runtime;

/** 通道里流动的统一元素。阶段②只有 Record 与 EndOfBroadcast；阶段④⑤可加 Watermark/Barrier 实现。 */
public interface StreamElement {
}
```

- [ ] **Step 4: 创建 `Record`**

```java
package org.miniflink.runtime;

/** 携带一条用户数据的通道元素。 */
public record Record<T>(T value) implements StreamElement {
}
```

- [ ] **Step 5: 创建 `EndOfBroadcast`（单例哨兵）**

```java
package org.miniflink.runtime;

/** EOB 哨兵：表示发送方不再发数据，驱动多线程管道级联关闭。单例。 */
public final class EndOfBroadcast implements StreamElement {
    public static final EndOfBroadcast INSTANCE = new EndOfBroadcast();

    private EndOfBroadcast() {
    }
}
```

- [ ] **Step 6: 运行测试验证通过**

Run: `mvn -q -Dtest=StreamElementTest test`
Expected: `BUILD SUCCESS`，`Tests run: 2, Failures: 0`

- [ ] **Step 7: 提交**

```bash
git add src/main/java/org/miniflink/runtime/StreamElement.java \
        src/main/java/org/miniflink/runtime/Record.java \
        src/main/java/org/miniflink/runtime/EndOfBroadcast.java \
        src/test/java/org/miniflink/runtime/StreamElementTest.java
git commit -m "feat(runtime): 添加 StreamElement/Record/EndOfBroadcast 通道元素

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

## Task 2: Channel（有界通道 + 反压）

**Files:**
- Create: `src/main/java/org/miniflink/runtime/Channel.java`
- Test: `src/test/java/org/miniflink/runtime/ChannelTest.java`

**Interfaces:**
- Consumes: `StreamElement`/`Record`/`EndOfBroadcast`（Task 1）。
- Produces: `Channel(int capacity)`、`send(StreamElement)`、`receive()`、`DEFAULT_CAPACITY`。

- [ ] **Step 1: 写失败测试 `ChannelTest`**

```java
package org.miniflink.runtime;

import org.junit.jupiter.api.Test;
import java.util.concurrent.TimeUnit;
import static org.junit.jupiter.api.Assertions.*;

class ChannelTest {

    @Test
    void send与receive应FIFO传递元素() throws Exception {
        Channel ch = new Channel(4);
        ch.send(new Record<>("a"));
        ch.send(new Record<>("b"));
        ch.send(EndOfBroadcast.INSTANCE);

        assertInstanceOf(Record.class, ch.receive());
        assertInstanceOf(EndOfBroadcast.class, ch.receive()); // 第三个，FIFO
        // 取第二个 Record 校验内容
        // （上面顺序已校验，这里补一个 value 断言）
    }

    @Test
    void 容量满时send应阻塞() throws Exception {
        Channel ch = new Channel(1); // 容量 1
        ch.send(new Record<>("x")); // 占满
        // 再 send 应阻塞；用另一个线程验证
        Thread blocker = new Thread(() -> {
            try { ch.send(new Record<>("y")); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        });
        blocker.start();
        Thread.sleep(100);
        assertEquals(Thread.State.WAITING, blocker.getState()); // 阻塞中（=反压）
        blocker.interrupt();
    }

    @Test
    void 空通道receive应阻塞() throws Exception {
        Channel ch = new Channel(2);
        Thread[] holder = new Thread[1];
        holder[0] = new Thread(() -> {
            try { ch.receive(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        });
        holder[0].start();
        Thread.sleep(100);
        assertEquals(Thread.State.WAITING, holder[0].getState());
        holder[0].interrupt();
    }
}
```

- [ ] **Step 2: 运行测试验证失败**

Run: `mvn -q -Dtest=ChannelTest test`
Expected: 编译失败 —— `Channel` 不存在。

- [ ] **Step 3: 创建 `Channel`**

```java
package org.miniflink.runtime;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * 有界通道：包装有界 BlockingQueue。
 * send = put（满则阻塞生产者 = 天然反压）；receive = take（空则阻塞消费者）。
 * 不提供 close：关闭完全由 EOB 哨兵驱动（见 Task 6）。
 */
public class Channel {
    public static final int DEFAULT_CAPACITY = 64;

    private final BlockingQueue<StreamElement> queue;

    public Channel(int capacity) {
        this.queue = new LinkedBlockingQueue<>(capacity);
    }

    public Channel() {
        this(DEFAULT_CAPACITY);
    }

    /** 发送元素；队列满则阻塞当前线程，直到有空间（反压）。 */
    public void send(StreamElement e) throws InterruptedException {
        queue.put(e);
    }

    /** 接收元素；队列空则阻塞，直到有元素可用。 */
    public StreamElement receive() throws InterruptedException {
        return queue.take();
    }
}
```

- [ ] **Step 4: 运行测试验证通过**

Run: `mvn -q -Dtest=ChannelTest test`
Expected: `BUILD SUCCESS`，`Tests run: 3, Failures: 0`

- [ ] **Step 5: 提交**

```bash
git add src/main/java/org/miniflink/runtime/Channel.java \
        src/test/java/org/miniflink/runtime/ChannelTest.java
git commit -m "feat(runtime): 添加有界 Channel（BlockingQueue + 反压）

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

## Task 3: ChannelWriter（Collector 适配通道）

**Files:**
- Create: `src/main/java/org/miniflink/runtime/ChannelWriter.java`
- Test: `src/test/java/org/miniflink/runtime/ChannelWriterTest.java`

**Interfaces:**
- Consumes: `Collector<T>`（阶段①，`collect`/`close`）、`Channel`、`Record`（Task 1-2）。
- Produces: `ChannelWriter<T>(Channel)`，`collect(T)` 把值包装成 `Record` 写入 Channel。

- [ ] **Step 1: 写失败测试 `ChannelWriterTest`**

```java
package org.miniflink.runtime;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class ChannelWriterTest {

    @Test
    void collect应把值包装成Record写入Channel() throws Exception {
        Channel ch = new Channel(4);
        ChannelWriter<String> writer = new ChannelWriter<>(ch);

        writer.collect("hello");

        StreamElement e = ch.receive();
        assertInstanceOf(Record.class, e);
        assertEquals("hello", ((Record<String>) e).value());
    }

    @Test
    void 多次collect应按序写入() throws Exception {
        Channel ch = new Channel(8);
        ChannelWriter<Integer> writer = new ChannelWriter<>(ch);

        writer.collect(1);
        writer.collect(2);
        writer.collect(3);

        assertEquals(1, ((Record<Integer>) ch.receive()).value());
        assertEquals(2, ((Record<Integer>) ch.receive()).value());
        assertEquals(3, ((Record<Integer>) ch.receive()).value());
    }
}
```

- [ ] **Step 2: 运行测试验证失败**

Run: `mvn -q -Dtest=ChannelWriterTest test`
Expected: 编译失败 —— `ChannelWriter` 不存在。

- [ ] **Step 3: 创建 `ChannelWriter`**

```java
package org.miniflink.runtime;

/**
 * Collector 实现：把 collect 的值包装成 Record 写入下游 Channel。
 * 算子通过 Collector 接口输出，对通道无感知（稳定边界）。
 */
public class ChannelWriter<T> implements Collector<T> {
    private final Channel channel;

    public ChannelWriter(Channel channel) {
        this.channel = channel;
    }

    @Override
    public void collect(T record) {
        try {
            channel.send(new Record<>(record));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("通道发送被中断", e);
        }
    }

    @Override
    public void close() {
        // EOB 由 Task 统一发送，ChannelWriter 无需操作
    }
}
```

- [ ] **Step 4: 运行测试验证通过**

Run: `mvn -q -Dtest=ChannelWriterTest test`
Expected: `BUILD SUCCESS`，`Tests run: 2, Failures: 0`

- [ ] **Step 5: 提交**

```bash
git add src/main/java/org/miniflink/runtime/ChannelWriter.java \
        src/test/java/org/miniflink/runtime/ChannelWriterTest.java
git commit -m "feat(runtime): 添加 ChannelWriter（Collector 适配通道）

Co-Authored-By: Claude <noreply@anthropic.com>"
```

## Task 4: ChainCollector + OperatorChain（链内 forward）

**Files:**
- Create: `src/main/java/org/miniflink/runtime/ChainCollector.java`
- Create: `src/main/java/org/miniflink/runtime/OperatorChain.java`
- Test: `src/test/java/org/miniflink/runtime/OperatorChainTest.java`

**Interfaces:**
- Consumes: `Collector<T>`、`Operator<IN,OUT>`（阶段①）、`ListCollector`（阶段① test）、`MapOperator`/`FilterOperator`（阶段①）。
- Produces: `ChainCollector<T>(Operator<T,?>)`；`OperatorChain<IN,OUT>(List<Operator<?,?>>)`，`open(Collector<OUT>)`/`processElement(IN)`/`close()`。

- [ ] **Step 1: 写失败测试 `OperatorChainTest`**

```java
package org.miniflink.runtime;

import org.junit.jupiter.api.Test;
import org.miniflink.api.function.FilterFunction;
import org.miniflink.api.function.MapFunction;
import org.miniflink.runtime.operator.FilterOperator;
import org.miniflink.runtime.operator.MapOperator;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class OperatorChainTest {

    @Test
    void 链化算子应链内forward传递并输出到链尾() throws Exception {
        // map(x -> x+1) -> filter(x > 2)，链尾输出到 ListCollector
        OperatorChain<Integer, Integer> chain = new OperatorChain<>(List.of(
                new MapOperator<>((MapFunction<Integer, Integer>) x -> x + 1),
                new FilterOperator<>((FilterFunction<Integer>) x -> x > 2)
        ));
        ListCollector<Integer> out = new ListCollector<>();
        chain.open(out);

        chain.processElement(1); // 1->2，2>2 false，丢弃
        chain.processElement(2); // 2->3，3>2 true，输出 3
        chain.processElement(3); // 3->4，4>2 true，输出 4

        assertEquals(List.of(3, 4), out.getResult());
    }

    @Test
    void 空算子列表应拒绝构造() {
        assertThrows(IllegalArgumentException.class, () -> new OperatorChain<Object, Object>(List.of()));
    }
}
```

- [ ] **Step 2: 运行测试验证失败**

Run: `mvn -q -Dtest=OperatorChainTest test`
Expected: 编译失败 —— `OperatorChain`/`ChainCollector` 不存在。

- [ ] **Step 3: 创建 `ChainCollector`**

```java
package org.miniflink.runtime;

/**
 * Collector 实现：链内 forward——collect 时直接同步调用下游算子的 processElement（不经 Channel）。
 * 用于 OperatorChain 内相邻算子的连接。
 */
public class ChainCollector<T> implements Collector<T> {
    private final Operator<T, ?> downstream;

    public ChainCollector(Operator<T, ?> downstream) {
        this.downstream = downstream;
    }

    @Override
    public void collect(T record) {
        try {
            downstream.processElement(record);
        } catch (Exception e) {
            throw new RuntimeException("链内算子执行异常", e);
        }
    }

    @Override
    public void close() {
        // 链内无需操作
    }
}
```

- [ ] **Step 4: 创建 `OperatorChain`**

```java
package org.miniflink.runtime;

import java.util.ArrayList;
import java.util.List;

/**
 * 一个 Task 内链化的算子序列 [op1(IN→?), op2, ..., opN(?→OUT)]。
 * 链内算子经 ChainCollector 直接函数调用（不经 Channel）；链尾算子输出到传入的 Collector（通常是 ChannelWriter）。
 * open 从尾到头接线：opN.open(output)；opN-1.open(ChainCollector(opN))；...。
 */
public class OperatorChain<IN, OUT> {
    private final List<Operator<?, ?>> operators;

    public OperatorChain(List<Operator<?, ?>> operators) {
        if (operators == null || operators.isEmpty()) {
            throw new IllegalArgumentException("OperatorChain 至少含一个算子");
        }
        this.operators = new ArrayList<>(operators);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public void open(Collector<OUT> output) {
        Collector current = output;
        // 从尾到头：每个算子 open(它的输出)，然后它的输出 = ChainCollector(它)，供上游 open
        for (int i = operators.size() - 1; i >= 0; i--) {
            Operator op = operators.get(i);
            op.open(current);
            current = new ChainCollector(op);
        }
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public void processElement(IN record) throws Exception {
        // 链头算子处理，经 ChainCollector 级联到链尾 → output
        ((Operator) operators.get(0)).processElement(record);
    }

    public void close() {
        for (Operator<?, ?> op : operators) {
            op.close();
        }
    }
}
```

- [ ] **Step 5: 运行测试验证通过**

Run: `mvn -q -Dtest=OperatorChainTest test`
Expected: `BUILD SUCCESS`，`Tests run: 2, Failures: 0`

- [ ] **Step 6: 提交**

```bash
git add src/main/java/org/miniflink/runtime/ChainCollector.java \
        src/main/java/org/miniflink/runtime/OperatorChain.java \
        src/test/java/org/miniflink/runtime/OperatorChainTest.java
git commit -m "feat(runtime): 添加 OperatorChain 与 ChainCollector（算子链内 forward）

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

## Task 5: Partitioner + KeySelector + OneInputTransformation 分区字段

> 注：Partitioner 放 `execution` 包（执行计划边的属性）。`OneInputTransformation`（graph）引用 `execution.Partitioner`——Java 包间引用合法。KeySelector 在本任务一并创建（后续 keyBy/Tsk 用）。

**Files:**
- Create: `src/main/java/org/miniflink/execution/Partitioner.java`
- Create: `src/main/java/org/miniflink/execution/ForwardPartitioner.java`
- Create: `src/main/java/org/miniflink/execution/HashPartitioner.java`
- Create: `src/main/java/org/miniflink/execution/RebalancePartitioner.java`
- Create: `src/main/java/org/miniflink/api/function/KeySelector.java`
- Modify: `src/main/java/org/miniflink/graph/OneInputTransformation.java`（加 partitioner + keySelector 字段）
- Test: `src/test/java/org/miniflink/execution/PartitionerTest.java`

**Interfaces:**
- Consumes: `OneInputTransformation`（阶段①）。
- Produces: `Partitioner.selectChannel(int numDownstream, Object key, int upstreamIndex)`；`ForwardPartitioner`/`HashPartitioner`/`RebalancePartitioner`；`KeySelector<T,K>.getKey(T)`；`OneInputTransformation.getPartitioner()`/`getKeySelector()`。

- [ ] **Step 1: 写失败测试 `PartitionerTest`**

```java
package org.miniflink.execution;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class PartitionerTest {

    @Test
    void forward返回上游索引() {
        ForwardPartitioner p = new ForwardPartitioner();
        assertEquals(0, p.selectChannel(2, null, 0));
        assertEquals(1, p.selectChannel(2, null, 1));
    }

    @Test
    void forward要求上下游并行度相同() {
        ForwardPartitioner p = new ForwardPartitioner();
        assertThrows(IllegalStateException.class,
                () -> p.selectChannel(2, null, 5)); // upstreamIndex >= numDownstream
    }

    @Test
    void hash同key落同一通道() {
        HashPartitioner p = new HashPartitioner();
        int c1 = p.selectChannel(4, "key-a", 0);
        int c2 = p.selectChannel(4, "key-a", 1); // 不同上游，同 key
        assertEquals(c1, c2);
        assertTrue(c1 >= 0 && c1 < 4);
    }

    @Test
    void rebalance轮询分发() {
        RebalancePartitioner p = new RebalancePartitioner();
        assertEquals(0, p.selectChannel(2, null, 0));
        assertEquals(1, p.selectChannel(2, null, 0));
        assertEquals(0, p.selectChannel(2, null, 0));
    }
}
```

- [ ] **Step 2: 运行测试验证失败**

Run: `mvn -q -Dtest=PartitionerTest test`
Expected: 编译失败 —— `Partitioner` 等不存在。

- [ ] **Step 3: 创建 `Partitioner` 接口**

```java
package org.miniflink.execution;

/** 分区器：决定一条数据发往哪个下游 subtask（0..numDownstream-1）。 */
public interface Partitioner {
    int selectChannel(int numDownstream, Object key, int upstreamIndex);
}
```

- [ ] **Step 4: 创建 `ForwardPartitioner`**

```java
package org.miniflink.execution;

/** 一对一：上游 i → 下游 i（要求上下游并行度相同）。 */
public class ForwardPartitioner implements Partitioner {
    @Override
    public int selectChannel(int numDownstream, Object key, int upstreamIndex) {
        if (upstreamIndex >= numDownstream || upstreamIndex < 0) {
            throw new IllegalStateException(
                    "forward 要求上下游并行度相同：upstreamIndex=" + upstreamIndex
                            + ", numDownstream=" + numDownstream);
        }
        return upstreamIndex;
    }
}
```

- [ ] **Step 5: 创建 `HashPartitioner`**

```java
package org.miniflink.execution;

/** 按 key 哈希取模：同 key 恒落同一下游 subtask（keyBy 用）。 */
public class HashPartitioner implements Partitioner {
    @Override
    public int selectChannel(int numDownstream, Object key, int upstreamIndex) {
        int h = (key == null) ? 0 : key.hashCode();
        return Math.floorMod(h, numDownstream);
    }
}
```

- [ ] **Step 6: 创建 `RebalancePartitioner`**

```java
package org.miniflink.execution;

import java.util.concurrent.atomic.AtomicInteger;

/** 轮询：多下游间循环分发（source→多下游并行用）。线程安全计数器。 */
public class RebalancePartitioner implements Partitioner {
    private final AtomicInteger counter = new AtomicInteger(0);

    @Override
    public int selectChannel(int numDownstream, Object key, int upstreamIndex) {
        return Math.floorMod(counter.getAndIncrement(), numDownstream);
    }
}
```

- [ ] **Step 7: 创建 `KeySelector`**

```java
package org.miniflink.api.function;

/** 从一条记录中提取 key（keyBy 分区用）。 */
@FunctionalInterface
public interface KeySelector<T, K> {
    K getKey(T value) throws Exception;
}
```

- [ ] **Step 8: 修改 `OneInputTransformation` 加分区字段**

把 `src/main/java/org/miniflink/graph/OneInputTransformation.java` 整体替换为：

```java
package org.miniflink.graph;

import org.miniflink.api.function.KeySelector;
import org.miniflink.execution.ForwardPartitioner;
import org.miniflink.execution.Partitioner;
import org.miniflink.runtime.Operator;

/** 单输入节点：持有一个处理算子及其上游 input，并记录其入边的分区策略。 */
public class OneInputTransformation<IN, OUT> extends Transformation<OUT> {
    private final Transformation<IN> input;
    private final Operator<IN, OUT> operator;
    private final Partitioner partitioner;
    private final KeySelector<?, ?> keySelector;

    /** 默认 forward 分区（向后兼容阶段①调用）。 */
    public OneInputTransformation(int id, String name, Transformation<IN> input, Operator<IN, OUT> operator) {
        this(id, name, input, operator, new ForwardPartitioner(), null);
    }

    public OneInputTransformation(int id, String name, Transformation<IN> input, Operator<IN, OUT> operator,
                                  Partitioner partitioner, KeySelector<?, ?> keySelector) {
        super(id, name);
        this.input = input;
        this.operator = operator;
        this.partitioner = partitioner;
        this.keySelector = keySelector;
    }

    public Transformation<IN> getInput() {
        return input;
    }

    public Operator<IN, OUT> getOperator() {
        return operator;
    }

    public Partitioner getPartitioner() {
        return partitioner;
    }

    public KeySelector<?, ?> getKeySelector() {
        return keySelector;
    }
}
```

- [ ] **Step 9: 运行测试验证通过 + 全量回归**

Run: `mvn -q -Dtest=PartitionerTest test`
Expected: `BUILD SUCCESS`，`Tests run: 4, Failures: 0`

Run: `mvn -q test`
Expected: 全量通过（阶段① 12 + 阶段②已加测试，无回归）。

- [ ] **Step 10: 提交**

```bash
git add src/main/java/org/miniflink/execution/Partitioner.java \
        src/main/java/org/miniflink/execution/ForwardPartitioner.java \
        src/main/java/org/miniflink/execution/HashPartitioner.java \
        src/main/java/org/miniflink/execution/RebalancePartitioner.java \
        src/main/java/org/miniflink/api/function/KeySelector.java \
        src/main/java/org/miniflink/graph/OneInputTransformation.java \
        src/test/java/org/miniflink/execution/PartitionerTest.java
git commit -m "feat(execution): 添加 Partitioner/KeySelector 与 OneInputTransformation 分区字段

Co-Authored-By: Claude <noreply@anthropic.com>"
```

## Task 6: Source 并行信息改造（subtaskIndex 分片）

让 source 知道自己在并行度中的位置，以支持 `parallelism>1` 时按 subtask 分片数据。

**Files:**
- Modify: `src/main/java/org/miniflink/runtime/SourceContext.java`（加 `getSubtaskIndex`/`getParallelism`）
- Modify: `src/main/java/org/miniflink/runtime/SourceContextImpl.java`（构造加 index/parallelism）
- Modify: `src/main/java/org/miniflink/runtime/SourceOperator.java`（`open` 加 index/parallelism 参数）
- Modify: `src/main/java/org/miniflink/runtime/operator/SourceOperatorImpl.java`（`open` 透传）
- Modify: `src/main/java/org/miniflink/connector/CollectionSource.java`（按 subtaskIndex 取模分片）
- Modify: `src/main/java/org/miniflink/runtime/StreamExecutor.java`（`source.open` 临时传 `0,1`，Task 11 再重构）
- Modify: `src/test/java/org/miniflink/runtime/operator/SourceOperatorImplTest.java`（`open` 加 `0,1`）
- Test: `src/test/java/org/miniflink/runtime/SourceParallelismTest.java`

**Interfaces:**
- Produces: `SourceContext.getSubtaskIndex()`/`getParallelism()`；`SourceContextImpl(Collector, int subtaskIndex, int parallelism)`；`SourceOperator.open(Collector, int, int)`。

- [ ] **Step 1: 写失败测试 `SourceParallelismTest`**

```java
package org.miniflink.runtime;

import org.junit.jupiter.api.Test;
import org.miniflink.connector.CollectionSource;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class SourceParallelismTest {

    @Test
    void CollectionSource应按subtaskIndex分片() throws Exception {
        CollectionSource<Integer> src = new CollectionSource<>(List.of(10, 11, 12, 13, 14));

        ListCollector<Integer> out0 = new ListCollector<>();
        src.run(new SourceContextImpl<>(out0, 0, 2)); // subtask 0 取索引 0,2,4
        assertEquals(List.of(10, 12, 14), out0.getResult());

        ListCollector<Integer> out1 = new ListCollector<>();
        src.run(new SourceContextImpl<>(out1, 1, 2)); // subtask 1 取索引 1,3
        assertEquals(List.of(11, 13), out1.getResult());
    }

    @Test
    void parallelism为1时取全部() throws Exception {
        CollectionSource<String> src = new CollectionSource<>(List.of("a", "b", "c"));
        ListCollector<String> out = new ListCollector<>();
        src.run(new SourceContextImpl<>(out, 0, 1));
        assertEquals(List.of("a", "b", "c"), out.getResult());
    }
}
```

- [ ] **Step 2: 运行测试验证失败**

Run: `mvn -q -Dtest=SourceParallelismTest test`
Expected: 编译失败 —— `SourceContextImpl` 构造不匹配 / `SourceContext` 无 getSubtaskIndex。

- [ ] **Step 3: 修改 `SourceContext` 接口**

整体替换 `src/main/java/org/miniflink/runtime/SourceContext.java`：

```java
package org.miniflink.runtime;

/** source 发数据用的上下文，并暴露该 subtask 的并行位置（分片用）。 */
public interface SourceContext<T> {
    void collect(T record);
    int getSubtaskIndex();
    int getParallelism();
}
```

- [ ] **Step 4: 修改 `SourceContextImpl`**

整体替换 `src/main/java/org/miniflink/runtime/SourceContextImpl.java`：

```java
package org.miniflink.runtime;

/** 把 SourceContext.collect 转发到下游 Collector；持有 subtask 位置供分片。 */
public class SourceContextImpl<T> implements SourceContext<T> {
    private final Collector<T> out;
    private final int subtaskIndex;
    private final int parallelism;

    public SourceContextImpl(Collector<T> out, int subtaskIndex, int parallelism) {
        this.out = out;
        this.subtaskIndex = subtaskIndex;
        this.parallelism = parallelism;
    }

    @Override
    public void collect(T record) {
        out.collect(record);
    }

    @Override
    public int getSubtaskIndex() {
        return subtaskIndex;
    }

    @Override
    public int getParallelism() {
        return parallelism;
    }
}
```

- [ ] **Step 5: 修改 `SourceOperator` 接口**（`open` 加参数）

整体替换 `src/main/java/org/miniflink/runtime/SourceOperator.java`：

```java
package org.miniflink.runtime;

/** source 算子接口：open 注入输出与并行位置，run 产生数据，close 释放资源。 */
public interface SourceOperator<OUT> {
    void open(Collector<OUT> out, int subtaskIndex, int parallelism);
    void run() throws Exception;
    void close();
}
```

- [ ] **Step 6: 修改 `SourceOperatorImpl`**（`open` 透传 index/parallelism）

整体替换 `src/main/java/org/miniflink/runtime/operator/SourceOperatorImpl.java`：

```java
package org.miniflink.runtime.operator;

import org.miniflink.api.function.SourceFunction;
import org.miniflink.runtime.Collector;
import org.miniflink.runtime.SourceContext;
import org.miniflink.runtime.SourceContextImpl;
import org.miniflink.runtime.SourceOperator;

/** 包装 SourceFunction 的 source 算子：open 建 SourceContextImpl（带并行位置），run 调用用户函数。 */
public class SourceOperatorImpl<OUT> implements SourceOperator<OUT> {
    private final SourceFunction<OUT> sourceFunction;
    private SourceContext<OUT> ctx;

    public SourceOperatorImpl(SourceFunction<OUT> sourceFunction) {
        this.sourceFunction = sourceFunction;
    }

    @Override
    public void open(Collector<OUT> out, int subtaskIndex, int parallelism) {
        this.ctx = new SourceContextImpl<>(out, subtaskIndex, parallelism);
    }

    @Override
    public void run() throws Exception {
        sourceFunction.run(ctx);
    }

    @Override
    public void close() {
        // 阶段②无需操作
    }
}
```

- [ ] **Step 7: 修改 `CollectionSource`**（按 subtaskIndex 取模分片）

整体替换 `src/main/java/org/miniflink/connector/CollectionSource.java`：

```java
package org.miniflink.connector;

import org.miniflink.api.function.SourceFunction;
import org.miniflink.runtime.SourceContext;

/** 内置 source：从 Iterable 读取，按 subtaskIndex 取模分片（元素 i → i % parallelism == subtaskIndex 的 subtask）。 */
public class CollectionSource<T> implements SourceFunction<T> {
    private final Iterable<T> data;

    public CollectionSource(Iterable<T> data) {
        this.data = data;
    }

    @Override
    public void run(SourceContext<T> ctx) {
        int parallelism = ctx.getParallelism();
        int subtask = ctx.getSubtaskIndex();
        int idx = 0;
        for (T item : data) {
            if (idx % parallelism == subtask) {
                ctx.collect(item);
            }
            idx++;
        }
    }
}
```

- [ ] **Step 8: 修改 `StreamExecutor`**（临时传 `0,1`，保持阶段①单并行度可跑）

在 `src/main/java/org/miniflink/runtime/StreamExecutor.java` 中，把 `source.open(typedSourceOut);` 这一行改为：

```java
        source.open(typedSourceOut, 0, 1);
```

> 这是临时改动：阶段①的 StreamExecutor 仍单并行度运行（index=0, parallelism=1），保证阶段②中途全量测试不回归。Task 11 会整体重构 StreamExecutor。

- [ ] **Step 9: 修改 `SourceOperatorImplTest`**（`open` 加 `0,1`）

在 `src/test/java/org/miniflink/runtime/operator/SourceOperatorImplTest.java` 中，把 `op.open(downstream);` 改为 `op.open(downstream, 0, 1);`（parallelism=1 取全部，断言不变）。

- [ ] **Step 10: 运行测试验证通过 + 全量回归**

Run: `mvn -q -Dtest=SourceParallelismTest test`
Expected: `BUILD SUCCESS`，`Tests run: 2, Failures: 0`

Run: `mvn -q test`
Expected: 全量通过（SourceOperatorImplTest 已适配新 open 签名，阶段①其他测试无回归）。

- [ ] **Step 11: 提交**

```bash
git add src/main/java/org/miniflink/runtime/SourceContext.java \
        src/main/java/org/miniflink/runtime/SourceContextImpl.java \
        src/main/java/org/miniflink/runtime/SourceOperator.java \
        src/main/java/org/miniflink/runtime/operator/SourceOperatorImpl.java \
        src/main/java/org/miniflink/connector/CollectionSource.java \
        src/main/java/org/miniflink/runtime/StreamExecutor.java \
        src/test/java/org/miniflink/runtime/operator/SourceOperatorImplTest.java \
        src/test/java/org/miniflink/runtime/SourceParallelismTest.java
git commit -m "feat(runtime): Source 并行信息改造（subtaskIndex 分片）

Co-Authored-By: Claude <noreply@anthropic.com>"
```

## Task 7: Output + OutputCollector（fan-out 路由）

**Files:**
- Create: `src/main/java/org/miniflink/runtime/Output.java`
- Create: `src/main/java/org/miniflink/runtime/OutputCollector.java`
- Test: `src/test/java/org/miniflink/runtime/OutputCollectorTest.java`

**Interfaces:**
- Consumes: `Channel`、`Record`、`EndOfBroadcast`、`Collector`、`Partitioner`（Task 1-5）、`KeySelector`（Task 5）。
- Produces: `Output(List<Channel>, Partitioner, KeySelector)`，`route(Object, int upstreamIndex)`，`sendEob()`，`getDownstreamChannels()`；`OutputCollector<T>(List<Output>, int upstreamIndex)`。

- [ ] **Step 1: 写失败测试 `OutputCollectorTest`**

```java
package org.miniflink.runtime;

import org.junit.jupiter.api.Test;
import org.miniflink.execution.ForwardPartitioner;
import org.miniflink.execution.HashPartitioner;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class OutputCollectorTest {

    @Test
    void forward应路由到对应索引的通道() {
        Channel c0 = new Channel(2);
        Channel c1 = new Channel(2);
        Output out = new Output(List.of(c0, c1), new ForwardPartitioner(), null);
        OutputCollector<String> col = new OutputCollector<>(List.of(out), 0); // upstreamIndex=0 → c0

        col.collect("x");

        assertInstanceOf(Record.class, c0.receive());
    }

    @Test
    void keyBy应按key哈希路由且同key同通道() {
        Channel c0 = new Channel(4);
        Channel c1 = new Channel(4);
        Output out = new Output(List.of(c0, c1), new HashPartitioner(),
                (org.miniflink.api.function.KeySelector<String, String>) s -> s);
        OutputCollector<String> col = new OutputCollector<>(List.of(out), 0);

        col.collect("a");
        col.collect("a");

        // 同 key 两次应落同一通道（但具体哪个由哈希决定）；取空一个验证 FIFO
        assertInstanceOf(Record.class, c0.receive());
    }

    @Test
    void sendEob应向所有下游通道发EOB() {
        Channel c0 = new Channel(2);
        Channel c1 = new Channel(2);
        Output out = new Output(List.of(c0, c1), new ForwardPartitioner(), null);

        out.sendEob();

        assertInstanceOf(EndOfBroadcast.class, c0.receive());
        assertInstanceOf(EndOfBroadcast.class, c1.receive());
    }
}
```

- [ ] **Step 2: 运行测试验证失败**

Run: `mvn -q -Dtest=OutputCollectorTest test`
Expected: 编译失败 —— `Output`/`OutputCollector` 不存在。

- [ ] **Step 3: 创建 `Output`**

```java
package org.miniflink.runtime;

import org.miniflink.api.function.KeySelector;
import org.miniflink.execution.Partitioner;

import java.util.List;

/**
 * 一个 fan-out 目标：下游 subtask 的输入 Channel 列表 + 分区器 + keySelector（hash 用）。
 * route 按分区器选一个 Channel 发送 Record；sendEob 向所有下游 Channel 广播 EOB。
 */
public class Output {
    private final List<Channel> downstreamChannels;
    private final Partitioner partitioner;
    private final KeySelector<?, ?> keySelector;

    public Output(List<Channel> downstreamChannels, Partitioner partitioner, KeySelector<?, ?> keySelector) {
        this.downstreamChannels = downstreamChannels;
        this.partitioner = partitioner;
        this.keySelector = keySelector;
    }

    public List<Channel> getDownstreamChannels() {
        return downstreamChannels;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public void route(Object value, int upstreamIndex) throws Exception {
        Object key = (keySelector != null) ? ((KeySelector) keySelector).getKey(value) : null;
        int idx = partitioner.selectChannel(downstreamChannels.size(), key, upstreamIndex);
        downstreamChannels.get(idx).send(new Record<>(value));
    }

    /** 向所有下游 Channel 广播 EOB（关闭语义用）。 */
    public void sendEob() {
        for (Channel c : downstreamChannels) {
            try {
                c.send(EndOfBroadcast.INSTANCE);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("发送 EOB 被中断", e);
            }
        }
    }
}
```

- [ ] **Step 4: 创建 `OutputCollector`**

```java
package org.miniflink.runtime;

import java.util.List;

/**
 * Collector 实现：collect 时把记录按分区器路由到下游 Channel（fan-out）。
 * 一个算子可能有多条出边（阶段②线性链通常 1 条），遍历每个 Output 路由。
 */
public class OutputCollector<T> implements Collector<T> {
    private final List<Output> outputs;
    private final int upstreamIndex;

    public OutputCollector(List<Output> outputs, int upstreamIndex) {
        this.outputs = outputs;
        this.upstreamIndex = upstreamIndex;
    }

    @Override
    public void collect(T record) {
        for (Output o : outputs) {
            try {
                o.route(record, upstreamIndex);
            } catch (Exception e) {
                throw new RuntimeException("输出路由异常", e);
            }
        }
    }

    @Override
    public void close() {
        // EOB 由 Task 统一发送
    }
}
```

- [ ] **Step 5: 运行测试验证通过**

Run: `mvn -q -Dtest=OutputCollectorTest test`
Expected: `BUILD SUCCESS`，`Tests run: 3, Failures: 0`

- [ ] **Step 6: 提交**

```bash
git add src/main/java/org/miniflink/runtime/Output.java \
        src/main/java/org/miniflink/runtime/OutputCollector.java \
        src/test/java/org/miniflink/runtime/OutputCollectorTest.java
git commit -m "feat(runtime): 添加 Output/OutputCollector（fan-out 分区路由）

Co-Authored-By: Claude <noreply@anthropic.com>"
```

## Task 8: Task + SourceTask + OperatorTask（多线程执行单元）

**Files:**
- Create: `src/main/java/org/miniflink/runtime/Task.java`
- Create: `src/main/java/org/miniflink/runtime/SourceTask.java`
- Create: `src/main/java/org/miniflink/runtime/OperatorTask.java`
- Test: `src/test/java/org/miniflink/runtime/SourceTaskTest.java`
- Test: `src/test/java/org/miniflink/runtime/OperatorTaskTest.java`

**Interfaces:**
- Consumes: `SourceOperator`（Task 6 新签名）、`OperatorChain`（Task 4）、`Output`/`OutputCollector`（Task 7）、`Channel`/`Record`/`EndOfBroadcast`（Task 1-2）、`NoopCollector`（阶段①）、`MapOperator`/`CollectionSource`/`SourceOperatorImpl`（阶段①）。
- Produces: `Task extends Runnable`（含 `default broadcastEob(List<Output>)`）；`SourceTask(SourceOperator, List<Output>, int subtaskIndex, int parallelism)`；`OperatorTask(OperatorChain, Channel input, int pendingUpstreams, List<Output>, int subtaskIndex)`。

- [ ] **Step 1: 写失败测试 `SourceTaskTest`**

```java
package org.miniflink.runtime;

import org.junit.jupiter.api.Test;
import org.miniflink.connector.CollectionSource;
import org.miniflink.execution.ForwardPartitioner;
import org.miniflink.runtime.operator.SourceOperatorImpl;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class SourceTaskTest {

    @Test
    void run后下游Channel应收到数据与EOB() throws Exception {
        Channel ch = new Channel(8);
        Output out = new Output(List.of(ch), new ForwardPartitioner(), null);
        SourceTask task = new SourceTask(
                new SourceOperatorImpl<>(new CollectionSource<>(List.of("a", "b"))),
                List.of(out), 0, 1);

        task.run(); // 单线程直接驱动

        assertEquals("a", ((Record<String>) ch.receive()).value());
        assertEquals("b", ((Record<String>) ch.receive()).value());
        assertInstanceOf(EndOfBroadcast.class, ch.receive());
    }
}
```

- [ ] **Step 2: 写失败测试 `OperatorTaskTest`**

```java
package org.miniflink.runtime;

import org.junit.jupiter.api.Test;
import org.miniflink.api.function.MapFunction;
import org.miniflink.execution.ForwardPartitioner;
import org.miniflink.runtime.operator.MapOperator;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class OperatorTaskTest {

    @Test
    void 处理数据并在所有上游EOB后向下游广播() throws Exception {
        Channel input = new Channel(8);
        input.send(new Record<>(1));
        input.send(new Record<>(2));
        input.send(EndOfBroadcast.INSTANCE);

        OperatorChain<Integer, Integer> chain = new OperatorChain<>(List.of(
                new MapOperator<>((MapFunction<Integer, Integer>) x -> x * 10)));
        Channel outCh = new Channel(8);
        Output out = new Output(List.of(outCh), new ForwardPartitioner(), null);

        new OperatorTask(chain, input, 1, List.of(out), 0).run();

        assertEquals(10, ((Record<Integer>) outCh.receive()).value());
        assertEquals(20, ((Record<Integer>) outCh.receive()).value());
        assertInstanceOf(EndOfBroadcast.class, outCh.receive());
    }

    @Test
    void 多上游时等所有EOB才退出不丢数据() throws Exception {
        Channel input = new Channel(8);
        input.send(new Record<>(1));
        input.send(EndOfBroadcast.INSTANCE);   // 上游 1 的 EOB
        input.send(new Record<>(2));
        input.send(EndOfBroadcast.INSTANCE);   // 上游 2 的 EOB

        OperatorChain<Integer, Integer> chain = new OperatorChain<>(List.of(
                new MapOperator<>((MapFunction<Integer, Integer>) x -> x)));
        Channel outCh = new Channel(8);
        Output out = new Output(List.of(outCh), new ForwardPartitioner(), null);

        new OperatorTask(chain, input, 2, List.of(out), 0).run(); // pendingUpstreams=2

        // 收到第 1 个 EOB 不退出，继续处理 2，收到第 2 个 EOB 才广播
        assertEquals(1, ((Record<Integer>) outCh.receive()).value());
        assertEquals(2, ((Record<Integer>) outCh.receive()).value());
        assertInstanceOf(EndOfBroadcast.class, outCh.receive());
    }
}
```

- [ ] **Step 3: 运行测试验证失败**

Run: `mvn -q -Dtest=SourceTaskTest,OperatorTaskTest test`
Expected: 编译失败 —— `Task`/`SourceTask`/`OperatorTask` 不存在。

- [ ] **Step 4: 创建 `Task` 接口**

```java
package org.miniflink.runtime;

import java.util.List;

/** 多线程执行单元。broadcastEob 向所有出边的下游 Channel 广播 EOB（关闭语义）。 */
public interface Task extends Runnable {

    default void broadcastEob(List<Output> outputs) {
        for (Output o : outputs) {
            o.sendEob();
        }
    }
}
```

- [ ] **Step 5: 创建 `SourceTask`**

```java
package org.miniflink.runtime;

import java.util.List;

/**
 * source 的执行单元：open source（注入并行位置）→ source.run() 产生数据 → 正常结束后广播 EOB。
 * open/run 在 try 内；close 在 finally（修复阶段① open 在 try 外的隐患）。
 */
public class SourceTask implements Task {
    private final SourceOperator<?> sourceOperator;
    private final List<Output> outputs;
    private final int subtaskIndex;
    private final int parallelism;

    public SourceTask(SourceOperator<?> sourceOperator, List<Output> outputs, int subtaskIndex, int parallelism) {
        this.sourceOperator = sourceOperator;
        this.outputs = outputs;
        this.subtaskIndex = subtaskIndex;
        this.parallelism = parallelism;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Override
    public void run() {
        OutputCollector out = new OutputCollector(outputs, subtaskIndex);
        try {
            sourceOperator.open((Collector) out, subtaskIndex, parallelism);
            sourceOperator.run();
            broadcastEob(outputs); // 正常结束才广播
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

- [ ] **Step 6: 创建 `OperatorTask`**

```java
package org.miniflink.runtime;

import java.util.List;

/**
 * 处理算子的执行单元：open chain → 循环从输入 Channel 读 → Record 经 chain 处理 → EOB 则计数--。
 * pendingUpstreams 归零（所有上游都 EOB）后向下游广播 EOB 并退出（fan-in 引用计数对齐）。
 */
public class OperatorTask implements Task {
    private final OperatorChain<?, ?> chain;
    private final Channel input;
    private final int pendingUpstreams;
    private final List<Output> outputs;
    private final int subtaskIndex;

    public OperatorTask(OperatorChain<?, ?> chain, Channel input, int pendingUpstreams,
                        List<Output> outputs, int subtaskIndex) {
        this.chain = chain;
        this.input = input;
        this.pendingUpstreams = pendingUpstreams;
        this.outputs = outputs;
        this.subtaskIndex = subtaskIndex;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Override
    public void run() {
        Collector outCollector = outputs.isEmpty() ? new NoopCollector<>() : new OutputCollector(outputs, subtaskIndex);
        try {
            chain.open((Collector) outCollector);
            int remaining = pendingUpstreams;
            while (remaining > 0) {
                StreamElement e = input.receive();
                if (e == EndOfBroadcast.INSTANCE) {
                    remaining--;
                } else if (e instanceof Record<?> r) {
                    chain.processElement(r.value());
                }
            }
            broadcastEob(outputs); // 所有上游结束，向下游广播
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

- [ ] **Step 7: 运行测试验证通过**

Run: `mvn -q -Dtest=SourceTaskTest,OperatorTaskTest test`
Expected: `BUILD SUCCESS`，`Tests run: 3, Failures: 0`

- [ ] **Step 8: 提交**

```bash
git add src/main/java/org/miniflink/runtime/Task.java \
        src/main/java/org/miniflink/runtime/SourceTask.java \
        src/main/java/org/miniflink/runtime/OperatorTask.java \
        src/test/java/org/miniflink/runtime/SourceTaskTest.java \
        src/test/java/org/miniflink/runtime/OperatorTaskTest.java
git commit -m "feat(runtime): 添加 Task/SourceTask/OperatorTask（多线程 + EOB 引用计数对齐）

Co-Authored-By: Claude <noreply@anthropic.com>"
```

## Task 9: ExecutionVertex + ExecutionEdge（物理计划节点）

**Files:**
- Create: `src/main/java/org/miniflink/execution/ExecutionVertex.java`
- Create: `src/main/java/org/miniflink/execution/ExecutionEdge.java`
- Test: `src/test/java/org/miniflink/execution/ExecutionVertexEdgeTest.java`

**Interfaces:**
- Consumes: `Operator`/`SourceOperator`（阶段①）、`Partitioner`（Task 5）、`KeySelector`（Task 5）。
- Produces: `ExecutionVertex(int id, int subtaskIndex, int parallelism, List<Operator>, SourceOperator)`（source 或处理二选一）；`ExecutionEdge(List<ExecutionVertex> sources, List<ExecutionVertex> targets, Partitioner partitioner, KeySelector keySelector)`。

- [ ] **Step 1: 写失败测试 `ExecutionVertexEdgeTest`**

```java
package org.miniflink.execution;

import org.junit.jupiter.api.Test;
import org.miniflink.api.function.KeySelector;
import org.miniflink.runtime.operator.MapOperator;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ExecutionVertexEdgeTest {

    @Test
    void sourceVertex判定为source() {
        ExecutionVertex sv = new ExecutionVertex(1, 0, 1, List.of(),
                new org.miniflink.runtime.operator.SourceOperatorImpl<>(
                        new org.miniflink.connector.CollectionSource<>(List.of("x"))));
        assertTrue(sv.isSource());
    }

    @Test
    void 处理vertex持有算子且非source() {
        ExecutionVertex v = new ExecutionVertex(2, 0, 2,
                List.of(new MapOperator<>((org.miniflink.api.function.MapFunction<Integer, Integer>) x -> x)), null);
        // 注意：sourceOperator 用一个非 null 占位才 isSource=true；这里传 null → 非 source
        assertFalse(v.isSource());
        assertEquals(1, v.getOperators().size());
        assertEquals(0, v.getSubtaskIndex());
        assertEquals(2, v.getParallelism());
    }

    @Test
    void edge持有上下游与分区器() {
        ExecutionVertex a = new ExecutionVertex(1, 0, 1, List.of(), null);
        ExecutionVertex b = new ExecutionVertex(2, 0, 1, List.of(), null);
        KeySelector<Integer, Integer> ks = x -> x;
        ExecutionEdge edge = new ExecutionEdge(List.of(a), List.of(b), new HashPartitioner(), ks);

        assertEquals(1, edge.getSources().size());
        assertEquals(1, edge.getTargets().size());
        assertInstanceOf(HashPartitioner.class, edge.getPartitioner());
        assertSame(ks, edge.getKeySelector());
    }
}
```

> Step 1 已含完整 3 个测试：source vertex / 处理 vertex / edge。实现时直接用，跳过原 Step 5 的占位修正（Step 5 已废弃，执行时跳过，Step 6 运行验证、Step 7 提交顺延为 Step 5、Step 6）。

- [ ] **Step 2: 运行测试验证失败**

Run: `mvn -q -Dtest=ExecutionVertexEdgeTest test`
Expected: 编译失败 —— `ExecutionVertex`/`ExecutionEdge` 不存在。

- [ ] **Step 3: 创建 `ExecutionVertex`**

```java
package org.miniflink.execution;

import org.miniflink.runtime.Operator;
import org.miniflink.runtime.SourceOperator;

import java.util.List;

/**
 * 物理执行计划的 subtask。
 * source vertex：operators 为空、sourceOperator 非 null。
 * 处理 vertex：operators 为链化算子序列、sourceOperator 为 null。
 */
public class ExecutionVertex {
    private final int id;
    private final int subtaskIndex;
    private final int parallelism;
    private final List<Operator<?, ?>> operators;
    private final SourceOperator<?> sourceOperator;

    public ExecutionVertex(int id, int subtaskIndex, int parallelism,
                           List<Operator<?, ?>> operators, SourceOperator<?> sourceOperator) {
        this.id = id;
        this.subtaskIndex = subtaskIndex;
        this.parallelism = parallelism;
        this.operators = operators;
        this.sourceOperator = sourceOperator;
    }

    public boolean isSource() {
        return sourceOperator != null;
    }

    public int getId() {
        return id;
    }

    public int getSubtaskIndex() {
        return subtaskIndex;
    }

    public int getParallelism() {
        return parallelism;
    }

    public List<Operator<?, ?>> getOperators() {
        return operators;
    }

    public SourceOperator<?> getSourceOperator() {
        return sourceOperator;
    }
}
```

- [ ] **Step 4: 创建 `ExecutionEdge`**

```java
package org.miniflink.execution;

import org.miniflink.api.function.KeySelector;

import java.util.List;

/** 物理边：上游 vertices 组 → 下游 vertices 组，含分区器与 keySelector（hash 用）。 */
public class ExecutionEdge {
    private final List<ExecutionVertex> sources;
    private final List<ExecutionVertex> targets;
    private final Partitioner partitioner;
    private final KeySelector<?, ?> keySelector;

    public ExecutionEdge(List<ExecutionVertex> sources, List<ExecutionVertex> targets,
                         Partitioner partitioner, KeySelector<?, ?> keySelector) {
        this.sources = sources;
        this.targets = targets;
        this.partitioner = partitioner;
        this.keySelector = keySelector;
    }

    public List<ExecutionVertex> getSources() {
        return sources;
    }

    public List<ExecutionVertex> getTargets() {
        return targets;
    }

    public Partitioner getPartitioner() {
        return partitioner;
    }

    public KeySelector<?, ?> getKeySelector() {
        return keySelector;
    }
}
```

- [ ] **Step 5: 修正测试（删除占位测试，补 source vertex 测试）**

把 `ExecutionVertexEdgeTest` 的第一个测试方法替换为：

```java
    @Test
    void sourceVertex判定为source() {
        SourceOperator<String> srcOp = new org.miniflink.runtime.operator.SourceOperatorImpl<>(
                new org.miniflink.connector.CollectionSource<>(java.util.List.of("x")));
        ExecutionVertex sv = new ExecutionVertex(1, 0, 1, java.util.List.of(), srcOp);
        assertTrue(sv.isSource());
        assertNull(sv.getOperators());
    }
```

- [ ] **Step 6: 运行测试验证通过**

Run: `mvn -q -Dtest=ExecutionVertexEdgeTest test`
Expected: `BUILD SUCCESS`，`Tests run: 3, Failures: 0`

- [ ] **Step 7: 提交**

```bash
git add src/main/java/org/miniflink/execution/ExecutionVertex.java \
        src/main/java/org/miniflink/execution/ExecutionEdge.java \
        src/test/java/org/miniflink/execution/ExecutionVertexEdgeTest.java
git commit -m "feat(execution): 添加 ExecutionVertex/ExecutionEdge（物理计划节点）

Co-Authored-By: Claude <noreply@anthropic.com>"
```

## Task 10: ExecutionGraph.from 重构 + StreamExecutor 多线程重构

> 这两个重构编译耦合（StreamExecutor 依赖 ExecutionGraph 的新结构），必须同任务完成。

**Files:**
- Rewrite: `src/main/java/org/miniflink/execution/ExecutionGraph.java`（vertices/edges + from 展开链化）
- Rewrite: `src/main/java/org/miniflink/runtime/StreamExecutor.java`（多线程 Task + join + 异常传播）
- Test: `src/test/java/org/miniflink/execution/ExecutionGraphFromTest.java`
- Test: `src/test/java/org/miniflink/execution/StreamExecutorTest.java`

**Interfaces:**
- Consumes: `ExecutionVertex`/`ExecutionEdge`（Task 9）、`OneInputTransformation.getPartitioner()/getKeySelector()`（Task 5）、`OperatorChain`/`SourceTask`/`OperatorTask`/`Output`/`Channel`（Task 2-8）。
- Produces: `ExecutionGraph(List<ExecutionVertex>, List<ExecutionEdge>)`，`getVertices()`/`getEdges()`，`from(StreamGraph)`；`StreamExecutor.execute(ExecutionGraph)` 多线程。

- [ ] **Step 1: 写失败测试 `ExecutionGraphFromTest`**

```java
package org.miniflink.execution;

import org.junit.jupiter.api.Test;
import org.miniflink.api.function.MapFunction;
import org.miniflink.connector.CollectionSource;
import org.miniflink.graph.OneInputTransformation;
import org.miniflink.graph.SourceTransformation;
import org.miniflink.graph.StreamGraph;
import org.miniflink.runtime.operator.MapOperator;
import org.miniflink.runtime.operator.SinkOperator;
import org.miniflink.runtime.operator.SourceOperatorImpl;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ExecutionGraphFromTest {

    private StreamGraph buildLinearGraph() {
        StreamGraph sg = new StreamGraph();
        SourceTransformation<Integer> src = new SourceTransformation<>(1, "source",
                new SourceOperatorImpl<>(new CollectionSource<>(List.of(1, 2, 3))));
        OneInputTransformation<Integer, Integer> map = new OneInputTransformation<>(2, "map", src,
                new MapOperator<>((MapFunction<Integer, Integer>) x -> x * 10));
        OneInputTransformation<Integer, Void> sink = new OneInputTransformation<>(3, "sink", map,
                new SinkOperator<>(v -> {}));
        sg.addTransformation(src);
        sg.addTransformation(map);
        sg.addSink(sink);
        return sg;
    }

    @Test
    void 单线性链应展开为source与chain两组vertices与一条边() {
        ExecutionGraph g = ExecutionGraph.from(buildLinearGraph());
        // source(p1) + [map,sink] chain(p1) → 2 vertices, 1 edge
        assertEquals(2, g.getVertices().size());
        assertTrue(g.getVertices().get(0).isSource());
        assertFalse(g.getVertices().get(1).isSource());
        assertEquals(2, g.getVertices().get(1).getOperators().size()); // map + sink 链化
        assertEquals(1, g.getEdges().size());
    }

    @Test
    void 多sink应抛IllegalStateException() {
        StreamGraph sg = new StreamGraph();
        SourceTransformation<Integer> src = new SourceTransformation<>(1, "s",
                new SourceOperatorImpl<>(new CollectionSource<>(List.of(1))));
        OneInputTransformation<Integer, Void> sink1 = new OneInputTransformation<>(2, "sink1", src,
                new SinkOperator<>(v -> {}));
        OneInputTransformation<Integer, Void> sink2 = new OneInputTransformation<>(3, "sink2", src,
                new SinkOperator<>(v -> {}));
        sg.addTransformation(src);
        sg.addSink(sink1);
        sg.addSink(sink2);
        assertThrows(IllegalStateException.class, () -> ExecutionGraph.from(sg));
    }
}
```

- [ ] **Step 2: 写失败测试 `StreamExecutorTest`**

```java
package org.miniflink.execution;

import org.junit.jupiter.api.Test;
import org.miniflink.api.StreamExecutionEnvironment;
import org.miniflink.connector.CollectSink;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class StreamExecutorTest {

    @Test
    void 多线程端到端执行单并行度链() throws Exception {
        StreamExecutionEnvironment env = new StreamExecutionEnvironment();
        CollectSink<Integer> sink = new CollectSink<>();
        env.fromCollection(List.of(1, 2, 3))
           .map(x -> x * 10)
           .addSink(sink::add);

        env.execute("test");

        // 单并行度 forward，顺序保持
        assertEquals(List.of(10, 20, 30), sink.getResults());
    }
}
```

- [ ] **Step 3: 运行测试验证失败**

Run: `mvn -q -Dtest=ExecutionGraphFromTest,StreamExecutorTest test`
Expected: 失败 —— `ExecutionGraph.from` 仍是阶段①单线性链逻辑 / StreamExecutor 仍单线程。

- [ ] **Step 4: 重写 `ExecutionGraph`**

整体替换 `src/main/java/org/miniflink/execution/ExecutionGraph.java`：

```java
package org.miniflink.execution;

import org.miniflink.api.function.KeySelector;
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
 * 物理执行计划：vertices（subtask）+ edges（组间边）。
 * from：回溯单线性链 → 链化分组（forward 同并行度合并）→ 按 parallelism 展开 → 组间边。
 * 单 sink 约束（与阶段①一致）。
 */
public class ExecutionGraph {
    private final List<ExecutionVertex> vertices;
    private final List<ExecutionEdge> edges;

    public ExecutionGraph(List<ExecutionVertex> vertices, List<ExecutionEdge> edges) {
        this.vertices = vertices;
        this.edges = edges;
    }

    public List<ExecutionVertex> getVertices() {
        return vertices;
    }

    public List<ExecutionEdge> getEdges() {
        return edges;
    }

    public static ExecutionGraph from(StreamGraph streamGraph) {
        List<Transformation<?>> sinks = streamGraph.getSinks();
        if (sinks.size() != 1) {
            throw new IllegalStateException("阶段②仅支持单个 sink，当前 sinks=" + sinks.size());
        }

        // 回溯得 sequence [sink.., source]，反转后 [source, op1, ..., sink]
        List<Transformation<?>> seq = new ArrayList<>();
        Transformation<?> cur = sinks.get(0);
        while (cur instanceof OneInputTransformation<?, ?> one) {
            seq.add(one);
            cur = one.getInput();
        }
        if (!(cur instanceof SourceTransformation<?> srcTx)) {
            throw new IllegalStateException("链回溯未终止于 source 节点");
        }
        seq.add(srcTx);
        Collections.reverse(seq);

        // 链化分组：source 单独一组；处理算子连续 forward 同并行度合并
        List<Group> groups = new ArrayList<>();
        groups.add(new Group(true, srcTx.getOperator(), new ArrayList<>(), srcTx.getParallelism(), null, null));

        List<Operator<?, ?>> curOps = new ArrayList<>();
        int curParallelism = -1;
        Partitioner curPart = null;
        KeySelector<?, ?> curKey = null;
        for (int i = 1; i < seq.size(); i++) {
            OneInputTransformation<?, ?> tx = (OneInputTransformation<?, ?>) seq.get(i);
            Partitioner part = tx.getPartitioner();
            KeySelector<?, ?> key = tx.getKeySelector();
            int p = tx.getParallelism();
            boolean canChain = !curOps.isEmpty() && (part instanceof ForwardPartitioner) && p == curParallelism;
            if (canChain) {
                curOps.add(tx.getOperator());
            } else {
                if (!curOps.isEmpty()) {
                    groups.add(new Group(false, null, new ArrayList<>(curOps), curParallelism, curPart, curKey));
                }
                curOps = new ArrayList<>();
                curOps.add(tx.getOperator());
                curParallelism = p;
                curPart = part;
                curKey = key;
            }
        }
        if (!curOps.isEmpty()) {
            groups.add(new Group(false, null, new ArrayList<>(curOps), curParallelism, curPart, curKey));
        }

        // 按 parallelism 展开
        int id = 0;
        List<List<ExecutionVertex>> groupVerts = new ArrayList<>();
        List<ExecutionVertex> allVerts = new ArrayList<>();
        for (Group g : groups) {
            List<ExecutionVertex> verts = new ArrayList<>();
            for (int i = 0; i < g.parallelism; i++) {
                ExecutionVertex v = g.isSource
                        ? new ExecutionVertex(id++, i, g.parallelism, List.of(), g.source)
                        : new ExecutionVertex(id++, i, g.parallelism, g.operators, null);
                verts.add(v);
                allVerts.add(v);
            }
            groupVerts.add(verts);
        }

        // 组间边；forward 但并行度不同时自动改 rebalance
        List<ExecutionEdge> edges = new ArrayList<>();
        for (int g = 1; g < groups.size(); g++) {
            List<ExecutionVertex> srcs = groupVerts.get(g - 1);
            List<ExecutionVertex> tgts = groupVerts.get(g);
            Partitioner part = groups.get(g).inputPartitioner;
            if (part instanceof ForwardPartitioner && srcs.size() != tgts.size()) {
                part = new RebalancePartitioner();
            }
            edges.add(new ExecutionEdge(srcs, tgts, part, groups.get(g).inputKeySelector));
        }

        return new ExecutionGraph(allVerts, edges);
    }

    /** 链化分组中间结构。 */
    private static final class Group {
        final boolean isSource;
        final SourceOperator<?> source;
        final List<Operator<?, ?>> operators;
        final int parallelism;
        final Partitioner inputPartitioner; // 该组入边（与上一组）的分区器
        final KeySelector<?, ?> inputKeySelector;

        Group(boolean isSource, SourceOperator<?> source, List<Operator<?, ?>> operators,
              int parallelism, Partitioner inputPartitioner, KeySelector<?, ?> inputKeySelector) {
            this.isSource = isSource;
            this.source = source;
            this.operators = operators;
            this.parallelism = parallelism;
            this.inputPartitioner = inputPartitioner;
            this.inputKeySelector = inputKeySelector;
        }
    }
}
```

- [ ] **Step 5: 重写 `StreamExecutor`**

整体替换 `src/main/java/org/miniflink/runtime/StreamExecutor.java`：

```java
package org.miniflink.runtime;

import org.miniflink.execution.ExecutionEdge;
import org.miniflink.execution.ExecutionGraph;
import org.miniflink.execution.ExecutionVertex;
import org.miniflink.execution.ForwardPartitioner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 多线程执行器：为每个 ExecutionVertex 建 Task，Task 间用 Channel 连接，启动线程并 join 等待。
 * 任一 Task 未捕获异常 → 记录并在 join 后抛出。
 */
public class StreamExecutor {

    public void execute(ExecutionGraph graph) throws Exception {
        // 1. 为每个 target vertex 建输入 Channel（fan-in 汇聚）
        Map<ExecutionVertex, Channel> inputChannelOf = new HashMap<>();
        for (ExecutionEdge edge : graph.getEdges()) {
            for (ExecutionVertex t : edge.getTargets()) {
                inputChannelOf.computeIfAbsent(t, k -> new Channel());
            }
        }

        // 2. 为每个 vertex 建 Task
        List<Task> tasks = new ArrayList<>();
        for (ExecutionVertex v : graph.getVertices()) {
            List<Output> outputs = buildOutputs(v, graph.getEdges(), inputChannelOf);
            if (v.isSource()) {
                tasks.add(new SourceTask(v.getSourceOperator(), outputs, v.getSubtaskIndex(), v.getParallelism()));
            } else {
                Channel input = inputChannelOf.get(v);
                int pending = countUpstreams(v, graph.getEdges());
                tasks.add(new OperatorTask(new OperatorChain<>(v.getOperators()), input, pending, outputs, v.getSubtaskIndex()));
            }
        }

        // 3. 启动所有线程
        List<Thread> threads = new ArrayList<>();
        AtomicReference<Throwable> error = new AtomicReference<>();
        for (Task t : tasks) {
            Thread th = new Thread(t, "miniflink-task-" + threads.size());
            th.setUncaughtExceptionHandler((tr, e) -> error.compareAndSet(null, e));
            threads.add(th);
            th.start();
        }

        // 4. join 等待全部
        for (Thread th : threads) {
            th.join();
        }

        // 5. 异常传播
        if (error.get() != null) {
            throw new RuntimeException("作业执行失败", error.get());
        }
    }

    private List<Output> buildOutputs(ExecutionVertex v, List<ExecutionEdge> edges,
                                      Map<ExecutionVertex, Channel> inputChannelOf) {
        List<Output> outputs = new ArrayList<>();
        for (ExecutionEdge edge : edges) {
            if (edge.getSources().contains(v)) {
                List<Channel> targetChannels = new ArrayList<>();
                for (ExecutionVertex t : edge.getTargets()) {
                    targetChannels.add(inputChannelOf.get(t));
                }
                outputs.add(new Output(targetChannels, edge.getPartitioner(), edge.getKeySelector()));
            }
        }
        return outputs;
    }

    private int countUpstreams(ExecutionVertex v, List<ExecutionEdge> edges) {
        int pending = 0;
        for (ExecutionEdge edge : edges) {
            if (edge.getTargets().contains(v)) {
                if (edge.getPartitioner() instanceof ForwardPartitioner) {
                    pending += 1; // forward 一对一：下游.i 只有一个上游
                } else {
                    pending += edge.getSources().size(); // fan-in：所有上游都会发
                }
            }
        }
        return pending;
    }
}
```

- [ ] **Step 6: 运行测试验证通过 + 全量回归**

Run: `mvn -q -Dtest=ExecutionGraphFromTest,StreamExecutorTest test`
Expected: `BUILD SUCCESS`，`Tests run: 3, Failures: 0`

Run: `mvn -q test`
Expected: 全量通过。阶段①的 `EndToEndExecutionTest` / `TextProcessingExampleTest` 仍通过（单并行度同步等价语义）。

- [ ] **Step 7: 提交**

```bash
git add src/main/java/org/miniflink/execution/ExecutionGraph.java \
        src/main/java/org/miniflink/runtime/StreamExecutor.java \
        src/test/java/org/miniflink/execution/ExecutionGraphFromTest.java \
        src/test/java/org/miniflink/execution/StreamExecutorTest.java
git commit -m "feat(execution): ExecutionGraph.from 展开链化 + StreamExecutor 多线程

Co-Authored-By: Claude <noreply@anthropic.com>"
```

## Task 11: API — setParallelism + keyBy

**Files:**
- Rewrite: `src/main/java/org/miniflink/api/DataStream.java`（加 setParallelism + keyBy + 下游分区器透传）
- Test: `src/test/java/org/miniflink/api/DataStreamKeyByTest.java`

**Interfaces:**
- Consumes: `Partitioner`/`ForwardPartitioner`/`HashPartitioner`、`KeySelector`（Task 5）、`OneInputTransformation`（Task 5 改造）。
- Produces: `DataStream.setParallelism(int)`、`DataStream.keyBy(KeySelector)`；`keyBy` 使下一个 transformation 的入边用 hash 分区。

- [ ] **Step 1: 写失败测试 `DataStreamKeyByTest`**

```java
package org.miniflink.api;

import org.junit.jupiter.api.Test;
import org.miniflink.api.function.KeySelector;
import org.miniflink.execution.HashPartitioner;
import org.miniflink.graph.OneInputTransformation;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class DataStreamKeyByTest {

    @Test
    void keyBy应使下一个算子入边用hash分区() {
        StreamExecutionEnvironment env = new StreamExecutionEnvironment();
        DataStream<Integer> keyed = env.fromCollection(List.of(1, 2, 3)).keyBy((KeySelector<Integer, Integer>) x -> x);
        DataStream<Integer> mapped = keyed.map(x -> x);

        OneInputTransformation<?, ?> mapTx = (OneInputTransformation<?, ?>) mapped.getTransformation();
        assertInstanceOf(HashPartitioner.class, mapTx.getPartitioner());
        assertNotNull(mapTx.getKeySelector());
    }

    @Test
    void keyBy后非keyBy算子应恢复forward() {
        StreamExecutionEnvironment env = new StreamExecutionEnvironment();
        DataStream<Integer> s = env.fromCollection(List.of(1, 2, 3)).keyBy((KeySelector<Integer, Integer>) x -> x).map(x -> x);
        DataStream<Integer> m2 = s.map(x -> x); // 不再 keyBy

        OneInputTransformation<?, ?> tx = (OneInputTransformation<?, ?>) m2.getTransformation();
        assertInstanceOf(org.miniflink.execution.ForwardPartitioner.class, tx.getPartitioner());
    }

    @Test
    void setParallelism应设到transformation() {
        StreamExecutionEnvironment env = new StreamExecutionEnvironment();
        DataStream<Integer> src = env.fromCollection(List.of(1, 2, 3)).setParallelism(2);
        assertEquals(2, src.getTransformation().getParallelism());
    }
}
```

- [ ] **Step 2: 运行测试验证失败**

Run: `mvn -q -Dtest=DataStreamKeyByTest test`
Expected: 编译失败 —— `setParallelism`/`keyBy` 不存在。

- [ ] **Step 3: 重写 `DataStream`**

整体替换 `src/main/java/org/miniflink/api/DataStream.java`：

```java
package org.miniflink.api;

import org.miniflink.api.function.FilterFunction;
import org.miniflink.api.function.FlatMapFunction;
import org.miniflink.api.function.KeySelector;
import org.miniflink.api.function.MapFunction;
import org.miniflink.api.function.SinkFunction;
import org.miniflink.execution.ForwardPartitioner;
import org.miniflink.execution.HashPartitioner;
import org.miniflink.execution.Partitioner;
import org.miniflink.graph.OneInputTransformation;
import org.miniflink.graph.Transformation;
import org.miniflink.runtime.Operator;
import org.miniflink.runtime.operator.FilterOperator;
import org.miniflink.runtime.operator.FlatMapOperator;
import org.miniflink.runtime.operator.MapOperator;
import org.miniflink.runtime.operator.SinkOperator;

/** 流抽象：链式调用算子方法。keyBy 设置下一条边的分区器；setParallelism 设并行度。 */
public class DataStream<T> {
    private final StreamExecutionEnvironment env;
    private final Transformation<T> transformation;
    private Partitioner nextPartitioner = null;   // keyBy 设置，供下一个 transformation 使用
    private KeySelector<T, ?> nextKeySelector = null;

    public DataStream(StreamExecutionEnvironment env, Transformation<T> transformation) {
        this.env = env;
        this.transformation = transformation;
    }

    public Transformation<T> getTransformation() {
        return transformation;
    }

    /** 设置当前 transformation 的并行度。 */
    public DataStream<T> setParallelism(int parallelism) {
        transformation.setParallelism(parallelism);
        return this;
    }

    /** 按key分区：使下一个算子的入边用 hash 分区。返回的流与原流共享同一 transformation。 */
    public DataStream<T> keyBy(KeySelector<T, ?> keySelector) {
        DataStream<T> keyed = new DataStream<>(env, transformation);
        keyed.nextPartitioner = new HashPartitioner();
        keyed.nextKeySelector = keySelector;
        return keyed;
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
        Partitioner part = (nextPartitioner != null) ? nextPartitioner : new ForwardPartitioner();
        OneInputTransformation<T, Void> sink = new OneInputTransformation<>(
                env.getNewNodeId(), "sink", transformation, new SinkOperator<>(sinkFunction), part, nextKeySelector);
        env.addSink(sink);
    }

    private <O> DataStream<O> transform(String name, Operator<T, O> operator) {
        Partitioner part = (nextPartitioner != null) ? nextPartitioner : new ForwardPartitioner();
        OneInputTransformation<T, O> tx = new OneInputTransformation<>(
                env.getNewNodeId(), name, transformation, operator, part, nextKeySelector);
        env.addTransformation(tx);
        return new DataStream<>(env, tx); // 新流重置为 forward（不继承 keyBy）
    }
}
```

- [ ] **Step 4: 运行测试验证通过 + 全量回归**

Run: `mvn -q -Dtest=DataStreamKeyByTest test`
Expected: `BUILD SUCCESS`，`Tests run: 3, Failures: 0`

Run: `mvn -q test`
Expected: 全量通过（阶段① DataStreamApiTest 仍通过：transform 链结构不变）。

- [ ] **Step 5: 提交**

```bash
git add src/main/java/org/miniflink/api/DataStream.java \
        src/test/java/org/miniflink/api/DataStreamKeyByTest.java
git commit -m "feat(api): 添加 setParallelism 与 keyBy（hash 分区）

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

## Task 12: list 封装加固（StreamGraph / CollectSink 返回不可变视图）

final review flag 项：防止跨线程误改共享 list。

**Files:**
- Modify: `src/main/java/org/miniflink/graph/StreamGraph.java`（getter 返回 `unmodifiableList`）
- Modify: `src/main/java/org/miniflink/connector/CollectSink.java`（`getResults` 返回 `unmodifiableList`）
- Test: `src/test/java/org/miniflink/ListEncapsulationTest.java`

- [ ] **Step 1: 写失败测试 `ListEncapsulationTest`**

```java
package org.miniflink;

import org.junit.jupiter.api.Test;
import org.miniflink.connector.CollectionSource;
import org.miniflink.connector.CollectSink;
import org.miniflink.graph.SourceTransformation;
import org.miniflink.graph.StreamGraph;
import org.miniflink.runtime.operator.SourceOperatorImpl;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertThrows;

class ListEncapsulationTest {

    @Test
    void StreamGraph的getter返回不可变视图() {
        StreamGraph sg = new StreamGraph();
        sg.addTransformation(new SourceTransformation<>(1, "s",
                new SourceOperatorImpl<>(new CollectionSource<>(List.of(1)))));
        assertThrows(UnsupportedOperationException.class, () -> sg.getTransformations().add(null));
        assertThrows(UnsupportedOperationException.class, () -> sg.getSinks().add(null));
    }

    @Test
    void CollectSink的getResults返回不可变视图() {
        CollectSink<Integer> sink = new CollectSink<>();
        sink.add(1);
        assertThrows(UnsupportedOperationException.class, () -> sink.getResults().add(2));
    }
}
```

- [ ] **Step 2: 运行测试验证失败**

Run: `mvn -q -Dtest=ListEncapsulationTest test`
Expected: 失败 —— getter 仍返回可变 list。

- [ ] **Step 3: 修改 `StreamGraph`**

把 `StreamGraph.java` 的 `getTransformations`/`getSinks` 改为返回不可变视图（加 `import java.util.Collections;`）：

```java
    public List<Transformation<?>> getTransformations() {
        return Collections.unmodifiableList(transformations);
    }

    public List<Transformation<?>> getSinks() {
        return Collections.unmodifiableList(sinks);
    }
```

- [ ] **Step 4: 修改 `CollectSink`**

把 `CollectSink.java` 的 `getResults` 改为（已 import `Collections`）：

```java
    public List<T> getResults() {
        return Collections.unmodifiableList(results);
    }
```

- [ ] **Step 5: 运行测试验证通过 + 全量回归**

Run: `mvn -q -Dtest=ListEncapsulationTest test`
Expected: `BUILD SUCCESS`，`Tests run: 2, Failures: 0`

Run: `mvn -q test`
Expected: 全量通过。

- [ ] **Step 6: 提交**

```bash
git add src/main/java/org/miniflink/graph/StreamGraph.java \
        src/main/java/org/miniflink/connector/CollectSink.java \
        src/test/java/org/miniflink/ListEncapsulationTest.java
git commit -m "feat: StreamGraph/CollectSink getter 返回不可变视图（final review flag）

Co-Authored-By: Claude <noreply@anthropic.com>"
```

## Task 13: 阶段②验收 — 多并行度端到端

**Files:**
- Test: `src/test/java/org/miniflink/examples/ParallelEndToEndTest.java`
- Create: `docs/examples/parallel.md`

**Interfaces:** Consumes 全部阶段② API（Task 10-12）。

- [ ] **Step 1: 写验收测试 `ParallelEndToEndTest`**

```java
package org.miniflink.examples;

import org.junit.jupiter.api.Test;
import org.miniflink.api.StreamExecutionEnvironment;
import org.miniflink.connector.CollectSink;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * 阶段②验收：parallelism=2 下，数据被分片到 2 个 source subtask，
 * 经 forward 到 2 个 map subtask 并行处理，CollectSink 汇总。
 */
class ParallelEndToEndTest {

    @Test
    void 多并行度forward下数据正确并行处理() throws Exception {
        StreamExecutionEnvironment env = new StreamExecutionEnvironment();
        CollectSink<Integer> sink = new CollectSink<>();

        env.fromCollection(List.of(1, 2, 3, 4, 5, 6))
           .setParallelism(2)        // 2 个 source subtask 各取一半
           .map(x -> x * 10)
           .setParallelism(2)        // 2 个 map subtask，forward 一对一
           .addSink(sink::add);

        env.execute("parallel");

        // 多线程顺序不定，排序后比较
        List<Integer> sorted = new ArrayList<>(sink.getResults());
        Collections.sort(sorted);
        assertEquals(List.of(10, 20, 30, 40, 50, 60), sorted);
    }
}
```

- [ ] **Step 2: 运行验收测试**

Run: `mvn -q -Dtest=ParallelEndToEndTest test`
Expected: `BUILD SUCCESS`，`Tests run: 1, Failures: 0`

- [ ] **Step 3: 创建示例文档 `docs/examples/parallel.md`**

````markdown
# 阶段②示例：多并行度并行处理

演示 `parallelism > 1` 下的多线程并行执行。

## 作业逻辑

```java
StreamExecutionEnvironment env = new StreamExecutionEnvironment();
CollectSink<Integer> sink = new CollectSink<>();

env.fromCollection(List.of(1, 2, 3, 4, 5, 6))
   .setParallelism(2)        // 2 个 source subtask 各取一半数据
   .map(x -> x * 10)
   .setParallelism(2)        // 2 个 map subtask，forward 一对一
   .addSink(sink::add);

env.execute("parallel");
// 结果（排序后）：[10, 20, 30, 40, 50, 60]
```

## 执行模型（阶段②）

- source parallelism=2：数据按 subtaskIndex 取模分片（subtask 0 取索引 0/2/4，subtask 1 取 1/3/5）。
- 每个 subtask 一个线程（`Task`），subtask 间用有界 `Channel` 连接。
- forward 分区：source.0 → map.0、source.1 → map.1（一对一）。
- 反压：Channel 满则上游阻塞。
- 关闭：source 结束发 EOB，经引用计数对齐级联退出。
````

- [ ] **Step 4: 运行全量测试做最终回归**

Run: `mvn -q test`
Expected: `BUILD SUCCESS`，全量通过（阶段① 12 + 阶段②新增测试）。

- [ ] **Step 5: 提交**

```bash
git add src/test/java/org/miniflink/examples/ParallelEndToEndTest.java \
        docs/examples/parallel.md
git commit -m "docs(examples): 添加阶段②多并行度验收示例

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

## Self-Review

### 1. Spec 覆盖（对照 `2026-07-10-mini-flink-stage2-parallel-design.md`）

| spec 节 | 覆盖任务 |
|---|---|
| StreamElement/Record | Task 1 |
| 有界 Channel + 反压 | Task 2 |
| Collector 稳定边界（ChannelWriter） | Task 3 |
| 算子链（OperatorChain/ChainCollector） | Task 4 |
| 三分区器 + KeySelector | Task 5 |
| Source 并行分片（CollectionSource subtaskIndex） | Task 6 |
| fan-out（Output/OutputCollector） | Task 7 |
| 多线程 Task + EOB 引用计数对齐 | Task 8 |
| ExecutionVertex/Edge | Task 9 |
| parallelism 展开 + 链化 + 分区边 + 两分支单测 | Task 10 |
| StreamExecutor 多线程 + 异常传播 | Task 10 |
| setParallelism + keyBy API | Task 11 |
| list 封装加固（final review flag） | Task 12 |
| 多并行度验收 | Task 13 |

全覆盖。spec §4.8「两分支 IllegalStateException 单测」：Task 10 覆盖「多 sink」分支；「回溯未终止 source」在合法 StreamGraph 不会触发，保留代码分支但不单独构造测试（注释说明）。

### 2. Placeholder 扫描

- Task 9 Step 1 最初含占位测试，已在段 9 前**就地修正**为完整 source vertex 测试；Step 5（占位修正步骤）已标注废弃、执行时跳过、后续编号顺延。
- 其余无 TBD/TODO/"适当处理"等占位。

### 3. 类型一致性

跨任务签名以「跨任务类型契约」为准，各任务实现严格匹配：`Collector` 不变；`SourceOperator.open(Collector, int, int)`；`Channel.send/receive`；`OperatorChain.open/processElement/close`；`Output.route/sendEob`；`Task.broadcastEob`；`SourceTask/OperatorTask` 构造签名；`ExecutionVertex/Edge`；`ExecutionGraph.from/getVertices/getEdges`；`Partitioner.selectChannel`。已交叉核对，无 `clearLayers`/`clearFullLayers` 式不一致。

### 已知简化（Self-Review 接受，非缺陷）

- `StreamExecutor`/`OperatorChain`/`Output.route` 用 raw type + `@SuppressWarnings`（异构算子链无法用 wildcard 表达），与阶段① `StreamExecutor` 一致。
- `RebalancePartitioner` 用全局 `AtomicInteger` 轮询（Flink 是 per-subtask 轮询）；学习项目近似可接受，注释说明。
- `ExecutionGraph.from` 仅处理单线性链 + 单 sink（与阶段①一致，spec §2 明确）。
- 线程异常：任一 Task 异常 → `UncaughtExceptionHandler` 记录，join 后抛出；未做"中断其他 Task"（其他 Task 会因 Channel 阻塞或 EOB 自然结束或挂起——后续阶段可加超时/中断）。

