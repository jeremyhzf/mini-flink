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
            // chain(IN=?) 与 Record(value=?) 各持独立的通配符 capture，互不兼容，
            // 经 raw OperatorChain 调用绕过泛型检查（运行时类型由算子链保证安全）。
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
            broadcastEob(outputs, subtaskIndex); // 所有上游结束，向下游广播
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            throw new RuntimeException("OperatorTask 执行异常", e);
        } finally {
            chain.close();
        }
    }
}
