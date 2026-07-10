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
