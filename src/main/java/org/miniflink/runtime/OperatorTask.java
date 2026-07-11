package org.miniflink.runtime;

import java.util.List;
import java.util.Map;

/** 处理算子执行单元：open chain → 循环读 InputGate → Record 经 chain 处理 → EOB 计数；归零广播 EOB。 */
public class OperatorTask implements Task {
    private final OperatorChain<?, ?> chain;
    private final InputGate input;
    private final int pendingUpstreams;
    private final List<Output> outputs;
    private final RuntimeContext ctx;
    private final CheckpointCoordinator coordinator;   // Phase 1 为 null（占位）
    private final String snapshotKey;                   // checkpoint 用

    public OperatorTask(OperatorChain<?, ?> chain, List<InputChannel> inputChannels, int pendingUpstreams,
                        List<Output> outputs, RuntimeContext ctx) {
        this(chain, inputChannels, pendingUpstreams, outputs, ctx, null, null);
    }

    public OperatorTask(OperatorChain<?, ?> chain, List<InputChannel> inputChannels, int pendingUpstreams,
                        List<Output> outputs, RuntimeContext ctx,
                        CheckpointCoordinator coordinator, String snapshotKey) {
        this.chain = chain;
        this.pendingUpstreams = pendingUpstreams;
        this.outputs = outputs;
        this.ctx = ctx;
        this.coordinator = coordinator;
        this.snapshotKey = snapshotKey;
        this.input = new InputGate(inputChannels, this::onAligned, this::forwardBarrier);
    }

    /** InputGate 全部上游对齐时回调：快照 backend + 算子状态 → ack coordinator。 */
    void onAligned(long checkpointId) throws Exception {
        if (coordinator == null) {
            return;   // Phase 1 占位（无 coordinator，不快照）
        }
        StateSnapshot keyed = ctx.getStateBackend().snapshot();
        Map<Integer, OperatorState> ops = chain.snapshotState();
        SubtaskSnapshot snap = new SubtaskSnapshot(keyed, -1L, ops);   // 非源 subtask，offset=-1
        coordinator.ack(snapshotKey, checkpointId, snap);
    }

    /** 对齐后向所有下游广播 barrier。 */
    private void forwardBarrier(Barrier barrier) {
        broadcastBarrier(outputs, barrier);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Override
    public void run() {
        Collector outCollector = outputs.isEmpty() ? new NoopCollector<>() : new OutputCollector(outputs, ctx);
        try {
            // 算子通过 ctx.emitWatermark 发 watermark；emitter 广播到本 subtask 的 outputs（保持 watermark 流）
            if (ctx instanceof RuntimeContextImpl impl) {
                impl.setWatermarkEmitter(wm -> broadcastWatermark(outputs, wm));
            }
            chain.open((Collector) outCollector, ctx);
            @SuppressWarnings("rawtypes")
            OperatorChain rawChain = chain;
            int remaining = pendingUpstreams;
            while (remaining > 0) {
                StreamElement e = input.receive();          // InputGate 内部消费 Barrier
                if (e == EndOfBroadcast.INSTANCE) {
                    remaining--;
                } else if (e instanceof Record<?> r) {
                    ctx.setCurrentTimestamp(r.timestamp());   // 入链：设当前 ts
                    rawChain.processElement(r.value());
                } else if (e instanceof Watermark wm) {
                    chain.onWatermark(wm);                       // 链内算子处理（WindowOperator 触发窗口）
                    broadcastWatermark(outputs, wm);             // 转发到下游（保持 watermark 流）
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
