package org.miniflink.runtime;

import java.util.List;
import java.util.Map;
import org.miniflink.runtime.operator.SourceOperatorImpl;
import org.miniflink.state.StateSnapshot;

/** source 执行单元：open source（注入 RuntimeContext）→ run → 正常结束广播 EOB。 */
public class SourceTask implements Task {
    private final SourceOperator<?> sourceOperator;
    private final List<Output> outputs;
    private final RuntimeContext ctx;
    private final CheckpointCoordinator coordinator;   // Phase 1 为 null（占位）；Task 10 注入后 requestCheckpoint 生效
    private final String snapshotKey;                   // checkpoint 用
    private final long restoreOffset;                   // 恢复时跳过前 N 条（-1=冷启）

    public SourceTask(SourceOperator<?> sourceOperator, List<Output> outputs, RuntimeContext ctx) {
        this(sourceOperator, outputs, ctx, null, null, -1L);
    }

    public SourceTask(SourceOperator<?> sourceOperator, List<Output> outputs, RuntimeContext ctx,
                      CheckpointCoordinator coordinator, String snapshotKey) {
        this(sourceOperator, outputs, ctx, coordinator, snapshotKey, -1L);
    }

    public SourceTask(SourceOperator<?> sourceOperator, List<Output> outputs, RuntimeContext ctx,
                      CheckpointCoordinator coordinator, String snapshotKey, long restoreOffset) {
        this.sourceOperator = sourceOperator;
        this.outputs = outputs;
        this.ctx = ctx;
        this.coordinator = coordinator;
        this.snapshotKey = snapshotKey;
        this.restoreOffset = restoreOffset;
    }

    /** coordinator 请求 source 发 barrier（仅置标志；处理在源线程 collect）。 */
    public void requestCheckpoint(long checkpointId) {
        sourceOperator.requestCheckpoint(checkpointId);
    }

    /** 配置源线程 checkpoint 钩子（在 open 之后调；snapshot backend + offset + ack + 发 barrier）。 */
    @SuppressWarnings({"rawtypes", "unchecked"})
    private void configureCheckpointEmitter(OutputCollector out) {
        if (!(sourceOperator instanceof SourceOperatorImpl impl) || coordinator == null) {
            return;
        }
        impl.setCheckpointEmitter((id, offset) -> {
            StateSnapshot keyed = ctx.getStateBackend().snapshot();
            SubtaskSnapshot snap = new SubtaskSnapshot(keyed, offset, Map.of());
            coordinator.ack(snapshotKey, id, snap);
            Barrier barrier = new Barrier(id);
            for (Output o : outputs) {
                o.sendBarrier(barrier);
            }
        });
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Override
    public void run() {
        OutputCollector out = new OutputCollector(outputs, ctx);
        try {
            sourceOperator.open((Collector) out, ctx);
            // 恢复：open 先建 SourceContextImpl，restoreOffset 设 skipUntil；之后 run 重放时跳过前 offset 条。
            // 顺序：open（建 ctx）→ restoreOffset（设 skipUntil）→ configureCheckpointEmitter → run。
            if (restoreOffset >= 0) {
                sourceOperator.restoreOffset(restoreOffset);
            }
            configureCheckpointEmitter(out);
            sourceOperator.run();
            // 末尾 drain：若最后一轮 checkpoint 请求在最后一条之后到达，补一次（offset=已全部 emitted）
            if (sourceOperator instanceof SourceOperatorImpl impl && coordinator != null) {
                impl.getSourceContext().drainPending();
            }
            broadcastWatermark(outputs, new Watermark(Long.MAX_VALUE));  // +∞：触发所有剩余窗口
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
