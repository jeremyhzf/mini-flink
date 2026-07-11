package org.miniflink.runtime;

import java.util.List;

/** source 执行单元：open source（注入 RuntimeContext）→ run → 正常结束广播 EOB。 */
public class SourceTask implements Task {
    private final SourceOperator<?> sourceOperator;
    private final List<Output> outputs;
    private final RuntimeContext ctx;
    private final CheckpointCoordinator coordinator;   // Phase 1 为 null（占位）；Task 10 注入后 triggerCheckpoint 生效
    private final String snapshotKey;                   // checkpoint 用

    public SourceTask(SourceOperator<?> sourceOperator, List<Output> outputs, RuntimeContext ctx) {
        this(sourceOperator, outputs, ctx, null, null);
    }

    public SourceTask(SourceOperator<?> sourceOperator, List<Output> outputs, RuntimeContext ctx,
                      CheckpointCoordinator coordinator, String snapshotKey) {
        this.sourceOperator = sourceOperator;
        this.outputs = outputs;
        this.ctx = ctx;
        this.coordinator = coordinator;
        this.snapshotKey = snapshotKey;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Override
    public void run() {
        OutputCollector out = new OutputCollector(outputs, ctx);
        try {
            sourceOperator.open((Collector) out, ctx);
            sourceOperator.run();
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
