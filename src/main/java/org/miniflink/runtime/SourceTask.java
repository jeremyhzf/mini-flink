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
