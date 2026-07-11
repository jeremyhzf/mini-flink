package org.miniflink.runtime;

import java.util.List;

/**
 * checkpoint 协调器骨架。Task 10 实现周期触发与 ack 汇聚；本任务仅提供 ack 接口供 OperatorTask/SourceTask 调用。
 * Task 10 会替换 start/stop/ack 为完整实现。
 */
public class CheckpointCoordinator {
    private final long intervalMillis;
    private final List<SourceTask> sources;
    private final List<String> snapshotKeys;
    private final int retainedCount;

    public CheckpointCoordinator(long intervalMillis, List<SourceTask> sources,
                                 List<String> snapshotKeys, int retainedCount) {
        this.intervalMillis = intervalMillis;
        this.sources = sources;
        this.snapshotKeys = snapshotKeys;
        this.retainedCount = retainedCount;
    }

    public void start() { /* Task 10 */ }
    public void stop() { /* Task 10 */ }
    public void ack(String snapshotKey, long checkpointId, SubtaskSnapshot snapshot) { /* Task 10 */ }
    public Checkpoint lastCompletedCheckpoint() { return null; /* Task 10 */ }

    public long getIntervalMillis() { return intervalMillis; }
    public List<SourceTask> getSources() { return sources; }
    public List<String> getSnapshotKeys() { return snapshotKeys; }
}
