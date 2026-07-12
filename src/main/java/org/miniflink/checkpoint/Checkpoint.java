package org.miniflink.checkpoint;

import java.io.Serializable;
import java.util.Map;

/** 一次 checkpoint：checkpointId + 各 subtask 快照（key = snapshotKey）。 */
public class Checkpoint implements Serializable {
    private final long checkpointId;
    private final Map<String, SubtaskSnapshot> snapshots;

    public Checkpoint(long checkpointId, Map<String, SubtaskSnapshot> snapshots) {
        this.checkpointId = checkpointId;
        this.snapshots = snapshots;
    }

    public long getCheckpointId() { return checkpointId; }
    public Map<String, SubtaskSnapshot> getSnapshots() { return snapshots; }
}
