package org.miniflink.runtime;

/** checkpoint 屏障：随数据流流动，触发下游对齐与快照。携带 checkpointId 区分不同轮次。 */
public final class Barrier implements StreamElement {
    private final long checkpointId;

    public Barrier(long checkpointId) {
        this.checkpointId = checkpointId;
    }

    public long getCheckpointId() {
        return checkpointId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Barrier that)) {
            return false;
        }
        return checkpointId == that.checkpointId;
    }

    @Override
    public int hashCode() {
        return Long.hashCode(checkpointId);
    }

    @Override
    public String toString() {
        return "Barrier(" + checkpointId + ")";
    }
}
