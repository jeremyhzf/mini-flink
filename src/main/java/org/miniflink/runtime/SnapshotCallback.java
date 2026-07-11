package org.miniflink.runtime;

/** InputGate 全部 channel 对齐某 barrier 时回调（task 在此做快照）。 */
@FunctionalInterface
public interface SnapshotCallback {
    void onAligned(long checkpointId) throws Exception;
}
