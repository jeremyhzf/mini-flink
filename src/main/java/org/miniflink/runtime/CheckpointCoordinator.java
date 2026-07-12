package org.miniflink.runtime;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicLong;
import org.miniflink.checkpoint.Checkpoint;
import org.miniflink.checkpoint.SubtaskSnapshot;

/**
 * checkpoint 协调器：daemon 线程按 interval 周期触发（向 source 置标志）；各 subtask ack 汇聚；
 * 收齐 snapshotKeys 数 → 完成一个 Checkpoint（retained 最近 retainedCount 个）。
 */
public class CheckpointCoordinator {
    private final long intervalMillis;
    private final List<SourceTask> sources;
    private final List<String> snapshotKeys;     // 全部 subtask 的 key（含 source + operator）
    private final int retainedCount;

    private final AtomicLong idCounter = new AtomicLong(0);
    private final Deque<Checkpoint> completed = new ConcurrentLinkedDeque<>();
    private final Object inflightLock = new Object();
    private long currentId = -1;
    private final Map<String, SubtaskSnapshot> pendingAcks = new HashMap<>();

    private volatile boolean running = false;
    private Thread daemon;

    public CheckpointCoordinator(long intervalMillis, List<SourceTask> sources,
                                 List<String> snapshotKeys, int retainedCount) {
        this.intervalMillis = intervalMillis;
        this.sources = sources;
        this.snapshotKeys = snapshotKeys;
        this.retainedCount = retainedCount;
    }

    public void start() {
        if (intervalMillis == Long.MAX_VALUE) {
            return;   // 未启用 checkpoint（测试/单次）
        }
        running = true;
        daemon = new Thread(this::loop, "miniflink-checkpoint");
        daemon.setDaemon(true);
        daemon.start();
    }

    private void loop() {
        while (running) {
            triggerOnce();
            try {
                Thread.sleep(intervalMillis);
            } catch (InterruptedException e) {
                return;
            }
        }
    }

    /** 触发一轮：新 id → 请求所有 source 在线程发 barrier（并快照自身）。 */
    private void triggerOnce() {
        synchronized (inflightLock) {
            currentId = idCounter.incrementAndGet();
            pendingAcks.clear();
        }
        for (SourceTask s : sources) {
            s.requestCheckpoint(currentId);
        }
    }

    /** subtask 完成快照后 ack；收齐则汇聚成 Checkpoint。 */
    public void ack(String snapshotKey, long checkpointId, SubtaskSnapshot snapshot) {
        synchronized (inflightLock) {
            if (currentId == -1) {
                // 无进行中轮次：本 ack 开启新轮次（daemon 模式下 triggerOnce 已先行置好 currentId；
                // 此分支仅用于 interval=Long.MAX_VALUE 时测试/手动直接 ack，以及轮次间隙的首个 ack）。
                currentId = checkpointId;
                pendingAcks.clear();
            } else if (checkpointId != currentId) {
                return;   // 过期/废弃轮次（不匹配进行中的 id）
            }
            pendingAcks.put(snapshotKey, snapshot);
            if (pendingAcks.size() == snapshotKeys.size()) {
                completed.addLast(new Checkpoint(currentId, new HashMap<>(pendingAcks)));
                while (completed.size() > retainedCount) {
                    completed.pollFirst();
                }
                currentId = -1;
            }
        }
    }

    public void stop() {
        running = false;
        if (daemon != null) {
            daemon.interrupt();
        }
    }

    public Checkpoint lastCompletedCheckpoint() {
        return completed.peekLast();
    }

    public long getIntervalMillis() { return intervalMillis; }
    public List<SourceTask> getSources() { return sources; }
    public List<String> getSnapshotKeys() { return snapshotKeys; }
}
