package org.miniflink.execution;

import org.miniflink.api.function.KeySelector;

import java.util.List;

/** 物理边：上游 vertices 组 → 下游 vertices 组，含分区器与 keySelector（hash 用）。 */
public class ExecutionEdge {
    private final List<ExecutionVertex> sources;
    private final List<ExecutionVertex> targets;
    private final Partitioner partitioner;
    private final KeySelector<?, ?> keySelector;

    public ExecutionEdge(List<ExecutionVertex> sources, List<ExecutionVertex> targets,
                         Partitioner partitioner, KeySelector<?, ?> keySelector) {
        this.sources = sources;
        this.targets = targets;
        this.partitioner = partitioner;
        this.keySelector = keySelector;
    }

    public List<ExecutionVertex> getSources() {
        return sources;
    }

    public List<ExecutionVertex> getTargets() {
        return targets;
    }

    public Partitioner getPartitioner() {
        return partitioner;
    }

    public KeySelector<?, ?> getKeySelector() {
        return keySelector;
    }
}
