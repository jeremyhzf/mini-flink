package org.miniflink.checkpoint;

import java.io.Serializable;
import java.util.Map;
import org.miniflink.state.OperatorState;
import org.miniflink.state.StateSnapshot;

/** 单 subtask 的快照：keyed state + source offset + 算子级状态。 */
public class SubtaskSnapshot implements Serializable {
    private final StateSnapshot keyedState;
    private final long sourceOffset;                          // 仅 source 有意义，其余 -1
    private final Map<Integer, OperatorState> operatorStates;

    public SubtaskSnapshot(StateSnapshot keyedState, long sourceOffset,
                           Map<Integer, OperatorState> operatorStates) {
        this.keyedState = keyedState;
        this.sourceOffset = sourceOffset;
        this.operatorStates = operatorStates;
    }

    public StateSnapshot getKeyedState() { return keyedState; }
    public long getSourceOffset() { return sourceOffset; }
    public Map<Integer, OperatorState> getOperatorStates() { return operatorStates; }
}
