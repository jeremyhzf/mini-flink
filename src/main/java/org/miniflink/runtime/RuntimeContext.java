package org.miniflink.runtime;

import org.miniflink.api.function.KeySelector;

/** 算子运行时上下文（per-subtask）：并行位置 + keyed state + currentKey + 当前记录的事件时间戳。 */
public interface RuntimeContext {
    int getSubtaskIndex();
    int getParallelism();
    Object getCurrentKey();
    void setCurrentKey(Object key);
    StateBackend getStateBackend();
    KeySelector<?, ?> getKeySelector();
    long getCurrentTimestamp();          // 当前记录的事件时间戳（OperatorTask 在 processElement 前设置）
    void setCurrentTimestamp(long ts);
}
