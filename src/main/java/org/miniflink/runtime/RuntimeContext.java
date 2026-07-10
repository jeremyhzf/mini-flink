package org.miniflink.runtime;

import org.miniflink.api.function.KeySelector;

/** 算子运行时上下文（per-subtask）：承载并行位置 + keyed state 访问 + currentKey。 */
public interface RuntimeContext {
    int getSubtaskIndex();
    int getParallelism();
    Object getCurrentKey();
    void setCurrentKey(Object key);       // 转发 StateBackend.setCurrentKey
    StateBackend getStateBackend();
    KeySelector<?, ?> getKeySelector();   // keyed 算子非 null，普通算子可 null
}
