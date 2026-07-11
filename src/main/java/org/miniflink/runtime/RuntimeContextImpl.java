package org.miniflink.runtime;

import org.miniflink.api.function.KeySelector;

/** RuntimeContext 默认实现：内置 MemoryStateBackend，持有 currentKey 与 currentTimestamp。 */
public class RuntimeContextImpl implements RuntimeContext {
    private final int subtaskIndex;
    private final int parallelism;
    private final KeySelector<?, ?> keySelector;
    private final MemoryStateBackend backend = new MemoryStateBackend();
    private long currentTimestamp = Long.MIN_VALUE;   // source 输出初始值；TS&WM 算子覆盖

    public RuntimeContextImpl(int subtaskIndex, int parallelism, KeySelector<?, ?> keySelector) {
        this.subtaskIndex = subtaskIndex;
        this.parallelism = parallelism;
        this.keySelector = keySelector;
    }

    @Override public int getSubtaskIndex() { return subtaskIndex; }
    @Override public int getParallelism() { return parallelism; }
    @Override public Object getCurrentKey() { return backend.currentKey(); }
    @Override public void setCurrentKey(Object key) { backend.setCurrentKey(key); }
    @Override public StateBackend getStateBackend() { return backend; }
    @Override public KeySelector<?, ?> getKeySelector() { return keySelector; }
    @Override public long getCurrentTimestamp() { return currentTimestamp; }
    @Override public void setCurrentTimestamp(long ts) { this.currentTimestamp = ts; }
}
