package org.miniflink.runtime;

/** ValueState 句柄：经 backend.currentKey() 寻址 backend 的 per-key 存储。 */
public class ValueStateImpl<T> implements ValueState<T> {
    private final MemoryStateBackend backend;
    private final String name;

    public ValueStateImpl(MemoryStateBackend backend, String name) {
        this.backend = backend;
        this.name = name;
    }

    @Override
    public T value() {
        Object key = backend.currentKey();
        return (T) backend.getValue(name, key);
    }

    @Override
    public void update(T v) {
        backend.putValue(name, backend.currentKey(), v);
    }
}
