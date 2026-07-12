package org.miniflink.state;

import java.util.List;

/** ListState 句柄：操作 backend 中当前 key 的 List。 */
public class ListStateImpl<T> implements ListState<T> {
    private final MemoryStateBackend backend;
    private final String name;

    public ListStateImpl(MemoryStateBackend backend, String name) {
        this.backend = backend;
        this.name = name;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Iterable<T> get() {
        List<Object> list = (List<Object>) backend.getOrCreateList(name, backend.currentKey());
        return (Iterable<T>) list;
    }

    @Override
    public void add(T v) {
        backend.getOrCreateList(name, backend.currentKey()).add(v);
    }

    @Override
    public void clear() {
        backend.getOrCreateList(name, backend.currentKey()).clear();
    }
}
