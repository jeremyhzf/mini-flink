package org.miniflink.runtime;

import java.util.Map;

/** MapState 句柄：操作 backend 中当前 key 的 Map。 */
public class MapStateImpl<K, V> implements MapState<K, V> {
    private final MemoryStateBackend backend;
    private final String name;

    public MapStateImpl(MemoryStateBackend backend, String name) {
        this.backend = backend;
        this.name = name;
    }

    @Override
    @SuppressWarnings("unchecked")
    public V get(K key) {
        Map<Object, Object> m = backend.getOrCreateMap(name, backend.currentKey());
        return (V) m.get(key);
    }

    @Override
    public void put(K key, V value) {
        backend.getOrCreateMap(name, backend.currentKey()).put(key, value);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Iterable<Map.Entry<K, V>> entries() {
        Map<Object, Object> m = backend.getOrCreateMap(name, backend.currentKey());
        return (Iterable<Map.Entry<K, V>>) (Iterable<?>) m.entrySet();
    }

    @Override
    public void clear() {
        backend.getOrCreateMap(name, backend.currentKey()).clear();
    }
}
