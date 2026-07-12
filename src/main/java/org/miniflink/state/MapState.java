package org.miniflink.state;
import java.util.Map;
/** 映射状态（绑定当前 key）。 */
public interface MapState<K, V> {
    V get(K key);
    void put(K key, V value);
    Iterable<Map.Entry<K, V>> entries();
    void clear();
}
