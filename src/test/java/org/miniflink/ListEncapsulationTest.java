package org.miniflink;

import org.junit.jupiter.api.Test;
import org.miniflink.connector.CollectionSource;
import org.miniflink.connector.CollectSink;
import org.miniflink.graph.SourceTransformation;
import org.miniflink.graph.StreamGraph;
import org.miniflink.runtime.operator.SourceOperatorImpl;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ListEncapsulationTest {

    /** StreamGraph 的 getter 返回不可变视图，对 getTransformations/getSinks 调用 add 抛出 UnsupportedOperationException。 */
    @Test
    void streamGraphGettersReturnImmutableView() {
        StreamGraph sg = new StreamGraph();
        sg.addTransformation(new SourceTransformation<>(1, "s",
                new SourceOperatorImpl<>(new CollectionSource<>(List.of(1)))));
        assertThrows(UnsupportedOperationException.class, () -> sg.getTransformations().add(null));
        assertThrows(UnsupportedOperationException.class, () -> sg.getSinks().add(null));
    }

    /** CollectSink.getResults 返回与内部数据隔离的独立可变快照，后续追加与快照改动互不影响。 */
    @Test
    void collectSinkGetResultsReturnsIsolatedSnapshot() {
        CollectSink<Integer> sink = new CollectSink<>();
        sink.add(1);
        List<Integer> snapshot = sink.getResults();
        assertEquals(List.of(1), snapshot);   // 快照含已写入元素
        sink.add(2);                          // 此后追加 2
        assertEquals(List.of(1), snapshot);   // 取过的快照不含 2（与内部隔离）
        // 快照是可变副本：改动不影响内部
        snapshot.add(99);
        assertEquals(List.of(1, 2), sink.getResults());
    }
}
