package org.miniflink.runtime;

import org.junit.jupiter.api.Test;
import org.miniflink.connector.CollectionSource;
import org.miniflink.runtime.operator.SourceOperatorImpl;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SourceOffsetTest {

    /** 恢复时根据 offset 跳过已发记录，只重放 offset 之后的元素。 */
    @Test
    void restoreSkipsAlreadyEmittedRecordsByOffset() throws Exception {
        // source: [1,2,3,4]，subtask 0/parallelism 1 → 全发
        SourceOperatorImpl<Integer> op = new SourceOperatorImpl<>(new CollectionSource<>(List.of(1, 2, 3, 4)));
        ListCollector<Integer> out = new ListCollector<>();
        op.open(out, new RuntimeContextImpl(0, 1, null));
        op.run();
        assertEquals(List.of(1, 2, 3, 4), out.getResult());

        // 全发后 snapshotOffset 返回已发条数（4）
        long offset = op.snapshotOffset();
        assertEquals(4L, offset);

        // 模拟 checkpoint 发生在 offset=2（中途），新实例 restoreOffset(2) 后重放同一 source，跳过前 2 条
        SourceOperatorImpl<Integer> recovered = new SourceOperatorImpl<>(new CollectionSource<>(List.of(1, 2, 3, 4)));
        ListCollector<Integer> out2 = new ListCollector<>();
        recovered.open(out2, new RuntimeContextImpl(0, 1, null));
        recovered.restoreOffset(2L);   // 跳过前 2 条
        recovered.run();
        assertEquals(List.of(3, 4), out2.getResult(), "恢复后应只发 offset 之后的记录");
    }
}
