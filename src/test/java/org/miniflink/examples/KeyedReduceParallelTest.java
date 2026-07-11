package org.miniflink.examples;

import org.junit.jupiter.api.Test;
import org.miniflink.api.StreamExecutionEnvironment;
import org.miniflink.api.function.KeySelector;
import org.miniflink.api.function.ReduceFunction;
import org.miniflink.connector.CollectSink;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * 阶段③核心并发属性回归守护（final review I1）：
 * reduce 算子在 parallelism=2 下经 HashPartitioner 在 2 个 subtask 上并发执行，
 * 同 key 恒落同一 subtask，per-key state 不分裂、累加完整正确。
 *
 * 拓扑：source(p=1) → map(p=1, forward) → keyBy(word) → reduce(p=2, hash) → sink(p=1)。
 * reduce 入边为 HashPartitioner：numDownstream=2 时 floorMod(word.hashCode(), 2)——
 *   "a"(97)→subtask1, "b"(98)→subtask0, "c"(99)→subtask1, "d"(100)→subtask0。
 * 故 subtask0 处理 {b,d}、subtask1 处理 {a,c}，两个 subtask 均活跃且各自承载多个 key，
 * 既验证「同 key 不跨 subtask 分裂」，又验证「同一 subtask 内多 key state 互不踩踏」。
 */
class KeyedReduceParallelTest {

    record WC(String word, int count) { }

    /** 验证多 subtask 并发下同一 key 的累加不分裂，per-key state 完整正确。 */
    @Test
    void sameKeyAccumulatesWithoutSplittingUnderMultiSubtaskConcurrency() throws Exception {
        StreamExecutionEnvironment env = new StreamExecutionEnvironment();
        CollectSink<WC> sink = new CollectSink<>();

        // 交错数据：a×3, b×2, c×4, d×1（共 10 条，按 HashPartitioner 散到 2 个 reduce subtask）
        env.fromCollection(List.of("a", "b", "c", "a", "d", "c", "a", "b", "c", "c"))
           .map(w -> new WC(w, 1))
           .keyBy((KeySelector<WC, String>) wc -> wc.word)
           .reduce((ReduceFunction<WC>) (a, b) -> new WC(a.word, a.count + b.count))
           .setParallelism(2)   // reduce 算子跑在 2 个 subtask 上（核心受测点）
           .addSink(sink::add);

        env.execute("keyed-reduce-parallel");

        // reduce 输出 running 结果（每输入一条）；按 word 取最大 count（=最终累加值）
        Map<String, Integer> result = new HashMap<>();
        for (WC wc : sink.getResults()) {
            result.merge(wc.word, wc.count, Math::max);
        }
        assertEquals(3, result.get("a"));  // subtask1：3 条全落同 subtask，累加完整
        assertEquals(2, result.get("b"));  // subtask0
        assertEquals(4, result.get("c"));  // subtask1
        assertEquals(1, result.get("d"));  // subtask0
    }
}
