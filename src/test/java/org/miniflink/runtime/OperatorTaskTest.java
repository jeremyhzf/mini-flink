package org.miniflink.runtime;

import org.junit.jupiter.api.Test;
import org.miniflink.api.function.MapFunction;
import org.miniflink.execution.ForwardPartitioner;
import org.miniflink.runtime.InputChannel;
import org.miniflink.runtime.operator.MapOperator;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class OperatorTaskTest {

    /** OperatorTask 处理上游数据并在所有上游 EOB 后向下游广播。 */
    @Test
    void processesDataAndBroadcastsDownstreamAfterAllUpstreamEob() throws Exception {
        Channel input = new Channel(8);
        input.send(new Record<>(1, 0L));
        input.send(new Record<>(2, 0L));
        input.send(EndOfBroadcast.INSTANCE);

        OperatorChain<Integer, Integer> chain = new OperatorChain<>(List.of(
                new MapOperator<>((MapFunction<Integer, Integer>) x -> x * 10)));
        Channel outCh = new Channel(8);
        Output out = new Output(List.of(outCh), new ForwardPartitioner(), null);

        new OperatorTask(chain, List.of(new InputChannel(input)), 1,
                List.of(out), new RuntimeContextImpl(0, 1, null)).run();

        assertEquals(10, ((Record<Integer>) outCh.receive()).value());
        assertEquals(20, ((Record<Integer>) outCh.receive()).value());
        assertInstanceOf(EndOfBroadcast.class, outCh.receive());
    }

    /** 多上游时需等所有上游 EOB 到齐才退出，期间数据不丢失。 */
    @Test
    void waitsForAllUpstreamEobBeforeExitWithoutDataLoss() throws Exception {
        Channel input = new Channel(8);
        input.send(new Record<>(1, 0L));
        input.send(EndOfBroadcast.INSTANCE);   // 上游 1 的 EOB
        input.send(new Record<>(2, 0L));
        input.send(EndOfBroadcast.INSTANCE);   // 上游 2 的 EOB

        OperatorChain<Integer, Integer> chain = new OperatorChain<>(List.of(
                new MapOperator<>((MapFunction<Integer, Integer>) x -> x)));
        Channel outCh = new Channel(8);
        Output out = new Output(List.of(outCh), new ForwardPartitioner(), null);

        new OperatorTask(chain, List.of(new InputChannel(input)), 2,
                List.of(out), new RuntimeContextImpl(0, 1, null)).run(); // pendingUpstreams=2

        // 收到第 1 个 EOB 不退出，继续处理 2，收到第 2 个 EOB 才广播
        assertEquals(1, ((Record<Integer>) outCh.receive()).value());
        assertEquals(2, ((Record<Integer>) outCh.receive()).value());
        assertInstanceOf(EndOfBroadcast.class, outCh.receive());
    }
}
