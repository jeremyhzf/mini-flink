package org.miniflink.time;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class WatermarkStrategyTest {

    /** boundedOutOfOrderness 生成的 watermark 等于 maxTs 减去乱序容忍度。 */
    @Test
    void boundedOutOfOrdernessWatermarkEqualsMaxTsMinusAllowedLateness() {
        WatermarkStrategy<String> strategy = WatermarkStrategy
                .<String>forBoundedOutOfOrderness(java.time.Duration.ofMillis(100), s -> Long.parseLong(s));
        assertEquals(Long.MIN_VALUE, strategy.currentWatermark());   // 初始

        strategy.extractTimestamp("1000");   // maxTs=1000
        assertEquals(900, strategy.currentWatermark());              // 1000-100

        strategy.extractTimestamp("500");    // 乱序到达，maxTs 不变
        assertEquals(900, strategy.currentWatermark());              // 仍 900（maxTs 单调）

        strategy.extractTimestamp("3000");   // maxTs=3000
        assertEquals(2900, strategy.currentWatermark());             // 3000-100
    }

    /** BoundedOutOfOrdernessWatermarks.copy() 返回状态隔离的独立实例。 */
    @Test
    void boundedOutOfOrdernessCopyReturnsIsolatedInstance() {
        BoundedOutOfOrdernessWatermarks<String> s1 = new BoundedOutOfOrdernessWatermarks<>(100, s -> Long.parseLong(s));
        s1.extractTimestamp("1000");   // s1.maxTimestamp=1000
        BoundedOutOfOrdernessWatermarks<String> s2 = s1.copy();
        assertNotSame(s1, s2);
        assertEquals(900, s1.currentWatermark());    // s1 仍 900
        // s2 是新实例，maxTimestamp 重置
        assertEquals(Long.MIN_VALUE, s2.currentWatermark());   // s2 未消费数据
        s2.extractTimestamp("500");
        assertEquals(400, s2.currentWatermark());    // s2 独立：500-100
        assertEquals(900, s1.currentWatermark());    // s1 不受 s2 影响
    }
}
