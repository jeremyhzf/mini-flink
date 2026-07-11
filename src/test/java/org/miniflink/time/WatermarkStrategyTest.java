package org.miniflink.time;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class WatermarkStrategyTest {

    @Test
    void boundedOutOfOrderness生成watermark等于maxTs减乱序容忍() {
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
}
