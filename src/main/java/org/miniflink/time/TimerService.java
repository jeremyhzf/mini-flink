package org.miniflink.time;

/** event time 定时器服务：算子注册定时器，watermark 推进时触发。 */
public interface TimerService {
    long currentWatermark();
    void registerEventTimeTimer(long time);
    void deleteEventTimeTimer(long time);
}
