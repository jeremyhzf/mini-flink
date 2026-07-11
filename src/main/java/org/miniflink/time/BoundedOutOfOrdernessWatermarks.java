package org.miniflink.time;

/** 固定乱序容忍实现：维护 maxTimestamp，currentWatermark = maxTimestamp - maxOutOfOrderness（单调）。 */
public class BoundedOutOfOrdernessWatermarks<T> implements WatermarkStrategy<T> {
    private final long maxOutOfOrderness;
    private final TimestampAssigner<T> assigner;
    private long maxTimestamp = Long.MIN_VALUE;

    public BoundedOutOfOrdernessWatermarks(long maxOutOfOrderness, TimestampAssigner<T> assigner) {
        this.maxOutOfOrderness = maxOutOfOrderness;
        this.assigner = assigner;
    }

    @Override
    public long extractTimestamp(T record) {
        long ts = assigner.extractTimestamp(record);
        if (ts > maxTimestamp) {
            maxTimestamp = ts;   // 单调：只增不减
        }
        return ts;
    }

    @Override
    public long currentWatermark() {
        // maxTimestamp - maxOutOfOrderness；maxTimestamp 为 MIN 时（无数据）返回 MIN
        return (maxTimestamp == Long.MIN_VALUE) ? Long.MIN_VALUE : maxTimestamp - maxOutOfOrderness;
    }
}
