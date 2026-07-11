package org.miniflink.runtime;

/** 事件时间水位线：表示 event time 进度。随数据流传播，推进下游算子时钟。 */
public final class Watermark implements StreamElement {
    private final long timestamp;

    public Watermark(long timestamp) {
        this.timestamp = timestamp;
    }

    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public String toString() {
        return "Watermark(" + timestamp + ")";
    }
}
