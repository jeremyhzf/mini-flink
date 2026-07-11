package org.miniflink.window;

/** 窗口抽象（时间区间）。阶段④仅 TimeWindow，为后续 GlobalWindow 等留口。 */
public abstract class Window {
    public abstract long start();
    public abstract long end();
}
