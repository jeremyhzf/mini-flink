package org.miniflink.window;

/** 触发结果：CONTINUE（继续等待）、FIRE_AND_PURGE（触发计算并清理窗口状态）。 */
public enum TriggerResult {
    CONTINUE,
    FIRE_AND_PURGE
}
