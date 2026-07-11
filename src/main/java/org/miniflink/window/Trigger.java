package org.miniflink.window;

/** 窗口触发器：决定窗口何时触发计算。 */
public abstract class Trigger<T, W extends Window> {
    public abstract TriggerResult onElement(T element, long timestamp, W window, TriggerContext ctx) throws Exception;
    public abstract TriggerResult onEventTime(long time, W window, TriggerContext ctx) throws Exception;
}
