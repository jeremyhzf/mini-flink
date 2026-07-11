package org.miniflink.runtime;

import java.io.Serializable;

/** 算子级状态标记接口（如 WindowOperator 的 timers + activeWindows）。Serializable 以便随快照持久化。 */
public interface OperatorState extends Serializable {
}
