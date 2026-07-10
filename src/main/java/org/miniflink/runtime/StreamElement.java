package org.miniflink.runtime;

/** 通道里流动的统一元素。阶段②只有 Record 与 EndOfBroadcast；阶段④⑤可加 Watermark/Barrier 实现。 */
public interface StreamElement {
}
