package org.miniflink.graph;

import java.util.ArrayList;
import java.util.List;

/** 逻辑图：收集所有 transformation 与 sink 节点。 */
public class StreamGraph {
    private final List<Transformation<?>> transformations = new ArrayList<>();
    private final List<Transformation<?>> sinks = new ArrayList<>();

    public void addTransformation(Transformation<?> t) {
        transformations.add(t);
    }

    public void addSink(Transformation<?> sink) {
        sinks.add(sink);
    }

    public List<Transformation<?>> getTransformations() {
        return transformations;
    }

    public List<Transformation<?>> getSinks() {
        return sinks;
    }
}
