package org.miniflink;

import org.junit.jupiter.api.Test;
import org.miniflink.connector.CollectionSource;
import org.miniflink.connector.CollectSink;
import org.miniflink.graph.SourceTransformation;
import org.miniflink.graph.StreamGraph;
import org.miniflink.runtime.operator.SourceOperatorImpl;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertThrows;

class ListEncapsulationTest {

    @Test
    void StreamGraph的getter返回不可变视图() {
        StreamGraph sg = new StreamGraph();
        sg.addTransformation(new SourceTransformation<>(1, "s",
                new SourceOperatorImpl<>(new CollectionSource<>(List.of(1)))));
        assertThrows(UnsupportedOperationException.class, () -> sg.getTransformations().add(null));
        assertThrows(UnsupportedOperationException.class, () -> sg.getSinks().add(null));
    }

    @Test
    void CollectSink的getResults返回不可变视图() {
        CollectSink<Integer> sink = new CollectSink<>();
        sink.add(1);
        assertThrows(UnsupportedOperationException.class, () -> sink.getResults().add(2));
    }
}
