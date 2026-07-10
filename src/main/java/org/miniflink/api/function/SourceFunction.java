package org.miniflink.api.function;

import org.miniflink.runtime.SourceContext;

/** 数据源：通过 SourceContext 向链路发出数据。 */
public interface SourceFunction<T> {
    void run(SourceContext<T> ctx) throws Exception;
}
