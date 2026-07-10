package org.miniflink.runtime.operator;

import org.miniflink.api.function.FilterFunction;
import org.miniflink.runtime.Collector;
import org.miniflink.runtime.Operator;

/** 包装 FilterFunction 的算子：仅转发满足条件的元素。 */
public class FilterOperator<IN> implements Operator<IN, IN> {
    private final FilterFunction<IN> filterFunction;
    private Collector<IN> out;

    public FilterOperator(FilterFunction<IN> filterFunction) {
        this.filterFunction = filterFunction;
    }

    @Override
    public void open(Collector<IN> out) {
        this.out = out;
    }

    @Override
    public void processElement(IN record) throws Exception {
        if (filterFunction.filter(record)) {
            out.collect(record);
        }
    }

    @Override
    public void close() {
        // 阶段①无需操作
    }
}
