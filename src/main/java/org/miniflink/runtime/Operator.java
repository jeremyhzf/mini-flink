package org.miniflink.runtime;

/** 处理算子接口：接收一条输入，处理后向下游输出。 */
public interface Operator<IN, OUT> {
    void open(Collector<OUT> out);
    void processElement(IN record) throws Exception;
    void close();
}
