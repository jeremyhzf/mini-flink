package org.miniflink.runtime;

/** source 算子接口：open 设置输出，run 产生数据，close 释放资源。 */
public interface SourceOperator<OUT> {
    void open(Collector<OUT> out);
    void run() throws Exception;
    void close();
}
