package org.miniflink.runtime;

/** source 算子接口：open 注入输出与并行位置，run 产生数据，close 释放资源。 */
public interface SourceOperator<OUT> {
    void open(Collector<OUT> out, int subtaskIndex, int parallelism);
    void run() throws Exception;
    void close();
}
