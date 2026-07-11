package org.miniflink.examples;

import org.miniflink.api.StreamExecutionEnvironment;
import org.miniflink.api.function.KeySelector;
import org.miniflink.api.function.ReduceFunction;
import org.miniflink.api.function.SinkFunction;
import org.miniflink.api.function.SourceFunction;
import org.miniflink.runtime.SourceContext;

/**
 * 自定义 Source + 自定义 Sink 案例（可独立运行）。
 *
 * <p>运行方式：
 * <pre>
 *   mvn -q compile && java -cp target/classes org.miniflink.examples.CustomSourceSinkExample
 * </pre>
 *
 * <p>演示如何实现两个用户扩展点：
 * <ul>
 *   <li><b>自定义 Source</b>（{@link SensorSource}）：实现 {@link SourceFunction}，
 *       在 {@code run(SourceContext)} 里用 {@code ctx.collect(...)} 向链路发数据。
 *       本例模拟温度传感器，生成一批 {@link SensorReading} 读数。</li>
 *   <li><b>自定义 Sink</b>（{@link AlertSink}）：实现 {@link SinkFunction}，
 *       每条到达的元素经 {@code invoke(T)} 消费。本例按温度阈值打印「正常/告警」。</li>
 * </ul>
 *
 * <p>链路：SensorSource（自定义源）→ keyBy(sensorId) → reduce（per-sensor 最高温度）→ AlertSink（自定义汇）。
 * reduce 是 running（每输入输出当前最大），故 sink 会持续收到每个传感器的累进最大值——
 * 这正展示了「自定义 sink 持续消费流」。
 */
public class CustomSourceSinkExample {

    /** 传感器读数：(sensorId, temperature)。 */
    public record SensorReading(String sensorId, double temperature) { }

    // ============================ 自定义 Source ============================
    /**
     * 自定义数据源：模拟温度传感器，生成「每个传感器 readingsPerSensor 条」读数。
     * 实现 {@link SourceFunction}，在 {@link #run(SourceContext)} 里通过 {@code ctx.collect} 发数据。
     * 多并行度下按 {@code idx % parallelism == subtaskIndex} 分片（演示 SourceContext 的并行位置）。
     */
    public static final class SensorSource implements SourceFunction<SensorReading> {
        // 每个传感器多少条数据
        private final int readingsPerSensor;

        public SensorSource(int readingsPerSensor) {
            this.readingsPerSensor = readingsPerSensor;
        }

        @Override
        public void run(SourceContext<SensorReading> ctx) {
            int parallelism = ctx.getParallelism();
            int subtask = ctx.getSubtaskIndex();
            String[] sensors = {"sensor-1", "sensor-2", "sensor-3"};
            int idx = 0;
            for (String sensor : sensors) {
                double base = 18.0 + Math.abs(sensor.hashCode() % 8);   // 各传感器基础温度不同
                for (int i = 0; i < readingsPerSensor; i++) {
                    if (idx % parallelism == subtask) {
                        double temp = base + i * 2.0;                   // 模拟逐次升温
                        ctx.collect(new SensorReading(sensor, temp));
                    }
                    idx++;
                }
            }
        }
    }

    // ============================ 自定义 Sink ============================
    /**
     * 自定义输出端：消费到达的 {@link SensorReading}，温度 ≥ threshold 打印「告警」，否则打印「正常」。
     * 实现 {@link SinkFunction}，每条元素经 {@link #invoke(SensorReading)} 处理。
     * （真实场景可替换为写文件/发 Kafka/调 HTTP，本例用控制台打印示意。）
     */
    public static final class AlertSink implements SinkFunction<SensorReading> {
        private final double threshold;

        public AlertSink(double threshold) {
            this.threshold = threshold;
        }

        @Override
        public void invoke(SensorReading value) {
            if (value.temperature() >= threshold) {
                System.out.println("⚠ 告警  " + value.sensorId()
                        + " 温度 " + value.temperature() + " ≥ " + threshold);
            } else {
                System.out.println("  正常  " + value.sensorId()
                        + " 温度 " + value.temperature());
            }
        }
    }

    public static final class SensorReduce implements ReduceFunction<SensorReading> {
        @Override
        public SensorReading reduce(SensorReading a, SensorReading b) {
            // per-sensor 最高温度（running）
            return a.temperature() >= b.temperature() ? a : b;
        }
    }

    // ============================ 作业装配 ============================
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = new StreamExecutionEnvironment();

        env.addSource(new SensorSource(3))                  // 自定义 source
           .keyBy(SensorReading::sensorId)                                  // 按 sensor 分区
           .reduce(new SensorReduce())                                      // per-sensor 最高温度（running）
           .addSink(new AlertSink(26.0));                          // 自定义 sink：≥26 告警

        System.out.println("=== 自定义 Source（SensorSource）→ keyBy → reduce → 自定义 Sink（AlertSink）===");
        env.execute("custom-source-sink");
        System.out.println("=== 完成（reduce 是 running：每传感器最后一条 = 其最高温度）===");
    }
}
