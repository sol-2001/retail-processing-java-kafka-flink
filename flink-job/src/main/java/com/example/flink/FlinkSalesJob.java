package com.example.flink;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.*;

public class FlinkSalesJob {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(10_000, CheckpointingMode.EXACTLY_ONCE);

        // Коннектор чтения из Kafka:
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("preprocessed_sales")
                .setGroupId("flink-sales-group")  // Группа для чтения
                .setProperty("transaction.timeout.ms", "900000")
                .setValueOnlyDeserializer(new org.apache.flink.api.common.serialization.SimpleStringSchema())
                .build();

        // Считываем сырые строки
        DataStream<String> rawStream = env.fromSource(
                source,
                WatermarkStrategy.noWatermarks(),
                "KafkaSource-preprocessed-sales"
        );

        // Преобразуем JSON в SalesData
        DataStream<SalesData> salesStream = rawStream
                .map(new ParseSalesDataFunction())
                .filter(Objects::nonNull);

        // Окно: считаем (count, avgCheck)
        DataStream<StatsResult> stats1m = salesStream
                .keyBy(s -> "global")
                .window(TumblingProcessingTimeWindows.of(Time.minutes(1)))
                .apply(new StatsWindowFunction("1min"));

        // Записываем статистику в топик "sales_stats_1m"
        KafkaSink<StatsResult> sinkStats1m = KafkaSink.<StatsResult>builder()
                .setBootstrapServers("localhost:9092")
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic("sales_stats_1m")
                                .setValueSerializationSchema(new StatsResultSerializationSchema())
                                .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setTransactionalIdPrefix("sales-stats-1m-")
                .setProperty("transaction.timeout.ms", "900000")
                .build();

        stats1m.sinkTo(sinkStats1m)
                .name("Sink-sales_stats_1m");

        stats1m.addSink(new MongoSink(
                "mongodb://localhost:27017",
                "flink_db",
                "stats_1m"
        )).name("MongoSink-sales_stats_1m");


        // Сравниваем текущее значение со средним за предыдущие 15 минут
        DataStream<String> alerts = stats1m
                .keyBy(r -> "global")
                .process(new CompareWithHistoryFunction(15));

        // Записываем алёрты в "sales_alerts_1m"
        KafkaSink<String> sinkAlerts1m = KafkaSink.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic("sales_alerts_1m")
                                .setValueSerializationSchema(new org.apache.flink.api.common.serialization.SimpleStringSchema())
                                .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setTransactionalIdPrefix("sales-alerts-1m-")
                .setProperty("transaction.timeout.ms", "900000")
                .build();

        alerts.sinkTo(sinkAlerts1m)
                .name("Sink-sales_alerts_1m");

        // Запускаем job
        env.execute("Flink Sales Statistics Job (1min + alerts)");
    }

    // парсер JSON в SalesData
    public static class ParseSalesDataFunction implements MapFunction<String, SalesData> {
        private final ObjectMapper mapper = new ObjectMapper();

        @Override
        public SalesData map(String rawJson) {
            try {

                Map<String, List<SalesData>> payload =
                        mapper.readValue(rawJson, new TypeReference<>() {});
                List<SalesData> list = payload.get("sales_info");
                if (list != null && !list.isEmpty()) {
                    return list.get(0);
                } else {
                    return null;
                }
            } catch (Exception e) {
                System.err.println("Parse error: " + e.getMessage() + " | raw: " + rawJson);
                return null;
            }
        }
    }

    // считаем count и avg
    public static class StatsWindowFunction implements WindowFunction<
            SalesData,
            StatsResult,
            String,
            TimeWindow> {

        private final String windowType;

        public StatsWindowFunction(String windowType) {
            this.windowType = windowType;
        }

        @Override
        public void apply(String key,
                          TimeWindow window,
                          Iterable<SalesData> input,
                          Collector<StatsResult> out) {

            int count = 0;
            double sum = 0.0;
            for (SalesData s : input) {
                count++;
                sum += s.getPrice();
            }
            double avg = (count > 0) ? (sum / count) : 0.0;

            StatsResult result = new StatsResult();
            result.setWindowType(windowType);
            result.setCount(count);
            result.setAvgCheck(avg);

            out.collect(result);
        }
    }

    // храним историю за полсдение минуты и сравниваем с предыдущими значения
    public static class CompareWithHistoryFunction extends KeyedProcessFunction<String, StatsResult, String> {

        private final int maxHistorySize;

        private ListState<StatsResult> historyState;

        public CompareWithHistoryFunction(int maxHistorySize) {
            this.maxHistorySize = maxHistorySize;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            ListStateDescriptor<StatsResult> desc =
                    new ListStateDescriptor<>("statsHistory", StatsResult.class);
            historyState = getRuntimeContext().getListState(desc);
        }

        @Override
        public void processElement(StatsResult current,
                                   KeyedProcessFunction<String, StatsResult, String>.Context ctx,
                                   Collector<String> out) throws Exception {

            List<StatsResult> oldList = new ArrayList<>();
            for (StatsResult s : historyState.get()) {
                oldList.add(s);
            }

            // Считаем средние по истории (count, avgCheck)
            double oldCountAvg = 0.0;
            double oldAvgCheckAvg = 0.0;

            if (!oldList.isEmpty()) {
                int sumCount = 0;
                double sumAvgCheck = 0.0;

                for (StatsResult s : oldList) {
                    sumCount += s.getCount();
                    sumAvgCheck += s.getAvgCheck();
                }
                oldCountAvg = (double) sumCount / oldList.size();
                oldAvgCheckAvg = sumAvgCheck / oldList.size();
            }

            // Сравниваем текущее с историческим средним
            boolean countExceeded = current.getCount() > oldCountAvg;
            boolean avgCheckExceeded = current.getAvgCheck() > oldAvgCheckAvg;

            if (countExceeded || avgCheckExceeded) {
                // Генерируем alert
                String alert = String.format(
                        "ALERT! window=%s | count=%d (oldAvg=%.2f) %s | avgCheck=%.2f (oldAvg=%.2f) %s",
                        current.getWindowType(),
                        current.getCount(), oldCountAvg, (countExceeded ? "EXCEEDED" : "OK"),
                        current.getAvgCheck(), oldAvgCheckAvg, (avgCheckExceeded ? "EXCEEDED" : "OK")
                );
                out.collect(alert);
            }

            oldList.add(current);

            while (oldList.size() > maxHistorySize) {
                oldList.remove(0);
            }

            historyState.update(oldList);
        }
    }
}
