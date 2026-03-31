package com.example.trading;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.time.Instant;
import java.util.Properties;

/**
 * Minimal Flink job that reads market data from a Kafka topic and emits
 * stub trading signals based on a simple momentum rule.
 */
public class SignalJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10_000L);

        String brokers = getenvOrDefault("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092");
        String marketDataTopic = getenvOrDefault("MARKET_DATA_TOPIC", "market-data");
        String signalsTopic = getenvOrDefault("SIGNALS_TOPIC", "signals");

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(brokers)
                .setTopics(marketDataTopic)
                .setGroupId("signal-job")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> raw = env.fromSource(
                source,
                WatermarkStrategy.noWatermarks(),
                "market-data-source");

        ObjectMapper mapper = new ObjectMapper();

        DataStream<MarketData> ticks = raw
                .map(value -> parseMarketData(mapper, value))
                .returns(TypeInformation.of(new TypeHint<MarketData>() {}));

        DataStream<Signal> signals = ticks
                .keyBy(MarketData::getSymbol)
                .process(new MomentumStrategyProcessFunction(0.001)); // 10 bps

        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers(brokers)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic(signalsTopic)
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build())
                .build();

        signals
                .map(signal -> mapper.writeValueAsString(signal))
                .sinkTo(sink)
                .name("signals-sink");

        env.execute("PaperTrading-SignalJob");
    }

    private static String getenvOrDefault(String key, String defaultValue) {
        String value = System.getenv(key);
        return value != null ? value : defaultValue;
    }

    private static MarketData parseMarketData(ObjectMapper mapper, String json) throws IOException {
        return mapper.readValue(json, MarketData.class);
    }

    // --- Data types ---

    public static class MarketData {
        private String symbol;
        private double price;
        private String timestamp;

        public MarketData() {
        }

        public MarketData(String symbol, double price, String timestamp) {
            this.symbol = symbol;
            this.price = price;
            this.timestamp = timestamp;
        }

        public String getSymbol() {
            return symbol;
        }

        public void setSymbol(String symbol) {
            this.symbol = symbol;
        }

        public double getPrice() {
            return price;
        }

        public void setPrice(double price) {
            this.price = price;
        }

        public String getTimestamp() {
            return timestamp;
        }

        public void setTimestamp(String timestamp) {
            this.timestamp = timestamp;
        }
    }

    public static class Signal {
        private String symbol;
        private String side;
        private double price;
        private int quantity;
        private String timestamp;

        public Signal() {
        }

        public Signal(String symbol, String side, double price, int quantity, String timestamp) {
            this.symbol = symbol;
            this.side = side;
            this.price = price;
            this.quantity = quantity;
            this.timestamp = timestamp;
        }

        public String getSymbol() {
            return symbol;
        }

        public void setSymbol(String symbol) {
            this.symbol = symbol;
        }

        public String getSide() {
            return side;
        }

        public void setSide(String side) {
            this.side = side;
        }

        public double getPrice() {
            return price;
        }

        public void setPrice(double price) {
            this.price = price;
        }

        public int getQuantity() {
            return quantity;
        }

        public void setQuantity(int quantity) {
            this.quantity = quantity;
        }

        public String getTimestamp() {
            return timestamp;
        }

        public void setTimestamp(String timestamp) {
            this.timestamp = timestamp;
        }
    }

    // --- Strategy ---

    public static class MomentumStrategyProcessFunction
            extends KeyedProcessFunction<String, MarketData, Signal> {

        private final double threshold; // fractional change, e.g. 0.001 = 10 bps
        private transient ValueState<Double> lastPrice;

        public MomentumStrategyProcessFunction(double threshold) {
            this.threshold = threshold;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<Double> descriptor = new ValueStateDescriptor<>(
                    "lastPrice", TypeInformation.of(Double.class));
            lastPrice = getRuntimeContext().getState(descriptor);
        }

        @Override
        public void processElement(
                MarketData value,
                Context ctx,
                Collector<Signal> out) throws Exception {
            Double prev = lastPrice.value();
            if (prev == null || prev <= 0) {
                lastPrice.update(value.getPrice());
                return;
            }

            double change = (value.getPrice() - prev) / prev;
            String side = null;

            if (change > threshold) {
                side = "BUY";
            } else if (change < -threshold) {
                side = "SELL";
            }

            lastPrice.update(value.getPrice());

            if (side != null) {
                Signal signal = new Signal(
                        value.getSymbol(),
                        side,
                        value.getPrice(),
                        10,
                        Instant.now().toString());
                out.collect(signal);
            }
        }
    }
}
