package com.example.trading;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;

/**
 * Apache Flink Streaming Job for Real-Time Trading Signal Generation.
 */
public class SignalJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10_000L);

        Config config = ConfigFactory.load("application.conf");

        String brokers = config.getString("kafka.bootstrap.servers");
        String marketDataTopic = config.getString("kafka.topics.market-data");
        String signalsTopic = config.getString("kafka.topics.signals");
        double momentumThreshold = config.getDouble("job.momentum-threshold");
        int signalQuantity = config.getInt("job.quantity");

        System.out.println("Loaded configuration:");
        System.out.println("  Kafka Brokers: " + brokers);
        System.out.println("  Market Data Topic: " + marketDataTopic);
        System.out.println("  Signals Topic: " + signalsTopic);
        System.out.println("  Momentum Threshold: " + momentumThreshold);
        System.out.println("  Signal Quantity: " + signalQuantity);

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
                .map(value -> parseMarketData(mapper, value));

        DataStream<Signal> signals = ticks
                .keyBy(MarketData::getSymbol)
                .process(new MomentumStrategyProcessFunction(
                        momentumThreshold, signalQuantity));

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

    private static MarketData parseMarketData(ObjectMapper mapper, String json)
            throws IOException {
        return mapper.readValue(json, MarketData.class);
    }
}
