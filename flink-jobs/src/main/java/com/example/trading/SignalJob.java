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

// Typesafe Config for reading application.conf
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/**
 * Apache Flink Streaming Job for Real-Time Trading Signal Generation
 * 
 * This job consumes market data from Kafka, applies a momentum-based trading strategy,
 * and produces trading signals back to Kafka for downstream processing.
 * 
 * Data Flow:
 * 1. Kafka Source (market-data topic) → 2. JSON Parsing → 3. Momentum Strategy → 4. Kafka Sink (signals topic)
 */
public class SignalJob {

    /**
     * Main entry point for the Flink streaming job
     * 
     * @param args Command line arguments (not used)
     * @throws Exception If job execution fails
     */
    public static void main(String[] args) throws Exception {
        // Create Flink streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // Enable checkpointing every 10 seconds for fault tolerance
        env.enableCheckpointing(10_000L);

        // Load configuration from application.conf file
        Config config = ConfigFactory.load("application.conf");
        
        // Get configuration from application.conf with defaults
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

        // Create Kafka source connector for consuming market data
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(brokers)                    // Kafka broker addresses
                .setTopics(marketDataTopic)                      // Topic to consume from
                .setGroupId("signal-job")                       // Consumer group ID
                .setStartingOffsets(OffsetsInitializer.latest()) // Start from latest offsets
                .setValueOnlyDeserializer(new SimpleStringSchema()) // Deserialize as strings
                .build();

        // Create data stream from Kafka source
        DataStream<String> raw = env.fromSource(
                source,
                WatermarkStrategy.noWatermarks(),    // No watermark strategy for simple demo
                "market-data-source");                // Operator name for monitoring

        // JSON mapper for parsing market data
        ObjectMapper mapper = new ObjectMapper();

        // Parse JSON strings into MarketData objects
        DataStream<MarketData> ticks = raw
                .map(value -> parseMarketData(mapper, value)) // Parse each JSON message
                .returns(TypeInformation.of(new TypeHint<MarketData>() {})); // Type information for Flink

        // Apply trading strategy: group by symbol and process with momentum strategy
        DataStream<Signal> signals = ticks
                .keyBy(MarketData::getSymbol)                            // Partition by stock symbol
                .process(new MomentumStrategyProcessFunction(momentumThreshold, signalQuantity));    // Use configured threshold and quantity

        // Create Kafka sink for producing trading signals
        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers(brokers)                            // Kafka broker addresses
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic(signalsTopic)                  // Target topic for signals
                                .setValueSerializationSchema(new SimpleStringSchema()) // Serialize as strings
                                .build())
                .build();

        // Convert signals to JSON and write to Kafka
        signals
                .map(signal -> mapper.writeValueAsString(signal)) // Convert Signal objects to JSON
                .sinkTo(sink)                                    // Connect to Kafka sink
                .name("signals-sink");                           // Operator name for monitoring

        // Execute the Flink job
        env.execute("PaperTrading-SignalJob");
    }

    /**
     * Gets environment variable value or returns default if not set
     * 
     * @param key Environment variable name
     * @param defaultValue Default value if environment variable is not set
     * @return Environment variable value or default
     */
    private static String getenvOrDefault(String key, String defaultValue) {
        String value = System.getenv(key);
        return value != null ? value : defaultValue;
    }

    /**
     * Parses JSON string into MarketData object
     * 
     * @param mapper Jackson ObjectMapper for JSON parsing
     * @param json JSON string containing market data
     * @return Parsed MarketData object
     * @throws IOException If JSON parsing fails
     */
    private static MarketData parseMarketData(ObjectMapper mapper, String json) throws IOException {
        return mapper.readValue(json, MarketData.class);
    }

    // --- Data types ---

    /**
     * MarketData class representing a single price tick
     * Contains symbol, price, and timestamp information
     */
    public static class MarketData {
        private String symbol;      // Stock symbol (e.g., "AAPL")
        private double price;       // Current price
        private String timestamp;  // ISO format timestamp

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

    /**
     * Signal class representing a trading signal generated by the strategy
     * Contains all information needed for order execution
     */
    public static class Signal {
        private String symbol;      // Stock symbol (e.g., "AAPL")
        private String side;        // Trade side: "BUY" or "SELL"
        private double price;      // Price at which signal was generated
        private int quantity;       // Quantity to trade (fixed at 10 in this implementation)
        private String timestamp;   // ISO format timestamp

        public Signal() {
        }

        public Signal(String symbol, String side, double price, int quantity, String timestamp) {
            this.symbol = symbol;
            this.side = side;
            this.price = price;
            this.quantity = quantity;
            this.timestamp = timestamp;
        }

        // Getter and setter methods with comments
        
        /** @return Stock symbol for this signal */
        public String getSymbol() {
            return symbol;
        }

        /** @param symbol Stock symbol to set */
        public void setSymbol(String symbol) {
            this.symbol = symbol;
        }

        /** @return Trade side: "BUY" or "SELL" */
        public String getSide() {
            return side;
        }

        /** @param side Trade side to set: "BUY" or "SELL" */
        public void setSide(String side) {
            this.side = side;
        }

        /** @return Price at which signal was generated */
        public double getPrice() {
            return price;
        }

        /** @param price Price to set */
        public void setPrice(double price) {
            this.price = price;
        }

        /** @return Quantity to trade */
        public int getQuantity() {
            return quantity;
        }

        /** @param quantity Quantity to set */
        public void setQuantity(int quantity) {
            this.quantity = quantity;
        }

        /** @return ISO format timestamp */
        public String getTimestamp() {
            return timestamp;
        }

        /** @param timestamp ISO format timestamp to set */
        public void setTimestamp(String timestamp) {
            this.timestamp = timestamp;
        }
    }

    // --- Strategy ---

    /**
     * Momentum-based trading strategy implemented as a Flink ProcessFunction
     * 
     * This strategy generates BUY signals when prices rise above a threshold
     * and SELL signals when prices fall below a negative threshold.
     * 
     * Key features:
     * - Stateful: Maintains last price for each symbol using Flink state
     * - Keyed: Processes each symbol independently
     * - Threshold-based: Configurable sensitivity to price changes
     */
    public static class MomentumStrategyProcessFunction
            extends KeyedProcessFunction<String, MarketData, Signal> {

        private final double threshold; // fractional change threshold, e.g. 0.001 = 10 bps
        private final int quantity;     // quantity to trade for each signal
        private transient ValueState<Double> lastPrice; // Flink state for storing last price

        /**
         * Constructor for momentum strategy
         * 
         * @param threshold Minimum absolute price change required to generate a signal
         *                  Expressed as a fraction (e.g., 0.001 = 0.1% change)
         * @param quantity Fixed quantity to trade for each signal
         */
        public MomentumStrategyProcessFunction(double threshold, int quantity) {
            this.threshold = threshold;
            this.quantity = quantity;
        }

        /**
         * Initialize Flink state when the function is opened
         * 
         * @param parameters Flink configuration parameters
         * @throws Exception If state initialization fails
         */
        @Override
        public void open(Configuration parameters) throws Exception {
            // Create state descriptor for storing the last price per symbol
            ValueStateDescriptor<Double> descriptor = new ValueStateDescriptor<>(
                    "lastPrice",           // State name
                    TypeInformation.of(Double.class)); // State type
            lastPrice = getRuntimeContext().getState(descriptor);
        }

        /**
         * Process each market data element and potentially emit trading signals
         * 
         * This method is called for each incoming price tick and:
         * 1. Compares current price with previous price
         * 2. Calculates percentage change
         * 3. Generates signals if change exceeds threshold
         * 4. Updates state with current price
         * 
         * @param value Current MarketData tick
         * @param ctx Flink context providing time and state access
         * @param out Collector for emitting output signals
         * @throws Exception If processing fails
         */
        @Override
        public void processElement(
                MarketData value,
                Context ctx,
                Collector<Signal> out) throws Exception {
            // Get previous price from state
            Double prev = lastPrice.value();
            
            // Initialize state if this is the first price for this symbol
            if (prev == null || prev <= 0) {
                lastPrice.update(value.getPrice());
                return;
            }

            // Calculate percentage price change
            double change = (value.getPrice() - prev) / prev;
            String side = null;

            // Generate BUY signal for positive momentum above threshold
            if (change > threshold) {
                side = "BUY";
            } 
            // Generate SELL signal for negative momentum below threshold
            else if (change < -threshold) {
                side = "SELL";
            }

            // Update state with current price for next comparison
            lastPrice.update(value.getPrice());

            // Emit signal if threshold was crossed
            if (side != null) {
                Signal signal = new Signal(
                        value.getSymbol(),      // Same symbol as input
                        side,                   // BUY or SELL
                        value.getPrice(),       // Current market price
                        quantity,               // Configured quantity from application.conf
                        Instant.now().toString()); // Current timestamp
                out.collect(signal);
            }
        }
    }
}
