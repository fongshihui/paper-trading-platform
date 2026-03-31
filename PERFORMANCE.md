# Performance Optimization Guide

This document provides performance tuning recommendations for the paper trading platform's Flink/Kafka pipeline.

## Current Performance Characteristics

### Baseline Metrics (Estimated)
- **Throughput**: ~1,000-5,000 messages/second
- **Latency**: 10-100ms end-to-end
- **Memory Usage**: 512MB-1GB per component
- **CPU Usage**: Low to moderate

## Flink Optimization Recommendations

### 1. Checkpointing Configuration

**Current**: 10 seconds interval
**Recommended**: Adjust based on latency requirements

```java
// In SignalJob.java main method
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

// Optimized checkpointing
env.enableCheckpointing(5000L); // 5 seconds for lower latency
env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L); // 2 seconds min pause
env.getCheckpointConfig().setCheckpointTimeout(30000L); // 30 seconds timeout
env.getCheckpointConfig().setTolerableCheckpointFailureNumber(3); // Allow some failures

// For better performance with state backends
// env.setStateBackend(new RocksDBStateBackend("file:///checkpoint-dir"));
```

### 2. Parallelism Tuning

**Current**: Default parallelism (usually 1)
**Recommended**: Scale based on available CPU cores

```java
// Set global parallelism
env.setParallelism(4); // Match available CPU cores

// Or set per-operator parallelism
ticks
    .map(value -> parseMarketData(mapper, value))
    .setParallelism(2)  // Higher for CPU-bound operations
    .returns(TypeInformation.of(new TypeHint<MarketData>() {}));

signals
    .map(signal -> mapper.writeValueAsString(signal))
    .setParallelism(2)  // Higher for serialization
    .sinkTo(sink)
    .setParallelism(2); // Match Kafka partitions
```

### 3. State Backend Optimization

**Current**: Default (usually memory)
**Recommended**: RocksDB for larger state or production use

```xml
<!-- Add to pom.xml -->
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-statebackend-rocksdb</artifactId>
    <version>${flink.version}</version>
</dependency>
```

```java
// Enable RocksDB state backend
env.setStateBackend(new RocksDBStateBackend("file:///checkpoint-dir", true));
```

### 4. Watermark Strategy

**Current**: No watermarks
**Recommended**: Add watermarks for event time processing

```java
// Add proper watermark strategy
WatermarkStrategy<MarketData> watermarkStrategy = WatermarkStrategy
    .<MarketData>forBoundedOutOfOrderness(Duration.ofSeconds(5))
    .withTimestampAssigner((event, timestamp) -> {
        // Parse timestamp from market data
        return Instant.parse(event.getTimestamp()).toEpochMilli();
    });

DataStream<MarketData> ticks = raw
    .map(value -> parseMarketData(mapper, value))
    .assignTimestampsAndWatermarks(watermarkStrategy)
    .returns(TypeInformation.of(new TypeHint<MarketData>() {}));
```

## Kafka Optimization Recommendations

### 1. Producer Configuration

**Current**: Default settings
**Recommended**: Tune for throughput/latency tradeoff

```java
// In mock_prices_producer.py, add producer configuration
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    # Performance optimizations
    linger_ms=5,           # Batch for 5ms
    batch_size=16384,      # 16KB batches
    compression_type='lz4', # Compress messages
    acks='1',              # Leader acknowledgment
    max_in_flight_requests_per_connection=5
)
```

### 2. Consumer Configuration

**Current**: Default settings
**Recommended**: Optimize for throughput

```python
# In simulator/execution.py, optimize consumer
consumer = KafkaConsumer(
    SIGNALS_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    group_id='simulator-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    # Performance optimizations
    fetch_min_bytes=1024,      # Wait for at least 1KB
    fetch_max_wait_ms=100,     # Max wait time
    max_poll_records=500,      # Process up to 500 records per poll
    enable_auto_commit=True,
    auto_commit_interval_ms=1000
)
```

### 3. Topic Configuration

**Recommended**: Create optimized topics

```bash
# Create topics with optimized configuration
docker compose exec kafka kafka-topics --create \
  --topic market-data \
  --partitions 4 \
  --replication-factor 1 \
  --config retention.ms=86400000 \
  --config segment.bytes=1073741824 \
  --bootstrap-server localhost:9092

docker compose exec kafka kafka-topics --create \
  --topic signals \
  --partitions 4 \
  --replication-factor 1 \
  --config retention.ms=86400000 \
  --bootstrap-server localhost:9092
```

## Python Component Optimizations

### 1. Memory Management

**Current**: Basic implementation
**Recommended**: Add memory monitoring and cleanup

```python
# In portfolio.py, add periodic cleanup
import gc

class Portfolio:
    def __init__(self, cash=100000.0):
        self.cash = cash
        self.positions = {}
        self.trade_history = []
        self._cleanup_counter = 0
    
    def periodic_cleanup(self):
        """Clean up memory every 1000 operations"""
        self._cleanup_counter += 1
        if self._cleanup_counter % 1000 == 0:
            gc.collect()
            # Trim history if too large
            if len(self.trade_history) > 10000:
                self.trade_history = self.trade_history[-5000:]
```

### 2. JSON Processing Optimization

**Current**: Standard json module
**Recommended**: Use ujson for faster serialization

```python
# Replace json with ujson in requirements.txt
# Add: ujson>=5.4.0

# In execution.py and producers
import ujson as json  # Much faster JSON processing
```

### 3. Async Processing

**Recommended**: Use asyncio for I/O-bound operations

```python
# Example async Kafka consumer
import asyncio
import aiokafka

async def consume_signals():
    consumer = aiokafka.AIOKafkaConsumer(
        SIGNALS_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id='async-simulator'
    )
    await consumer.start()
    
    try:
        async for msg in consumer:
            signal = json.loads(msg.value.decode())
            await process_signal(signal)
    finally:
        await consumer.stop()
```

## Monitoring and Metrics

### 1. Add Performance Metrics

```java
// In SignalJob.java, add metrics
public void processElement(MarketData value, Context ctx, Collector<Signal> out) {
    // Get metrics registry
    MetricGroup metrics = getRuntimeContext().getMetricGroup();
    metrics.counter("processed_ticks").inc();
    
    long startTime = System.currentTimeMillis();
    // ... processing logic
    
    long duration = System.currentTimeMillis() - startTime;
    metrics.histogram("processing_time_ms").update(duration);
}
```

### 2. Docker Resource Limits

**Recommended**: Add resource constraints to docker-compose.yml

```yaml
# In docker-compose.yml, add to services
services:
  flink-jobmanager:
    deploy:
      resources:
        limits:
          memory: 2G
          cpus: '2'
        reservations:
          memory: 1G
          cpus: '1'
  
  flink-taskmanager:
    deploy:
      resources:
        limits:
          memory: 4G
          cpus: '4'
        reservations:
          memory: 2G
          cpus: '2'
```

## Expected Performance Improvements

| Optimization | Expected Impact |
|--------------|-----------------|
| Checkpointing tuning | 20-30% lower latency |
| Parallelism increase | 2-4x throughput scaling |
| Kafka batching | 3-5x throughput improvement |
| JSON optimization | 2-3x faster processing |
| Memory management | Reduced memory usage by 30-50% |

## Performance Testing

### 1. Load Testing Script

Create `scripts/load_test.py`:

```python
import time
import json
from kafka import KafkaProducer

def run_load_test():
    producer = KafkaProducer(bootstrap_servers='localhost:9092')
    
    start_time = time.time()
    messages_sent = 0
    
    try:
        for i in range(10000):
            message = {
                'symbol': 'TEST',
                'price': 100.0 + (i % 10) - 5.0,
                'timestamp': time.time()
            }
            producer.send('market-data', json.dumps(message).encode())
            messages_sent += 1
            
            if i % 1000 == 0:
                print(f"Sent {messages_sent} messages")
                
    finally:
        producer.close()
        duration = time.time() - start_time
        print(f"Throughput: {messages_sent/duration:.2f} msg/sec")
```

### 2. Monitoring Dashboard

Add Prometheus/Grafana for real-time monitoring:

```yaml
# Add to docker-compose.yml
services:
  prometheus:
    image: prom/prometheus
    ports:
      - "9090:9090"
    
  grafana:
    image: grafana/grafana
    ports:
      - "3000:3000"
```

## Troubleshooting Performance Issues

1. **High CPU Usage**: Reduce parallelism or optimize code
2. **Memory Leaks**: Add garbage collection and memory limits
3. **Kafka Lag**: Increase consumer instances or optimize polling
4. **Checkpoint Timeouts**: Increase timeout or reduce checkpoint interval

Start with the most critical optimizations first (checkpointing, parallelism) and test each change systematically.