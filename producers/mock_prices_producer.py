import json
import os
import random
import time
from datetime import datetime

from dotenv import load_dotenv
from kafka import KafkaProducer


def _get_env(name: str, default: str) -> str:
    value = os.getenv(name, default)
    return value if value is not None else default


def create_producer(bootstrap_servers: str) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )


def price_stream(symbols, base_price: float = 100.0):
    """Simple random walk price generator for a set of symbols."""

    prices = {s: base_price + random.uniform(-1, 1) for s in symbols}
    while True:
        for symbol in symbols:
            # tiny random walk with drift + noise
            drift = random.uniform(-0.1, 0.1)
            shock = random.gauss(0, 0.5)
            prices[symbol] = max(1.0, prices[symbol] + drift + shock)

            yield {
                "symbol": symbol,
                "price": round(prices[symbol], 4),
                "timestamp": datetime.utcnow().isoformat() + "Z",
            }


def main() -> None:
    load_dotenv()

    bootstrap_servers = _get_env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic = _get_env("MARKET_DATA_TOPIC", "market-data")
    symbols_env = _get_env("SYMBOLS", "AAPL,MSFT,GOOG")
    interval_seconds = float(_get_env("PRODUCER_INTERVAL_SECS", "1.0"))

    symbols = [s.strip().upper() for s in symbols_env.split(",") if s.strip()]
    if not symbols:
        raise SystemExit("No symbols configured; set SYMBOLS env var.")

    producer = create_producer(bootstrap_servers)
    print(
        f"Starting mock price producer to {bootstrap_servers}, "
        f"topic='{topic}', symbols={symbols}"
    )

    try:
        for tick in price_stream(symbols):
            producer.send(topic, value=tick)
            print(f"sent tick: {tick}")
            time.sleep(interval_seconds)
    except KeyboardInterrupt:
        print("Stopping producer...")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
