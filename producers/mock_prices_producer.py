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
    """
    Infinite generator that produces mock price data using random walk.

    This function uses a geometric Brownian motion model to simulate
    realistic-looking price movements for testing and development.

    Args:
        symbols: List of stock symbols to generate prices for
        base_price: Starting price for all symbols (default: 100.0)

    Yields:
        Dictionary with symbol, price, and timestamp for each price tick
    """
    # Initialize prices with random variation around base price
    prices = {s: base_price + random.uniform(-1, 1) for s in symbols}

    # Infinite loop to continuously generate price data
    while True:
        for symbol in symbols:
            # Random walk components:
            # - drift: small deterministic trend (-0.1 to 0.1)
            # - shock: random noise with normal distribution (mean=0, std=0.5)
            drift = random.uniform(-0.1, 0.1)
            shock = random.gauss(0, 0.5)

            # Update price with drift and shock, ensuring it doesn't go below 1.0
            prices[symbol] = max(1.0, prices[symbol] + drift + shock)

            # Yield price data as a dictionary with proper formatting
            yield {
                "symbol": symbol,  # Stock symbol (e.g., 'AAPL')
                "price": round(prices[symbol], 4),  # Rounded to 4 decimal places
                "timestamp": datetime.utcnow().isoformat()
                + "Z",  # ISO format with UTC timezone
            }


def main() -> None:
    """
    Main function that runs the mock price producer.

    Loads configuration from environment variables, creates Kafka producer,
    and continuously generates and sends mock price data to Kafka.
    """
    # Load environment variables from .env file
    load_dotenv()

    # Get configuration from environment variables with defaults
    bootstrap_servers = _get_env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic = _get_env("MARKET_DATA_TOPIC", "market-data")
    symbols_env = _get_env("SYMBOLS", "AAPL,MSFT,GOOG")
    interval_seconds = float(_get_env("PRODUCER_INTERVAL_SECS", "1.0"))

    # Parse and validate symbols list
    symbols = [s.strip().upper() for s in symbols_env.split(",") if s.strip()]
    if not symbols:
        raise SystemExit("No symbols configured; set SYMBOLS env var.")

    # Create Kafka producer instance
    producer = create_producer(bootstrap_servers)
    print(
        f"Starting mock price producer to {bootstrap_servers}, "
        f"topic='{topic}', symbols={symbols}"
    )

    try:
        # Main loop: generate prices and send to Kafka
        for tick in price_stream(symbols):
            # Send price data to Kafka topic
            producer.send(topic, value=tick)
            print(f"sent tick: {tick}")

            # Control the rate of price generation
            time.sleep(interval_seconds)

    except KeyboardInterrupt:
        # Graceful shutdown on Ctrl+C
        print("Stopping producer...")
    finally:
        # Ensure all messages are sent before exiting
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
