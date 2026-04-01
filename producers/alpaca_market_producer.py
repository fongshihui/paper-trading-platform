"""
Alpaca Market Data Producer
===========================

This module provides real-time market data integration with Alpaca API
for the paper trading platform. It automatically falls back to mock data
when Alpaca API is unavailable or not configured.

Key Features:
- Real-time price streaming from Alpaca API
- Automatic fallback to realistic mock data
- Support for multiple stock symbols
- Graceful error handling and recovery
- Kafka integration for downstream processing

Usage:
    python -m producers.alpaca_market_producer
"""

import json
import os
import time
import random
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Generator

import pandas as pd
from dotenv import load_dotenv
from kafka import KafkaProducer

# Try to import Alpaca SDK, fall back to mock if not available
try:
    import alpaca_trade_api as tradeapi
    from alpaca_trade_api.rest import REST
    from alpaca_trade_api.stream import Stream

    ALPACA_AVAILABLE = True
except ImportError:
    ALPACA_AVAILABLE = False
    print("Warning: alpaca-trade-api not installed. Falling back to mock data.")


def _get_env(name: str, default: str) -> str:
    value = os.getenv(name, default)
    return value if value is not None else default


def create_producer(bootstrap_servers: str) -> KafkaProducer:
    """
    Create and configure Kafka producer with performance optimizations.

    Args:
        bootstrap_servers: Kafka broker connection string

    Returns:
        Configured KafkaProducer instance
    """
    compression_type = _get_env("KAFKA_COMPRESSION_TYPE", "gzip").strip().lower()
    compression_setting = None if compression_type in {"", "none"} else compression_type

    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        linger_ms=5,
        batch_size=16384,
        compression_type=compression_setting,
    )


def initialize_alpaca_client() -> Optional[REST]:
    """
    Initialize Alpaca REST client with environment variables.

    Returns:
        Initialized Alpaca REST client or None if unavailable/unconfigured
    """
    if not ALPACA_AVAILABLE:
        return None

    api_key = os.getenv("ALPACA_API_KEY")
    api_secret = os.getenv("ALPACA_API_SECRET")
    base_url = os.getenv("ALPACA_BASE_URL", "https://paper-api.alpaca.markets")

    if not api_key or not api_secret:
        print("Warning: ALPACA_API_KEY or ALPACA_API_SECRET not set. Using mock data.")
        return None

    try:
        return tradeapi.REST(api_key, api_secret, base_url, api_version="v2")
    except Exception as e:
        print(f"Error initializing Alpaca client: {e}")
        return None


def get_real_time_prices(api: REST, symbols: List[str]) -> Dict[str, float]:
    """Get real-time prices from Alpaca API."""
    if not api:
        return {}

    try:
        trades = api.get_latest_trades(symbols)
        return {symbol: float(trade.price) for symbol, trade in trades.items() if trade}
    except Exception as e:
        print(f"Error fetching real-time prices: {e}")
        return {}


def get_historical_prices(api: REST, symbols: List[str]) -> Dict[str, float]:
    """Get historical prices (fallback if real-time not available)."""
    if not api:
        return {}

    try:
        end_time = datetime.now()
        start_time = end_time - timedelta(minutes=5)

        bars = api.get_bars(
            symbols, "1Min", start=start_time.isoformat(), end=end_time.isoformat()
        ).df

        if bars.empty:
            return {}

        latest_prices = {}
        for symbol in symbols:
            symbol_bars = bars[bars["symbol"] == symbol]
            if not symbol_bars.empty:
                latest_prices[symbol] = float(symbol_bars["close"].iloc[-1])

        return latest_prices
    except Exception as e:
        print(f"Error fetching historical prices: {e}")
        return {}


def mock_price_stream(symbols: List[str], base_price: float = 100.0):
    """Fallback mock price generator if Alpaca is unavailable."""
    prices = {s: base_price for s in symbols}

    while True:
        for symbol in symbols:
            drift = random.uniform(-0.1, 0.1)
            shock = random.gauss(0, 0.5)

            prices[symbol] = max(1.0, prices[symbol] + drift + shock)

            yield {
                "symbol": symbol,
                "price": round(prices[symbol], 4),
                "timestamp": datetime.utcnow().isoformat() + "Z",
                "source": "mock",
            }


def alpaca_price_stream(api: REST, symbols: List[str]):
    """Real-time price stream from Alpaca API."""
    last_prices = {}

    while True:
        try:
            real_time_prices = get_real_time_prices(api, symbols)

            if real_time_prices:
                for symbol, price in real_time_prices.items():
                    last_prices[symbol] = price
                    yield {
                        "symbol": symbol,
                        "price": price,
                        "timestamp": datetime.utcnow().isoformat() + "Z",
                        "source": "alpaca_realtime",
                    }
            else:
                historical_prices = get_historical_prices(api, symbols)

                for symbol in symbols:
                    if symbol in historical_prices:
                        price = historical_prices[symbol]
                        last_prices[symbol] = price
                    elif symbol in last_prices:
                        price = last_prices[symbol]
                    else:
                        price = 100.0 + (hash(symbol) % 100) / 10.0
                        last_prices[symbol] = price

                    yield {
                        "symbol": symbol,
                        "price": round(price, 4),
                        "timestamp": datetime.utcnow().isoformat() + "Z",
                        "source": "alpaca_historical",
                    }

            time.sleep(1.0)

        except Exception as e:
            print(f"Error in Alpaca price stream: {e}")
            for tick in mock_price_stream(symbols):
                tick["source"] = "mock_fallback"
                yield tick
            break


def main() -> None:
    """
    Main function for real-time market data production.

    This function orchestrates the entire market data production pipeline.
    """
    load_dotenv()

    bootstrap_servers = _get_env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic = _get_env("MARKET_DATA_TOPIC", "market-data")
    symbols_env = _get_env("SYMBOLS", "AAPL,MSFT,GOOG,TSLA,NVDA")
    interval_seconds = float(_get_env("PRODUCER_INTERVAL_SECS", "1.0"))

    symbols = [s.strip().upper() for s in symbols_env.split(",") if s.strip()]
    if not symbols:
        raise SystemExit("No symbols configured; set SYMBOLS env var.")

    producer = create_producer(bootstrap_servers)
    alpaca_client = initialize_alpaca_client()

    if alpaca_client:
        print(f"Using Alpaca API for real market data: {symbols}")
        price_generator = alpaca_price_stream(alpaca_client, symbols)
    else:
        print(f"Using mock data (Alpaca not available): {symbols}")
        price_generator = mock_price_stream(symbols)

    print(f"Starting market data producer to {bootstrap_servers}, topic='{topic}'")

    try:
        for tick in price_generator:
            producer.send(topic, value=tick)

            if hash(tick["symbol"]) % 10 == 0:
                print(f"Sent: {tick['symbol']} ${tick['price']} ({tick['source']})")

            time.sleep(interval_seconds)

    except KeyboardInterrupt:
        print("\nShutting down market data producer...")
    except Exception as e:
        print(f"Unexpected error: {e}")
    finally:
        producer.close()


if __name__ == "__main__":
    main()
