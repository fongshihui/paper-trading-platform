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

import asyncio
import json
import os
import queue
import random
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

from dotenv import load_dotenv
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# Try to import modern Alpaca SDK first, then legacy SDK as fallback.
try:
    from alpaca.data.live import StockDataStream

    ALPACA_PY_AVAILABLE = True
    ALPACA_AVAILABLE = True
except ImportError:
    ALPACA_PY_AVAILABLE = False
    try:
        import alpaca_trade_api as tradeapi
        from alpaca_trade_api.rest import REST

        ALPACA_AVAILABLE = True
    except ImportError:
        ALPACA_AVAILABLE = False
        print("Warning: no supported Alpaca SDK found. Falling back to mock data.")
        REST = None


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


def create_producer_with_retry(bootstrap_servers: str) -> KafkaProducer:
    """
    Create Kafka producer with bounded retries to handle startup race conditions.

    Environment variables:
      - KAFKA_CONNECT_MAX_RETRIES (default: 30)
      - KAFKA_CONNECT_RETRY_DELAY_SECS (default: 2.0)
    """
    max_retries = int(_get_env("KAFKA_CONNECT_MAX_RETRIES", "30"))
    retry_delay_secs = float(_get_env("KAFKA_CONNECT_RETRY_DELAY_SECS", "2.0"))
    last_error: Optional[Exception] = None

    for attempt in range(1, max_retries + 1):
        try:
            producer = create_producer(bootstrap_servers)
            print(
                f"Connected to Kafka at {bootstrap_servers} "
                f"(attempt {attempt}/{max_retries})"
            )
            return producer
        except NoBrokersAvailable as exc:
            last_error = exc
            print(
                f"Kafka broker not ready at {bootstrap_servers} "
                f"(attempt {attempt}/{max_retries}). Retrying in {retry_delay_secs}s..."
            )
            time.sleep(retry_delay_secs)

    raise RuntimeError(
        f"Unable to connect to Kafka broker(s) at {bootstrap_servers} after "
        f"{max_retries} attempts"
    ) from last_error


def initialize_alpaca_client():
    """Initialize Alpaca client config for either alpaca-py or legacy SDK."""
    if ALPACA_PY_AVAILABLE:
        api_key = os.getenv("ALPACA_API_KEY") or os.getenv("ALPACA_KEY_ID")
        api_secret = os.getenv("ALPACA_API_SECRET") or os.getenv("ALPACA_SECRET_KEY")
        base_url = os.getenv("ALPACA_BASE_URL", "https://paper-api.alpaca.markets")
        data_url = os.getenv("ALPACA_DATA_URL", "https://data.alpaca.markets")

        if not api_key or not api_secret:
            print("Warning: Alpaca credentials not set. Using mock data.")
            return None

        return {
            "api_key": api_key,
            "secret_key": api_secret,
            "base_url": base_url,
            "data_url": data_url,
            "feed": os.getenv("ALPACA_DATA_FEED", "iex"),
        }

    if not ALPACA_AVAILABLE:
        return None

    api_key = os.getenv("ALPACA_API_KEY") or os.getenv("ALPACA_KEY_ID")
    api_secret = os.getenv("ALPACA_API_SECRET") or os.getenv("ALPACA_SECRET_KEY")
    base_url = os.getenv("ALPACA_BASE_URL", "https://paper-api.alpaca.markets")

    if not api_key or not api_secret:
        print("Warning: ALPACA_API_KEY or ALPACA_API_SECRET not set. Using mock data.")
        return None

    try:
        return tradeapi.REST(api_key, api_secret, base_url, api_version="v2")
    except Exception as e:
        print(f"Error initializing Alpaca client: {e}")
        return None


def get_real_time_prices(api: object, symbols: List[str]) -> Dict[str, float]:
    """Get real-time prices from Alpaca API."""
    if not api:
        return {}

    try:
        trades = api.get_latest_trades(symbols)
        return {symbol: float(trade.price) for symbol, trade in trades.items() if trade}
    except Exception as e:
        print(f"Error fetching real-time prices: {e}")
        return {}


def get_historical_prices(api: object, symbols: List[str]) -> Dict[str, float]:
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


def alpaca_price_stream(api, symbols: List[str]):
    """Use the modern alpaca-py websocket when available; otherwise fall back to legacy polling."""
    if ALPACA_PY_AVAILABLE and isinstance(api, dict):
        stream_queue = queue.Queue()
        api_key = api["api_key"]
        api_secret = api["secret_key"]
        feed = api.get("feed", "iex")

        # Convert feed string to the alpaca-py Feed enum when necessary
        try:
            from alpaca.data.enums import DataFeed

            if isinstance(feed, str):
                feed_enum = getattr(DataFeed, feed.upper(), None)
            else:
                feed_enum = feed
            if feed_enum is None:
                # default to IEX if unknown
                feed_enum = DataFeed.IEX
        except Exception:
            feed_enum = feed

        print(f"DEBUG: creating StockDataStream with feed={feed!r}, feed_enum={feed_enum!r}, type={type(feed_enum)}")
        stream = StockDataStream(api_key, api_secret, feed=feed_enum)

        async def _on_trade(trade):
            symbol = getattr(trade, "symbol", None)
            if not symbol or symbol not in symbols:
                return
            price = getattr(trade, "price", None)
            if price is None:
                return
            ts = getattr(trade, "timestamp", datetime.now(timezone.utc))
            if hasattr(ts, "isoformat"):
                ts_str = ts.isoformat().replace("+00:00", "Z")
            else:
                ts_str = datetime.fromtimestamp(float(ts), tz=timezone.utc).isoformat().replace("+00:00", "Z")
            stream_queue.put({
                "symbol": symbol,
                "price": round(float(price), 4),
                "timestamp": ts_str,
                "source": "alpaca_realtime",
            })

        def _runner():
            try:
                stream.subscribe_trades(_on_trade, *symbols)
                stream.run()
            except Exception as exc:
                print(f"Alpaca websocket error: {exc}")

        worker = threading.Thread(target=_runner, daemon=True)
        worker.start()

        while True:
            try:
                yield stream_queue.get(timeout=1.0)
            except queue.Empty:
                continue

    try:
        from alpaca_trade_api.stream import DataStream
    except ImportError:
        print("Warning: Alpaca WebSocket stream unavailable; falling back to REST polling.")
        yield from alpaca_price_stream_polling(api, symbols)
        return

    stream_queue = queue.Queue()
    data_stream_url = os.getenv("ALPACA_DATA_STREAM_URL", "https://data.alpaca.markets")

    ds = DataStream(
        os.getenv("ALPACA_API_KEY") or os.getenv("ALPACA_KEY_ID"),
        os.getenv("ALPACA_API_SECRET") or os.getenv("ALPACA_SECRET_KEY"),
        data_stream_url,
        raw_data=False,
        feed=os.getenv("ALPACA_DATA_FEED", "iex"),
    )

    async def _on_trade(trade):
        symbol = getattr(trade, "S", None)
        if not symbol or symbol not in symbols:
            return
        price = float(getattr(trade, "p", 0) or 0)
        if price <= 0:
            return
        t_ns = getattr(trade, "t", None)
        if t_ns is None:
            ts = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        else:
            ts = datetime.fromtimestamp(int(t_ns) / 1e9, tz=timezone.utc).isoformat().replace("+00:00", "Z")
        stream_queue.put({
            "symbol": symbol,
            "price": round(price, 4),
            "timestamp": ts,
            "source": "alpaca_realtime",
        })

    ds.subscribe_trades(_on_trade, *symbols)

    def _runner():
        asyncio.run(ds._run_forever())

    worker = threading.Thread(target=_runner, daemon=True)
    worker.start()

    while True:
        try:
            tick = stream_queue.get(timeout=1.0)
            yield tick
        except queue.Empty:
            continue


def alpaca_price_stream_polling(api: object, symbols: List[str]):
    """Legacy polling fallback used when websocket stream is unavailable."""
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

    producer = create_producer_with_retry(bootstrap_servers)
    alpaca_client = initialize_alpaca_client()

    if alpaca_client:
        print(f"Using Alpaca API for real market data: {symbols}")
        price_generator = alpaca_price_stream(alpaca_client, symbols)
    else:
        print(f"Using mock data (Alpaca not available): {symbols}")
        price_generator = mock_price_stream(symbols)

    print(f"DEBUG: Alpaca client initialized? {alpaca_client is not None}")

    print(f"Starting market data producer to {bootstrap_servers}, topic='{topic}'")
    print(f"Producer config: symbols={symbols}, interval_seconds={interval_seconds}, bootstrap_servers={bootstrap_servers}")

    try:
        tick_count = 0
        for tick in price_generator:
            tick_count += 1
            print(f"DEBUG tick[{tick_count}] before send: {tick}")
            producer.send(topic, value=tick)
            print(f"DEBUG tick[{tick_count}] sent to Kafka topic={topic}")

            if hash(tick["symbol"]) % 10 == 0:
                print(f"Sent: {tick['symbol']} ${tick['price']} ({tick['source']})")

            time.sleep(interval_seconds)
    except KeyboardInterrupt:
        print("\nShutting down market data producer...")
    except Exception as e:
        import traceback
        print("Unexpected error:")
        traceback.print_exc()
    finally:
        producer.close()


if __name__ == "__main__":
    main()
