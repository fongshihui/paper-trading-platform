# Paper Trading Simulator - Order Execution Module
# This module consumes trading signals from Kafka and simulates order execution
# with configurable slippage and transaction fees.

import json
import os
from pathlib import Path

from dotenv import load_dotenv
from kafka import KafkaConsumer

from .portfolio import Portfolio


def _get_env(name: str, default: str) -> str:
    value = os.getenv(name, default)
    return value if value is not None else default


def create_consumer(bootstrap_servers: str, topic: str) -> KafkaConsumer:
    """
    Create and configure a Kafka consumer for trading signals.

    Args:
        bootstrap_servers: Kafka broker addresses (e.g., 'localhost:9092')
        topic: Kafka topic to consume from (e.g., 'signals')

    Returns:
        Configured KafkaConsumer instance with JSON deserialization
    """
    return KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        group_id="paper-trading-simulator",  # Consumer group ID
        auto_offset_reset="latest",  # Start from latest messages
        enable_auto_commit=True,  # Automatically commit offsets
        value_deserializer=lambda v: json.loads(
            v.decode("utf-8")
        ),  # Deserialize JSON messages
    )


def run_simulator() -> None:
    """
    Main simulator function that consumes trading signals and simulates order execution.

    This function:
    - Loads configuration from environment variables
    - Creates Kafka consumer for trading signals
    - Initializes portfolio with starting capital
    - Processes each signal with slippage and fees
    - Updates portfolio state and writes to JSON file
    - Runs continuously until interrupted
    """
    # Load environment variables from .env file
    load_dotenv()

    # Get configuration from environment variables with defaults
    bootstrap_servers = _get_env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic = _get_env("SIGNALS_TOPIC", "signals")
    slippage_bps = float(
        _get_env("SLIPPAGE_BPS", "5")
    )  # Slippage in basis points (5 bps = 0.05%)
    fee_per_trade = float(
        _get_env("FEE_PER_TRADE", "1.0")
    )  # Fixed fee per trade in dollars
    state_path = Path(
        _get_env("SIMULATOR_STATE_PATH", "dashboard/state.json")
    )  # Path for state JSON file
    state_path.parent.mkdir(parents=True, exist_ok=True)  # Ensure directory exists

    # Initialize portfolio with default starting capital ($100,000)
    portfolio = Portfolio()

    # Create Kafka consumer for trading signals
    consumer = create_consumer(bootstrap_servers, topic)

    print(
        f"Starting simulator consuming from {bootstrap_servers}, "
        f"topic='{topic}', slippage_bps={slippage_bps}, fee_per_trade={fee_per_trade}"
    )

    try:
        # Main processing loop: consume messages from Kafka indefinitely
        for message in consumer:
            # Parse incoming trading signal from Kafka message
            signal = message.value
            symbol = signal.get("symbol")  # Stock symbol (e.g., "AAPL")
            side = signal.get("side")  # Trade side: "BUY" or "SELL"
            price = float(signal.get("price", 0) or 0)  # Signal price
            quantity = int(signal.get("quantity", 0) or 0)  # Signal quantity

            # Validate signal - skip if invalid
            if not symbol or side not in {"BUY", "SELL"}:
                continue  # Skip invalid signals
            if price <= 0 or quantity <= 0:
                continue  # Skip signals with invalid price/quantity

            # Calculate execution details with slippage
            direction = 1 if side == "BUY" else -1  # 1 for buy, -1 for sell
            fill_price = price * (
                1 + direction * slippage_bps / 10_000.0
            )  # Apply slippage
            signed_qty = (
                direction * quantity
            )  # Signed quantity (positive for buy, negative for sell)

            # Update portfolio with executed trade
            portfolio.update_cash(
                signed_qty, fill_price, fee_per_trade
            )  # Update cash balance
            portfolio.update_position(symbol, signed_qty, fill_price)  # Update position

            # Calculate portfolio metrics with current market prices
            metrics = portfolio.mark_to_market({symbol: price})
            metrics["last_signal"] = signal  # Include last signal for reference

            # Write updated portfolio state to JSON file for dashboard
            with state_path.open("w") as f:
                json.dump(metrics, f, indent=2)

            # Log processing results
            print(
                f"processed signal: {signal}, "
                f"equity={metrics['equity']:.2f}, cash={metrics['cash']:.2f}"
            )

    except KeyboardInterrupt:
        # Graceful shutdown on Ctrl+C
        print("Stopping simulator...")

    finally:
        # Clean up Kafka consumer
        consumer.close()


if __name__ == "__main__":
    run_simulator()
