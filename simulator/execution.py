# Paper Trading Simulator - Order Execution Module
# This module consumes trading signals from Kafka and simulates order execution
# with configurable slippage and transaction fees.

import json
import os

from dotenv import load_dotenv
from kafka import KafkaConsumer

from .portfolio import Portfolio
from .storage import connect_with_retry, ensure_schema, save_portfolio_snapshot


def _get_env(name: str, default: str) -> str:
    value = os.getenv(name, default)
    return value if value is not None else default


def _str_to_bool(value: str) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


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
        group_id="paper-trading-simulator",
        auto_offset_reset="latest",
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )


def run_simulator() -> None:
    """
    Main simulator function that consumes trading signals and simulates order execution.

    This function:
    - Loads configuration from environment variables
    - Creates Kafka consumer for trading signals
    - Initializes portfolio with starting capital
    - Processes each signal with slippage and fees
    - Updates portfolio state and writes snapshots to Postgres
    - Runs continuously until interrupted
    """
    load_dotenv()

    bootstrap_servers = _get_env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic = _get_env("SIGNALS_TOPIC", "signals")
    slippage_bps = float(_get_env("SLIPPAGE_BPS", "5"))
    fee_per_trade = float(_get_env("FEE_PER_TRADE", "1.0"))
    database_url = _get_env("DATABASE_URL", "postgresql://localhost:5432/paper_trading")
    starting_cash = float(_get_env("STARTING_CASH", "100000.0"))
    max_position = float(_get_env("MAX_POSITION_VALUE_PER_SYMBOL", "25000.0"))
    max_total_position = float(_get_env("MAX_TOTAL_POSITION_VALUE", "100000.0"))
    max_drawdown = float(_get_env("MAX_DRAWDOWN_LIMIT", "0.20"))
    daily_loss_limit = float(_get_env("DAILY_LOSS_LIMIT", "0.05"))
    kill_switch = _str_to_bool(_get_env("TRADING_KILL_SWITCH", "false"))

    portfolio = Portfolio(
        cash=starting_cash,
        max_position_value_per_symbol=max_position,
        max_total_position_value=max_total_position,
        max_drawdown_limit=max_drawdown,
        daily_loss_limit=daily_loss_limit,
        kill_switch=kill_switch,
        daily_start_equity=starting_cash,
    )

    consumer = create_consumer(bootstrap_servers, topic)
    db_conn = connect_with_retry(database_url)
    ensure_schema(db_conn)

    print(
        f"Starting simulator consuming from {bootstrap_servers}, "
        f"topic='{topic}', slippage_bps={slippage_bps}, "
        f"fee_per_trade={fee_per_trade}, risk_limits={portfolio.max_position_value_per_symbol}/{portfolio.max_total_position_value}/{portfolio.max_drawdown_limit}"
    )

    try:
        for message in consumer:
            signal = message.value
            if not isinstance(signal, dict):
                continue

            if "kill_switch" in signal:
                portfolio.set_kill_switch(bool(signal["kill_switch"]))
                print(f"Kill switch set to {portfolio.kill_switch}")
                continue

            symbol = signal.get("symbol")
            side = signal.get("side")
            price = float(signal.get("price", 0) or 0)
            quantity = int(signal.get("quantity", 0) or 0)

            if not symbol or side not in {"BUY", "SELL"}:
                continue
            if price <= 0 or quantity <= 0:
                continue

            direction = 1 if side == "BUY" else -1
            fill_price = price * (1 + direction * slippage_bps / 10_000.0)
            signed_qty = direction * quantity

            accepted = portfolio.execute_trade(
                symbol,
                signed_qty,
                fill_price,
                fees=fee_per_trade,
                prices={symbol: price},
            )
            if not accepted:
                print(
                    f"Rejected trade for {symbol}: {portfolio.last_risk_rejection} "
                    f"(signal={signal})"
                )
                continue

            metrics = portfolio.mark_to_market({symbol: price})
            metrics["last_signal"] = signal
            save_portfolio_snapshot(db_conn, metrics)

            print(
                f"processed signal: {signal}, "
                f"equity={metrics['equity']:.2f}, cash={metrics['cash']:.2f}"
            )

    except KeyboardInterrupt:
        print("Stopping simulator...")

    finally:
        consumer.close()
        db_conn.close()


if __name__ == "__main__":
    run_simulator()
