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
    return KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        group_id="paper-trading-simulator",
        auto_offset_reset="latest",
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )


def run_simulator() -> None:
    load_dotenv()

    bootstrap_servers = _get_env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic = _get_env("SIGNALS_TOPIC", "signals")
    slippage_bps = float(_get_env("SLIPPAGE_BPS", "5"))
    fee_per_trade = float(_get_env("FEE_PER_TRADE", "1.0"))
    state_path = Path(_get_env("SIMULATOR_STATE_PATH", "dashboard/state.json"))
    state_path.parent.mkdir(parents=True, exist_ok=True)

    portfolio = Portfolio()
    consumer = create_consumer(bootstrap_servers, topic)

    print(
        f"Starting simulator consuming from {bootstrap_servers}, "
        f"topic='{topic}', slippage_bps={slippage_bps}, fee_per_trade={fee_per_trade}"
    )

    try:
        for message in consumer:
            signal = message.value
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

            portfolio.update_cash(signed_qty, fill_price, fee_per_trade)
            portfolio.update_position(symbol, signed_qty, fill_price)

            metrics = portfolio.mark_to_market({symbol: price})
            metrics["last_signal"] = signal

            with state_path.open("w") as f:
                json.dump(metrics, f, indent=2)

            print(
                f"processed signal: {signal}, "
                f"equity={metrics['equity']:.2f}, cash={metrics['cash']:.2f}"
            )
    except KeyboardInterrupt:
        print("Stopping simulator...")
    finally:
        consumer.close()


if __name__ == "__main__":
    run_simulator()
