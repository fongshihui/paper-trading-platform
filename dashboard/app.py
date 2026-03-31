import json
import os
from pathlib import Path

import pandas as pd
import streamlit as st
from dotenv import load_dotenv


def load_metrics(path: Path):
    if not path.exists():
        return None
    try:
        with path.open() as f:
            return json.load(f)
    except json.JSONDecodeError:
        return None


def main() -> None:
    load_dotenv()

    state_path = Path(os.getenv("DASHBOARD_STATE_PATH", "dashboard/state.json"))

    st.set_page_config(page_title="Paper Trading Dashboard", layout="wide")
    st.title("Paper Trading Platform — Live Risk & P&L")

    st.caption(f"Reading simulator metrics from `{state_path}`")

    # Auto-refresh every 5 seconds
    st.autorefresh(interval=5000, key="metrics_refresh")

    metrics = load_metrics(state_path)
    if metrics is None:
        st.warning(
            "No metrics found yet. Start the simulator so it can write to "
            f"`{state_path}`."
        )
        return

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Equity", f"{metrics.get('equity', 0):,.2f}")
    col2.metric("Realized P&L", f"{metrics.get('realized_pnl', 0):,.2f}")
    col3.metric("Unrealized P&L", f"{metrics.get('unrealized_pnl', 0):,.2f}")
    col4.metric("Drawdown", f"{metrics.get('drawdown', 0) * 100:.2f}%")

    st.subheader("Positions")
    positions = metrics.get("positions", [])
    if positions:
        df_positions = pd.DataFrame(positions)
        st.dataframe(df_positions, use_container_width=True)
    else:
        st.info("No open positions.")

    st.subheader("Equity Curve")
    curve = metrics.get("equity_curve", [])
    if curve:
        df_curve = pd.DataFrame(curve)
        df_curve["timestamp"] = pd.to_datetime(df_curve["timestamp"])
        df_curve = df_curve.set_index("timestamp")
        st.line_chart(df_curve["equity"], height=300)
    else:
        st.info("Equity history is empty.")

    st.subheader("Volatility")
    st.write(f"Rolling volatility (approx): {metrics.get('volatility', 0):.4f}")


if __name__ == "__main__":
    main()
