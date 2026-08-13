import os

import pandas as pd
import streamlit as st
import streamlit.components.v1 as components
from dotenv import load_dotenv

from simulator.storage import (
    connect_with_retry,
    ensure_schema,
    load_latest_portfolio_snapshot,
)


@st.cache_resource
def get_db_connection(database_url: str):
    conn = connect_with_retry(database_url)
    ensure_schema(conn)
    return conn


def main() -> None:
    load_dotenv()

    database_url = os.getenv(
        "DATABASE_URL", "postgresql://paper:paper@localhost:5432/paper_trading"
    )

    st.set_page_config(page_title="Paper Trading Dashboard", layout="wide")
    st.title("Paper Trading Platform — Live Risk & P&L")

    st.caption("Reading simulator metrics from Postgres")

    # Auto-refresh every 5 seconds. Streamlit does not provide st.autorefresh
    # in all versions, so use a tiny browser-side fallback.
    components.html(
        """
        <script>
          setTimeout(() => window.parent.location.reload(), 5000);
        </script>
        """,
        height=0,
    )

    conn = get_db_connection(database_url)
    metrics = load_latest_portfolio_snapshot(conn)
    if metrics is None:
        st.warning(
            "No metrics found yet. Start the simulator so it can write snapshots."
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
