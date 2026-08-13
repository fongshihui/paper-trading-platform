import json
import os
import time
from typing import Dict, Optional

import psycopg
from psycopg.rows import dict_row


def get_database_url() -> str:
    return os.getenv(
        "DATABASE_URL",
        "postgresql://paper:paper@localhost:5432/paper_trading",
    )


def connect_with_retry(database_url: Optional[str] = None, retries: int = 30):
    url = database_url or get_database_url()
    last_error = None
    for _ in range(retries):
        try:
            return psycopg.connect(url, row_factory=dict_row)
        except psycopg.OperationalError as exc:
            last_error = exc
            time.sleep(2)
    raise RuntimeError(f"Could not connect to database: {last_error}") from last_error


def ensure_schema(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS portfolio_snapshots (
                id BIGSERIAL PRIMARY KEY,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                snapshot JSONB NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_portfolio_snapshots_created_at
                ON portfolio_snapshots (created_at DESC);
            """
        )
    conn.commit()


def save_portfolio_snapshot(conn, snapshot: Dict) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO portfolio_snapshots (snapshot) VALUES (%s)",
            (json.dumps(snapshot),),
        )
    conn.commit()


def load_latest_portfolio_snapshot(conn) -> Optional[Dict]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT snapshot
            FROM portfolio_snapshots
            ORDER BY created_at DESC
            LIMIT 1
            """
        )
        row = cur.fetchone()
    return dict(row["snapshot"]) if row else None
