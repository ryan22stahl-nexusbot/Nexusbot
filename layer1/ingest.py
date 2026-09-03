#!/usr/bin/env python3
"""Layer 1 ingestion: Databento -> normalized trades -> QuestDB.

Usage:
    python layer1/ingest.py --symbol ES --start 2024-01-01 --end 2024-01-02
"""
from __future__ import annotations

import argparse
import hashlib
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import databento as db
import pandas as pd
from questdb.ingress import Sender, TimestampNanos

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

DATASET = "GLBX.MDP3"
QUESTDB_CONF = os.getenv("QUESTDB_CONF", "http::addr=localhost:9000;")


def get_client() -> db.Historical:
    key = os.getenv("DATABENTO_API_KEY")
    if not key:
        raise RuntimeError("Set DATABENTO_API_KEY in environment or .env")
    return db.Historical(key)


def fetch_trades(symbol: str, start: str, end: str) -> pd.DataFrame:
    client = get_client()
    data = client.timeseries.get_range(
        dataset=DATASET,
        symbols=f"{symbol}.FUT",
        stype_in="parent",
        schema="trades",
        start=start,
        end=end,
    )
    df = data.to_df()
    if df.empty:
        return df
    # Normalize
    out = pd.DataFrame({
        "ts_event": pd.to_datetime(df["ts_event"], utc=True),
        "symbol": df.get("symbol", symbol),
        "price": df["price"].astype(float),
        "size": df["size"].astype("int64"),
        "side": df.get("side", ""),
        "source": "databento",
    })
    out = out.sort_values("ts_event").reset_index(drop=True)
    out["seq"] = range(1, len(out) + 1)
    out["checksum"] = out.apply(
        lambda r: hashlib.sha256(
            f"{r.ts_event.isoformat()}{r.symbol}{r.price}{r.size}{r.side}".encode()
        ).hexdigest()[:16],
        axis=1,
    )
    return out


def ensure_table(sender: Sender) -> None:
    # QuestDB auto-creates on first insert; explicit DDL for safety
    pass


def insert_to_questdb(df: pd.DataFrame, table: str = "trades") -> int:
    if df.empty:
        print("No rows to insert.")
        return 0
    with Sender.from_conf(QUESTDB_CONF) as sender:
        sender.dataframe(
            df,
            table_name=table,
            at="ts_event",
            symbols=["symbol", "side", "source"],
            columns=["price", "size", "seq", "checksum"],
        )
        sender.flush()
    return len(df)


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--symbol", default="ES")
    p.add_argument("--start", required=True)
    p.add_argument("--end", required=True)
    p.add_argument("--table", default="trades")
    args = p.parse_args()

    print(f"Fetching {args.symbol} trades {args.start} -> {args.end}...")
    df = fetch_trades(args.symbol, args.start, args.end)
    print(f"Got {len(df)} ticks.")
    n = insert_to_questdb(df, args.table)
    print(f"Inserted {n} rows into QuestDB table '{args.table}'.")


if __name__ == "__main__":
    main()
