#!/usr/bin/env python3
"""Fetch daily turnover_rate for all A-shares via AKShare spot endpoint.

Runs after market close on the ECS mirror runner (mainland China network).
Merges turnover_rate into the existing market_daily parquet for today.

AKShare's `stock_zh_a_spot_em()` returns a full A-share snapshot including
换手率 (turnover rate as percentage, e.g. 0.86 = 0.86%). We convert to
decimal fraction (0.0086) to match SLATE's convention.

This script is designed to run AFTER mirror-market.yml has written
today's OHLCV parquet. It reads the existing file, adds/updates the
turnover_rate column, and writes it back.
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import akshare as ak
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = REPO_ROOT / "data"
STATUS_FILE = DATA_DIR / "_status.json"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("turnover")

CST = timezone(timedelta(hours=8))
AKSHARE_SLEEP = float(os.environ.get("MIRROR_AKSHARE_SLEEP", "1.5"))


def _today_cn() -> date:
    return datetime.now(CST).date()


def _market_daily_path(d: date) -> Path:
    return DATA_DIR / "market_daily" / f"{d.year}" / f"{d.month:02d}" / f"{d.isoformat()}.parquet"


def _code_to_ticker(code: str) -> str:
    """Convert 6-digit code to SLATE ticker format (e.g. 600519 -> 600519.SH)."""
    code = str(code).zfill(6)
    if code.startswith(("6", "9")):
        return f"{code}.SH"
    elif code.startswith(("0", "2", "3")):
        return f"{code}.SZ"
    elif code.startswith(("4", "8")):
        return f"{code}.BJ"
    return f"{code}.SZ"


def fetch_turnover_for_date(d: date) -> pd.DataFrame | None:
    """Fetch turnover_rate for all A-shares for a given date.

    Uses stock_zh_a_spot_em which returns today's real-time snapshot.
    Only useful for today (or intraday); historical turnover needs
    per-ticker calls.
    """
    time.sleep(AKSHARE_SLEEP)
    try:
        df = ak.stock_zh_a_spot_em()
    except Exception as e:
        log.error("stock_zh_a_spot_em failed: %s", e)
        return None

    if df is None or df.empty:
        log.warning("stock_zh_a_spot_em returned empty")
        return None

    # The DataFrame has columns including:
    # 代码, 名称, 换手率, ...
    if "代码" not in df.columns or "换手率" not in df.columns:
        log.error("unexpected columns: %s", list(df.columns)[:20])
        return None

    rows = []
    for _, r in df.iterrows():
        code = str(r["代码"]).zfill(6)
        ticker = _code_to_ticker(code)
        tr_pct = r.get("换手率")
        if pd.isna(tr_pct) or tr_pct is None:
            continue
        # Convert percentage to decimal fraction (0.86% -> 0.0086)
        tr_decimal = float(tr_pct) / 100.0
        rows.append({
            "ticker": ticker,
            "trade_date": d.isoformat(),
            "turnover_rate": tr_decimal,
        })

    if not rows:
        return None

    return pd.DataFrame(rows)


def merge_turnover_into_parquet(d: date, tr_df: pd.DataFrame) -> int:
    """Merge turnover_rate into existing market_daily parquet for date d."""
    path = _market_daily_path(d)

    if not path.exists():
        log.warning("market_daily parquet for %s does not exist yet; writing turnover-only", d)
        # Write a minimal parquet with just ticker + trade_date + turnover_rate
        # The main market_daily fetch will merge it later via _merge_market_daily
        path.parent.mkdir(parents=True, exist_ok=True)
        tr_df["open"] = None
        tr_df["close"] = None
        tr_df["high"] = None
        tr_df["low"] = None
        tr_df["volume"] = None
        tr_df["market_cap"] = None
        cols = ["ticker", "trade_date", "open", "high", "low", "close",
                "volume", "market_cap", "turnover_rate"]
        tr_df[cols].to_parquet(path, compression="snappy", index=False)
        return len(tr_df)

    # Read existing parquet and merge turnover_rate
    existing = pd.read_parquet(path)

    if "turnover_rate" not in existing.columns:
        existing["turnover_rate"] = None

    # Build a ticker -> turnover_rate map
    tr_map = dict(zip(tr_df["ticker"], tr_df["turnover_rate"]))

    # Update turnover_rate for matching tickers
    updated = 0
    existing["turnover_rate"] = existing["ticker"].map(
        lambda t: tr_map.get(t, existing.loc[existing["ticker"] == t, "turnover_rate"].iloc[0]
                             if t in existing["ticker"].values else None)
    )
    # Simpler: just overwrite the column with mapped values, keeping existing where no match
    existing["turnover_rate"] = existing["ticker"].map(tr_map).combine_first(existing["turnover_rate"])
    updated = existing["turnover_rate"].notna().sum()

    existing.to_parquet(path, compression="snappy", index=False)
    log.info("merged turnover_rate into %s: %d/%d tickers populated",
             d, updated, len(existing))
    return int(updated)


def main():
    today = _today_cn()
    now = datetime.now(CST)

    if now.hour < 16:
        log.info("before 16:00 CST — market not closed yet, skipping")
        return

    log.info("fetching turnover_rate for %s", today)
    tr_df = fetch_turnover_for_date(today)
    if tr_df is None:
        log.error("failed to fetch turnover data")
        sys.exit(1)

    log.info("got turnover_rate for %d tickers", len(tr_df))
    populated = merge_turnover_into_parquet(today, tr_df)
    log.info("done: %d tickers with turnover_rate for %s", populated, today)

    # Update status
    status = {}
    if STATUS_FILE.exists():
        try:
            status = json.loads(STATUS_FILE.read_text())
        except Exception:
            status = {"endpoints": {}}
    if "endpoints" not in status:
        status["endpoints"] = {}
    status["endpoints"]["turnover_rate"] = {
        "last_success": datetime.now(timezone.utc).isoformat(),
        "last_attempt": datetime.now(timezone.utc).isoformat(),
        "rows": populated,
        "fail_streak": 0,
    }
    status["updated_at"] = datetime.now(timezone.utc).isoformat()
    STATUS_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp = STATUS_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(status, ensure_ascii=False, indent=2))
    tmp.replace(STATUS_FILE)


if __name__ == "__main__":
    main()
