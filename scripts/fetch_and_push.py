#!/usr/bin/env python3
"""Fetch AKShare data and push as Parquet to a private GitHub mirror.

Usage (from an interactive run, after install.sh):
    ./scripts/fetch_and_push.py               # daily incremental (today only)
    AKSHARE_MIRROR_DAYS_BACK=30 ./scripts/fetch_and_push.py   # backfill
    AKSHARE_MIRROR_SKIP_PUSH=1 ./scripts/fetch_and_push.py    # dry-run

Design notes:
- One day of market_daily is ~5600 rows (~2 MB Parquet with snappy).
  Daily runs write `data/market_daily/YYYY-MM-DD.parquet`.
- `securities`, `financials`, `macro` are small-ish snapshots — overwrite
  in place at `data/<group>/latest.parquet` (+ dated snapshot).
- Financials are per-ticker; we fetch all ~5600 symbols in a thread pool
  and concat. Expect ~30-40 min; run weekly, not daily.
- We never store the GitHub PAT on disk. It must be provided via
  GITHUB_TOKEN env var (install.sh bakes it into a systemd EnvironmentFile
  with mode 0600).
"""

from __future__ import annotations

import logging
import os
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta
from pathlib import Path

import akshare as ak
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = REPO_ROOT / "data"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("akshare-mirror")


def _write_parquet(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, compression="snappy", index=False)
    log.info("wrote %s rows=%d size=%dKB", path.relative_to(REPO_ROOT), len(df), path.stat().st_size // 1024)


def fetch_securities() -> None:
    df = ak.stock_info_a_code_name()
    _write_parquet(df, DATA_DIR / "securities" / "latest.parquet")


def fetch_market_daily(target_date: date) -> pd.DataFrame | None:
    """Fetch OHLCV for all A-shares on a single date.

    AKShare has no single-date cross-sectional bar endpoint, so we use
    `stock_zh_a_spot_em` (live snapshot) when target_date == today, and
    `stock_zh_a_hist` per ticker for historical backfill.
    """
    today = date.today()
    if target_date == today:
        # Cheap path: one call gets the whole market's latest close + cap.
        spot = ak.stock_zh_a_spot_em()
        spot["trade_date"] = today.isoformat()
        _write_parquet(spot, DATA_DIR / "market_daily" / f"{today.isoformat()}.parquet")
        return spot

    # Historical backfill: per-ticker hist. Slow — only used with DAYS_BACK.
    sec = ak.stock_info_a_code_name()
    codes = sec["code"].astype(str).tolist()
    start = end = target_date.strftime("%Y%m%d")
    rows: list[pd.DataFrame] = []

    def _one(code: str):
        try:
            df = ak.stock_zh_a_hist(symbol=code, period="daily", start_date=start, end_date=end, adjust="qfq")
            if df is not None and not df.empty:
                df["code"] = code
                return df
        except Exception as e:  # noqa: BLE001
            log.debug("hist failed for %s: %s", code, e)
        return None

    with ThreadPoolExecutor(max_workers=16) as pool:
        futures = [pool.submit(_one, c) for c in codes]
        for i, fut in enumerate(as_completed(futures), 1):
            out = fut.result()
            if out is not None:
                rows.append(out)
            if i % 500 == 0:
                log.info("market_daily %s: %d/%d", target_date, i, len(codes))

    if not rows:
        log.warning("no bars for %s — skipping write", target_date)
        return None
    df = pd.concat(rows, ignore_index=True)
    df["trade_date"] = target_date.isoformat()
    _write_parquet(df, DATA_DIR / "market_daily" / f"{target_date.isoformat()}.parquet")
    return df


def fetch_financials() -> None:
    sec = ak.stock_info_a_code_name()
    codes = sec["code"].astype(str).tolist()
    rows: list[pd.DataFrame] = []

    def _one(code: str):
        try:
            df = ak.stock_financial_abstract(symbol=code)
            if df is not None and not df.empty:
                df = df.copy()
                df["code"] = code
                return df
        except Exception as e:  # noqa: BLE001
            log.debug("financials failed for %s: %s", code, e)
        return None

    with ThreadPoolExecutor(max_workers=6) as pool:
        futures = [pool.submit(_one, c) for c in codes]
        for i, fut in enumerate(as_completed(futures), 1):
            out = fut.result()
            if out is not None:
                rows.append(out)
            if i % 500 == 0:
                log.info("financials: %d/%d", i, len(codes))

    if not rows:
        log.warning("financials empty — skipping write")
        return
    df = pd.concat(rows, ignore_index=True)
    _write_parquet(df, DATA_DIR / "financials" / "latest.parquet")
    # Dated snapshot so we can track revisions without overwriting history.
    _write_parquet(df, DATA_DIR / "financials" / f"{date.today().isoformat()}.parquet")


def fetch_macro() -> None:
    calls = {
        "pmi": ak.macro_china_pmi_yearly,
        "cpi": ak.macro_china_cpi_yearly,
        "m2": ak.macro_china_money_supply,
        "shibor": ak.macro_china_shibor_all,
    }
    for name, fn in calls.items():
        try:
            df = fn()
            if df is None or df.empty:
                log.warning("macro %s returned empty", name)
                continue
            _write_parquet(df, DATA_DIR / "macro" / f"{name}.parquet")
        except Exception as e:  # noqa: BLE001
            log.warning("macro %s failed: %s", name, e)


def git_push() -> None:
    token = os.environ.get("GITHUB_TOKEN")
    owner = os.environ.get("GITHUB_OWNER", "azzon")
    repo = os.environ.get("GITHUB_REPO", "slate-akshare-mirror")
    if not token:
        log.error("GITHUB_TOKEN not set — skipping push")
        sys.exit(1)

    def run(*args: str, check: bool = True) -> subprocess.CompletedProcess:
        return subprocess.run(args, cwd=REPO_ROOT, check=check, text=True, capture_output=True)

    # Configure identity + remote per-invocation so `~/.gitconfig` stays untouched.
    run("git", "config", "user.email", "akshare-mirror@localhost")
    run("git", "config", "user.name", "akshare-mirror")
    remote = f"https://x-access-token:{token}@github.com/{owner}/{repo}.git"
    # Replace any existing remote URL without logging the token.
    existing = run("git", "remote", check=False).stdout.strip().splitlines()
    if "origin" in existing:
        run("git", "remote", "set-url", "origin", remote)
    else:
        run("git", "remote", "add", "origin", remote)

    run("git", "add", "data/")
    diff = run("git", "diff", "--cached", "--quiet", check=False)
    if diff.returncode == 0:
        log.info("no data changes — skipping commit")
        return
    stamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    run("git", "commit", "-m", f"data: akshare snapshot {stamp}")
    run("git", "push", "origin", "HEAD:main")
    log.info("pushed snapshot %s", stamp)


def main() -> int:
    started = time.time()
    days_back = int(os.environ.get("AKSHARE_MIRROR_DAYS_BACK", "0"))
    skip_financials = os.environ.get("AKSHARE_MIRROR_SKIP_FINANCIALS") == "1"
    skip_push = os.environ.get("AKSHARE_MIRROR_SKIP_PUSH") == "1"

    log.info("securities")
    fetch_securities()

    log.info("market_daily today")
    fetch_market_daily(date.today())

    if days_back > 0:
        log.info("backfilling market_daily %d days", days_back)
        for offset in range(1, days_back + 1):
            fetch_market_daily(date.today() - timedelta(days=offset))

    if skip_financials:
        log.info("skipping financials (AKSHARE_MIRROR_SKIP_FINANCIALS=1)")
    else:
        log.info("financials")
        fetch_financials()

    log.info("macro")
    fetch_macro()

    if skip_push:
        log.info("skipping push (AKSHARE_MIRROR_SKIP_PUSH=1)")
    else:
        git_push()

    log.info("done in %.1fs", time.time() - started)
    return 0


if __name__ == "__main__":
    sys.exit(main())
