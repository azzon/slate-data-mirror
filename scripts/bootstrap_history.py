#!/usr/bin/env python3
"""One-shot backfill of historical data.

Call once on a fresh mirror to populate `market_daily/` with ~5 years of
per-ticker OHLCV. Steady-state cron (`fetch_all.py`) then appends the
current day only.

Strategy:
- **Bulk-mode per ticker, not per day.** For each A-share code, fetch
  the full start→end window in one AKShare call, then group by
  trade_date and write one parquet per date.
- **Resumable.** On each ticker, skip if today's window is already
  covered. We keep a `data/_bootstrap_progress.json` file that tracks
  which tickers are done so a crashed run can resume without duplicating
  ~5600 5-year requests.
- **Incremental push.** Every 200 completed tickers we push current
  state. A full bootstrap can take 2-3h and we don't want to lose all
  that on a runner disconnect.
- **Never overwrites a date that already has a file.** We read each
  target date's parquet (if any), merge, dedupe on (code, trade_date),
  and rewrite. Safe to re-run at any time.

Usage:
    python3 scripts/bootstrap_history.py --years 5
    python3 scripts/bootstrap_history.py --years 5 --only market_daily
    python3 scripts/bootstrap_history.py --resume
"""

from __future__ import annotations

import argparse
import json
import logging
import random
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta, timezone
from http.client import RemoteDisconnected
from pathlib import Path
from urllib.error import URLError

import akshare as ak
import pandas as pd
import requests

TRANSIENT = (
    RemoteDisconnected,
    ConnectionError,
    ConnectionResetError,
    URLError,
    requests.exceptions.ConnectionError,
    requests.exceptions.ReadTimeout,
    requests.exceptions.ChunkedEncodingError,
    TimeoutError,
)

REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = REPO_ROOT / "data"
PROGRESS_FILE = DATA_DIR / "_bootstrap_progress.json"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("bootstrap")


def _load_progress() -> dict:
    if PROGRESS_FILE.exists():
        return json.loads(PROGRESS_FILE.read_text())
    return {"market_daily": {"done_codes": [], "started_at": datetime.now(timezone.utc).isoformat()}}


def _save_progress(p: dict) -> None:
    PROGRESS_FILE.parent.mkdir(parents=True, exist_ok=True)
    PROGRESS_FILE.write_text(json.dumps(p, ensure_ascii=False, indent=2))


def _market_daily_path(d: date) -> Path:
    return DATA_DIR / "market_daily" / f"{d.year}" / f"{d.month:02d}" / f"{d.isoformat()}.parquet"


def _git_push(msg: str) -> None:
    def r(*args):
        return subprocess.run(args, cwd=REPO_ROOT, check=False, text=True, capture_output=True)
    r("git", "add", "data/")
    if r("git", "diff", "--cached", "--quiet").returncode == 0:
        return
    r("git", "commit", "-m", msg)
    p = r("git", "push", "origin", "HEAD:main")
    if p.returncode != 0:
        log.warning("  git push failed: %s", (p.stderr or p.stdout or "").strip()[-200:])


def bootstrap_market_daily(years: int, workers: int = 8) -> None:
    today = date.today()
    start = (today - timedelta(days=365 * years + 1)).strftime("%Y%m%d")
    end = today.strftime("%Y%m%d")

    sec = ak.stock_info_a_code_name()
    all_codes = sec["code"].astype(str).tolist()
    progress = _load_progress()
    done = set(progress["market_daily"]["done_codes"])
    failed = progress["market_daily"].get("failed_codes", {})  # code -> last_error
    # Retry previously-failed codes this run
    todo = [c for c in all_codes if c not in done]

    log.info("market_daily bootstrap: %d total, %d done, %d remaining (of which %d previously failed)",
             len(all_codes), len(done), len(todo), len(failed))
    log.info("window: %s → %s, workers: %d", start, end, workers)

    # When the upstream rate-limits us (RemoteDisconnected wave), pause
    # the whole pool until the error stream dies down. We track consecutive
    # transient failures and back off exponentially if we see a burst.
    rl_lock = threading.Lock()
    rl_state = {"consecutive_fails": 0, "cool_until": 0.0}

    def one(code: str):
        # per-call jitter to avoid all workers hammering in lockstep
        time.sleep(random.uniform(0.05, 0.25))

        last_err: str | None = None
        for attempt in range(1, 4):  # 3 tries max per ticker
            # Respect an active cooldown window (set by peer workers)
            while time.time() < rl_state["cool_until"]:
                time.sleep(1)
            try:
                df = ak.stock_zh_a_hist(
                    symbol=code, period="daily",
                    start_date=start, end_date=end, adjust="qfq",
                )
            except TRANSIENT as e:
                last_err = f"transient: {type(e).__name__}"
                with rl_lock:
                    rl_state["consecutive_fails"] += 1
                    if rl_state["consecutive_fails"] >= 10:
                        cool = min(30 * (2 ** min(rl_state["consecutive_fails"] // 10, 4)), 300)
                        rl_state["cool_until"] = time.time() + cool
                        log.warning("  rate-limit burst (%d consec) → cool %ds",
                                    rl_state["consecutive_fails"], cool)
                        rl_state["consecutive_fails"] = 0
                if attempt < 3:
                    time.sleep(random.uniform(1, 3) * attempt)
                continue
            except Exception as e:  # noqa: BLE001
                return code, None, f"{type(e).__name__}: {str(e)[:100]}"

            # Request succeeded — reset fail streak
            with rl_lock:
                rl_state["consecutive_fails"] = max(0, rl_state["consecutive_fails"] - 1)
            if df is None or df.empty:
                return code, None, "empty_response"
            df = df.copy()
            df["code"] = code
            return code, df, None

        return code, None, last_err or "max_retries"

    CHUNK = 200
    processed_in_chunk = 0
    started = time.time()

    # Track within-run progress separately so the progress JSON reflects
    # *genuine* success.
    new_done: set[str] = set()
    new_failed: dict[str, str] = {}

    # In-memory rows by date, flushed per CHUNK successful tickers.
    rows_by_date: dict[str, list[pd.DataFrame]] = {}

    def flush_chunk() -> None:
        if not rows_by_date:
            return
        written = 0
        for trade_date, frames in rows_by_date.items():
            d = date.fromisoformat(trade_date)
            path = _market_daily_path(d)
            path.parent.mkdir(parents=True, exist_ok=True)
            new_df = pd.concat(frames, ignore_index=True)
            # Merge with existing file if present
            if path.exists():
                prior = pd.read_parquet(path)
                merged = pd.concat([prior, new_df], ignore_index=True)
                merged = merged.drop_duplicates(subset=["code", "trade_date"], keep="last")
                merged.to_parquet(path, compression="snappy", index=False)
            else:
                new_df.to_parquet(path, compression="snappy", index=False)
            written += len(new_df)
        rows_by_date.clear()
        log.info("  flushed %d rows across %d dates", written, len(list(rows_by_date)))

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(one, c): c for c in todo}
        completed = 0
        for fut in as_completed(futures):
            code, df, err = fut.result()
            completed += 1
            if err:
                new_failed[code] = err
            elif df is not None and not df.empty:
                if "日期" in df.columns:
                    df["trade_date"] = pd.to_datetime(df["日期"]).dt.strftime("%Y-%m-%d")
                elif "trade_date" not in df.columns:
                    candidate = next((c for c in df.columns if "date" in c.lower()), None)
                    if candidate:
                        df["trade_date"] = pd.to_datetime(df[candidate]).dt.strftime("%Y-%m-%d")
                    else:
                        new_failed[code] = f"no_date_col: {list(df.columns)[:5]}"
                        continue
                for trade_date, grp in df.groupby("trade_date"):
                    rows_by_date.setdefault(trade_date, []).append(grp)
                new_done.add(code)
                processed_in_chunk += 1

            if completed % 100 == 0:
                elapsed = time.time() - started
                rate = completed / elapsed
                eta_min = (len(todo) - completed) / rate / 60 if rate > 0 else 0
                log.info("  progress: %d/%d ok=%d fail=%d (%.1f/s, eta %.1fm)",
                         completed, len(todo), len(new_done), len(new_failed),
                         rate, eta_min)

            if processed_in_chunk >= CHUNK:
                flush_chunk()
                done.update(new_done)
                progress["market_daily"]["done_codes"] = sorted(done)
                progress["market_daily"]["failed_codes"] = new_failed
                progress["market_daily"]["last_checkpoint"] = datetime.now(timezone.utc).isoformat()
                _save_progress(progress)
                _git_push(f"bootstrap(market_daily): +{processed_in_chunk} ok, "
                          f"{len(new_failed)} fail ({len(done)}/{len(all_codes)} total)")
                processed_in_chunk = 0

    # Final flush
    flush_chunk()
    done.update(new_done)
    progress["market_daily"]["done_codes"] = sorted(done)
    progress["market_daily"]["failed_codes"] = new_failed
    progress["market_daily"]["completed_at"] = datetime.now(timezone.utc).isoformat()
    _save_progress(progress)
    total_mins = (time.time() - started) / 60
    _git_push(
        f"bootstrap(market_daily): {len(new_done)} ok, {len(new_failed)} fail "
        f"(total done {len(done)}/{len(all_codes)}, {years}y, {total_mins:.1f}m)"
    )
    log.info("market_daily bootstrap: ok=%d fail=%d in %.1f min",
             len(new_done), len(new_failed), total_mins)


def bootstrap_financials() -> None:
    """Financials are already full-history per fetch — steady-state
    fetch_financials already stores the whole available window.
    Nothing to do here beyond running it once."""
    log.info("financials: nothing special — run fetch_all.py --only financials")


def bootstrap_macro() -> None:
    """Macro series return full history per call — same deal."""
    log.info("macro: nothing special — run fetch_all.py --only macro")


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--years", type=int, default=5, help="how many years of market_daily to backfill")
    p.add_argument("--only", help="comma-separated group names (market_daily only for now)")
    p.add_argument("--resume", action="store_true",
                   help="continue from _bootstrap_progress.json (default if file exists)")
    p.add_argument("--reset", action="store_true", help="ignore prior progress")
    p.add_argument("--workers", type=int, default=8,
                   help="concurrent workers (tune down if rate-limited)")
    args = p.parse_args()

    if args.reset and PROGRESS_FILE.exists():
        PROGRESS_FILE.unlink()

    only = [s.strip() for s in args.only.split(",")] if args.only else ["market_daily"]

    if "market_daily" in only:
        bootstrap_market_daily(years=args.years, workers=args.workers)
    if "financials" in only:
        bootstrap_financials()
    if "macro" in only:
        bootstrap_macro()
    return 0


if __name__ == "__main__":
    sys.exit(main())
