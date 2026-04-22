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
import subprocess
import sys
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
import tencent_market as tm  # noqa: E402

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

    # Universe from Tencent (cheap probe, ~45s). This also populates
    # securities/latest.parquet consistency with fetch_all.py's source.
    universe = tm.fetch_universe()
    all_tickers = [u["ticker"] for u in universe]
    progress = _load_progress()
    done = set(progress["market_daily"]["done_codes"])  # Now stores tickers, not bare codes
    failed = progress["market_daily"].get("failed_codes", {})
    todo = [t for t in all_tickers if t not in done]

    log.info("market_daily bootstrap: %d total, %d done, %d remaining (of which %d previously failed)",
             len(all_tickers), len(done), len(todo), len(failed))
    log.info("window: %s → %s, workers: %d (Tencent gtimg)", start, end, workers)

    # Tencent has its own internal keep-alive pool + worker management in
    # tm.fetch_daily_bars. We batch per CHUNK tickers and let it manage
    # concurrency. Unlike Eastmoney, Tencent was stable at 8 workers for
    # 5500-ticker sweeps in prior slate nightly runs.

    # Lower chunk size: 1.6 GB aliyun box can't hold 200 tickers × 4 windows ×
    # 1200 bars = ~1M rows in rows_by_date before flushing. Flushing smaller
    # chunks keeps peak memory well under the OOM threshold that cooked the
    # prior bootstrap run (took out sshd along with the worker).
    CHUNK = 50
    processed_in_chunk = 0
    started = time.time()

    new_done: set[str] = set()
    new_failed: dict[str, str] = {}

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

    # Process in CHUNK-sized ticker batches. Each batch hits Tencent
    # via tm.fetch_daily_bars (max_workers=8 internally), flushes to
    # parquet, checkpoints progress, and pushes.
    completed = 0
    for chunk_start in range(0, len(todo), CHUNK):
        chunk = todo[chunk_start:chunk_start + CHUNK]
        try:
            bars_by_ticker = tm.fetch_daily_bars(
                chunk, start=start, end=end, max_workers=workers
            )
        except Exception as e:  # noqa: BLE001
            log.error("  chunk %d-%d failed: %s — skipping",
                      chunk_start, chunk_start + len(chunk), e)
            for t in chunk:
                new_failed[t] = f"chunk_error: {type(e).__name__}"
            continue

        for ticker in chunk:
            bars = bars_by_ticker.get(ticker, [])
            if not bars:
                new_failed[ticker] = "no_bars"
                continue
            code = ticker.split(".")[0]
            df = pd.DataFrame([
                {
                    "code": code, "ticker": ticker,
                    "trade_date": b["trade_date"],
                    "open": b["open"], "close": b["close"],
                    "high": b["high"], "low": b["low"],
                    "volume": b["volume"],
                }
                for b in bars
            ])
            for trade_date, grp in df.groupby("trade_date"):
                rows_by_date.setdefault(trade_date, []).append(grp)
            new_done.add(ticker)
            processed_in_chunk += 1

        completed = chunk_start + len(chunk)
        elapsed = time.time() - started
        rate = completed / elapsed if elapsed > 0 else 0
        eta_min = (len(todo) - completed) / rate / 60 if rate > 0 else 0
        log.info("  batch done: %d/%d ok=%d fail=%d (%.1f ticker/s, eta %.1fm)",
                 completed, len(todo), len(new_done), len(new_failed),
                 rate, eta_min)

        # Flush + checkpoint + push after every chunk.
        flush_chunk()
        done.update(new_done)
        progress["market_daily"]["done_codes"] = sorted(done)
        progress["market_daily"]["failed_codes"] = new_failed
        progress["market_daily"]["last_checkpoint"] = datetime.now(timezone.utc).isoformat()
        _save_progress(progress)
        _git_push(
            f"bootstrap(market_daily): +{processed_in_chunk} ok, "
            f"{len(new_failed)} fail ({len(done)}/{len(all_tickers)} total)"
        )
        processed_in_chunk = 0

    # Final flush (safety — chunk loop flushes every iter).
    flush_chunk()
    done.update(new_done)
    progress["market_daily"]["done_codes"] = sorted(done)
    progress["market_daily"]["failed_codes"] = new_failed
    progress["market_daily"]["completed_at"] = datetime.now(timezone.utc).isoformat()
    _save_progress(progress)
    total_mins = (time.time() - started) / 60
    _git_push(
        f"bootstrap(market_daily): {len(new_done)} ok, {len(new_failed)} fail "
        f"(total done {len(done)}/{len(all_tickers)}, {years}y, {total_mins:.1f}m)"
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
