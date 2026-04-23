#!/usr/bin/env python3
"""One-shot LHB (龙虎榜) history backfill.

Populates data/lhb/history/YYYY-MM-DD.parquet for every trading day
from START_DATE to TODAY-30 (last 30d covered by fetch_lhb cron).

Resumable via data/_lhb_backfill_progress.json. Skips dates with
existing parquets. Peak-skip 15:00-17:00 CST.

Same design as backfill_filings.py — per-day serial call, 60-day
git commit batches.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import subprocess
import sys
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import akshare as ak
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = REPO_ROOT / "data"
PROGRESS_FILE = DATA_DIR / "_lhb_backfill_progress.json"

CST = timezone(timedelta(hours=8))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("backfill-lhb")


AKSHARE_SLEEP = float(os.environ.get("MIRROR_AKSHARE_SLEEP", "1.5"))


def _today_cn() -> date:
    return datetime.now(CST).date()


def _is_peak_window() -> bool:
    hour = datetime.now(CST).hour
    return 15 <= hour < 17


def _load_progress() -> dict:
    if not PROGRESS_FILE.exists():
        return {"last_done": None, "completed_days": 0}
    try:
        return json.loads(PROGRESS_FILE.read_text())
    except Exception:  # noqa: BLE001
        return {"last_done": None, "completed_days": 0}


def _save_progress(p: dict) -> None:
    p["updated_at"] = datetime.now(timezone.utc).isoformat()
    PROGRESS_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp = PROGRESS_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(p, indent=2))
    tmp.replace(PROGRESS_FILE)


def _fetch_one_day(d: date) -> pd.DataFrame | None:
    ds = d.strftime("%Y%m%d")
    last_err = None
    for attempt in range(3):
        try:
            df = ak.stock_lhb_detail_em(start_date=ds, end_date=ds)
            return df
        except Exception as e:  # noqa: BLE001
            last_err = e
            if attempt < 2:
                delay = 5 * (attempt + 1)
                log.warning("  %s try %d/3 failed (%s), retry in %ds",
                            ds, attempt + 1, type(e).__name__, delay)
                time.sleep(delay)
    log.error("  %s all retries failed: %s", ds, last_err)
    return None


def _git_commit_batch(start: date, end: date, days_done: int) -> bool:
    def run(*args, check=True):
        return subprocess.run(args, cwd=REPO_ROOT, check=check,
                              text=True, capture_output=True)
    try:
        run("git", "add", "data/lhb/history/", "data/_lhb_backfill_progress.json")
        if run("git", "diff", "--cached", "--quiet", check=False).returncode == 0:
            return True
        run("git", "commit", "-m",
            f"backfill(lhb): {start} → {end} ({days_done} days)")
        run("git", "push", "origin", "HEAD:main")
        return True
    except subprocess.CalledProcessError as e:
        log.error("git push failed: %s\nstderr: %s", e, e.stderr)
        return False


def backfill(start_date: date, end_date: date, *,
             batch_days: int = 60, max_errors: int = 20) -> int:
    progress = _load_progress()
    resume_from = None
    if progress.get("last_done"):
        try:
            resume_from = date.fromisoformat(progress["last_done"]) + timedelta(days=1)
        except ValueError:
            resume_from = None
    cursor = resume_from if resume_from and resume_from > start_date else start_date

    days_attempted = 0
    days_succeeded = 0
    days_skipped = 0
    days_empty = 0
    rows_total = 0
    errors = 0
    batch_start = cursor

    log.info("backfill-lhb: cursor=%s end=%s", cursor, end_date)

    while cursor <= end_date:
        while _is_peak_window():
            log.info("  peak (hour=%d CST), sleeping 10min",
                     datetime.now(CST).hour)
            time.sleep(600)

        day_parquet = DATA_DIR / "lhb" / "history" / f"{cursor.isoformat()}.parquet"
        if day_parquet.exists():
            days_skipped += 1
            cursor += timedelta(days=1)
            continue

        days_attempted += 1
        time.sleep(AKSHARE_SLEEP)
        df = _fetch_one_day(cursor)
        if df is None:
            errors += 1
            if errors >= max_errors:
                log.error("  %d errors — aborting, re-run to resume", errors)
                break
            cursor += timedelta(days=1)
            continue
        if df.empty:
            # Empty = non-trading day (holiday / weekend). Still write
            # an empty marker so next run doesn't retry.
            day_parquet.parent.mkdir(parents=True, exist_ok=True)
            df.to_parquet(day_parquet, compression="zstd", index=False)
            days_empty += 1
        else:
            df.to_parquet(day_parquet, compression="zstd", index=False)
            rows_total += len(df)
            days_succeeded += 1

        progress["last_done"] = cursor.isoformat()
        progress["completed_days"] = progress.get("completed_days", 0) + 1

        if (days_succeeded + days_empty) % batch_days == 0 and (days_succeeded + days_empty) > 0:
            _save_progress(progress)
            ok = _git_commit_batch(batch_start, cursor, days_succeeded)
            if ok:
                batch_start = cursor + timedelta(days=1)
            log.info("  batch commit at %s (ok=%d empty=%d rows=%d)",
                     cursor, days_succeeded, days_empty, rows_total)

        cursor += timedelta(days=1)

    _save_progress(progress)
    if days_succeeded + days_empty > 0:
        _git_commit_batch(batch_start, cursor - timedelta(days=1), days_succeeded)
    log.info(
        "done: attempted=%d ok=%d empty=%d skipped=%d rows=%d errors=%d",
        days_attempted, days_succeeded, days_empty,
        days_skipped, rows_total, errors,
    )
    return 0 if errors < max_errors else 1


def main() -> int:
    today = _today_cn()
    default_start = (today - timedelta(days=5 * 365)).isoformat()
    default_end = (today - timedelta(days=31)).isoformat()

    parser = argparse.ArgumentParser(description="LHB 5-year backfill")
    parser.add_argument("--start-date", default=default_start)
    parser.add_argument("--end-date", default=default_end)
    parser.add_argument("--batch-days", type=int, default=60)
    parser.add_argument("--max-errors", type=int, default=20)
    args = parser.parse_args()

    return backfill(
        start_date=date.fromisoformat(args.start_date),
        end_date=date.fromisoformat(args.end_date),
        batch_days=args.batch_days,
        max_errors=args.max_errors,
    )


if __name__ == "__main__":
    sys.exit(main())
