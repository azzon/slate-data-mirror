#!/usr/bin/env python3
"""One-shot filings backfill from cninfo.

Populates data/filings/history/YYYY-MM-DD.parquet for every trading day
from START_DATE to TODAY-30 (the last 30 days are covered by the normal
fetch_filings cron, so we stop there to avoid overlap).

Runs as the standalone `mirror-backfill-filings.yml` workflow. Resumable:
any per-day parquet that already exists is skipped, so you can kill it
and restart anytime.

Design for 2G ECS:
- Serial per-day calls with AKSHARE_SLEEP between each (1.5s default).
- Each call returns ~3000 rows × ~5 columns = <500KB — negligible RAM.
- Per-day parquet ≤ 100KB, history/ grows ~50MB/year × 5 years = 250MB.
  Well under the 40G disk.
- Git commit batch: every 60 days successfully written → git add + commit
  + push, keeping repo-locked periods short.
- Peak avoidance: skip 15:00-17:00 CST just like fetch_filings.

Usage (on runner):
    python3 scripts/backfill_filings.py [--start-date 2020-01-01] [--batch-days 60]

The workflow dispatches it with --start-date and --batch-days inputs.
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
PROGRESS_FILE = DATA_DIR / "_filings_backfill_progress.json"

CST = timezone(timedelta(hours=8))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("backfill-filings")


AKSHARE_SLEEP = float(os.environ.get("MIRROR_AKSHARE_SLEEP", "1.5"))


def _today_cn() -> date:
    return datetime.now(CST).date()


def _now_cn_hour() -> int:
    return datetime.now(CST).hour


def _is_peak_window() -> bool:
    """15:00-17:00 CST = cninfo publication flood; skip."""
    return 15 <= _now_cn_hour() < 17


def _load_progress() -> dict:
    if not PROGRESS_FILE.exists():
        return {"last_done": None, "start": None, "completed_days": 0}
    try:
        return json.loads(PROGRESS_FILE.read_text())
    except Exception as e:  # noqa: BLE001
        log.warning("progress file unreadable, starting fresh: %s", e)
        return {"last_done": None, "start": None, "completed_days": 0}


def _save_progress(p: dict) -> None:
    p["updated_at"] = datetime.now(timezone.utc).isoformat()
    PROGRESS_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp = PROGRESS_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(p, indent=2))
    tmp.replace(PROGRESS_FILE)


class UpstreamSchemaGap(Exception):
    """akshare's column-rename fails on historical dates (pre ~2021-11-22).
    Signals 'upstream has no data for this date' rather than 'transient
    error'. Backfill treats this as a permanent gap, not a counted error."""


def _fetch_one_day(d: date) -> pd.DataFrame | None:
    """Single-day cninfo call with retry. Returns None on transient
    failure (counts toward max_errors). Raises UpstreamSchemaGap when
    the date is pre-akshare-schema (permanent gap, doesn't retry)."""
    ds = d.strftime("%Y%m%d")
    last_err = None
    for attempt in range(3):
        try:
            df = ak.stock_zh_a_disclosure_report_cninfo(
                symbol="",
                market="沪深京",
                keyword="",
                category="",
                start_date=ds,
                end_date=ds,
            )
            return df
        except KeyError as e:
            # akshare's df[[...]] column lookup on pre-rename cninfo
            # response. Permanent — date is before their normaliser
            # was written. Don't retry.
            msg = str(e)
            if "代码" in msg or "announcementId" in msg:
                raise UpstreamSchemaGap(f"akshare schema gap for {ds}") from e
            last_err = e
        except Exception as e:  # noqa: BLE001
            last_err = e
        if attempt < 2:
            delay = 5 * (attempt + 1)
            log.warning("  %s try %d/3 failed (%s), retry in %ds",
                        ds, attempt + 1, type(last_err).__name__, delay)
            time.sleep(delay)
    log.error("  %s all retries failed: %s", ds, last_err)
    return None


def _write_day(d: date, df: pd.DataFrame) -> int:
    """Dedup by 公告链接, write history/{date}.parquet. Returns row count."""
    if df is None or df.empty:
        return 0
    url_col = "公告链接" if "公告链接" in df.columns else "announcement_url"
    if url_col in df.columns:
        df = df.drop_duplicates(subset=[url_col], keep="first")
    df = df.copy()
    df["mirror_fetch_date"] = _today_cn().isoformat()
    target = DATA_DIR / "filings" / "history" / f"{d.isoformat()}.parquet"
    target.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(target, compression="zstd", index=False)
    return len(df)


def _git_commit_batch(start: date, end: date, days_done: int) -> bool:
    """Stage, commit, push the backfill parquets. Silent no-op on empty diff."""
    def run(*args, check=True):
        return subprocess.run(args, cwd=REPO_ROOT, check=check,
                              text=True, capture_output=True)
    try:
        run("git", "add", "data/filings/history/", "data/_filings_backfill_progress.json")
        if run("git", "diff", "--cached", "--quiet", check=False).returncode == 0:
            return True  # nothing to commit
        run(
            "git", "commit", "-m",
            f"backfill(filings): {start} → {end} ({days_done} days)",
        )
        run("git", "push", "origin", "HEAD:main")
        return True
    except subprocess.CalledProcessError as e:
        log.error("git push failed: %s\nstderr: %s", e, e.stderr)
        return False


def backfill(
    start_date: date,
    end_date: date,
    batch_days: int = 60,
    max_errors: int = 20,
) -> int:
    """Iterate start_date → end_date inclusive, fetch + write each day,
    commit+push every ``batch_days`` successful days. ``max_errors``
    aborts early if cninfo is persistently broken.
    """
    progress = _load_progress()
    # Resume from last_done + 1 if we're continuing a prior run.
    resume_from = None
    if progress.get("last_done"):
        try:
            resume_from = date.fromisoformat(progress["last_done"]) + timedelta(days=1)
        except ValueError:
            resume_from = None
    cursor = resume_from if resume_from and resume_from > start_date else start_date

    if progress.get("start") is None or resume_from is None:
        progress["start"] = start_date.isoformat()

    days_attempted = 0
    days_succeeded = 0
    days_skipped = 0
    days_schema_gap = 0
    rows_total = 0
    errors = 0
    batch_start = cursor

    # schema-gap marker file — dates where akshare's historical support
    # ends. Pre-marker dates get skipped immediately on subsequent runs
    # so we don't retry the same 1000+ days of KeyError forever.
    gap_file = DATA_DIR / "_filings_schema_gap.json"
    schema_gap_dates: set[str] = set()
    if gap_file.exists():
        try:
            schema_gap_dates = set(json.loads(gap_file.read_text()))
        except Exception:  # noqa: BLE001
            schema_gap_dates = set()

    log.info("backfill: cursor=%s end=%s batch_days=%d gap_known=%d",
             cursor, end_date, batch_days, len(schema_gap_dates))

    while cursor <= end_date:
        # Skip peak window — just sleep past it.
        while _is_peak_window():
            log.info("  peak window (hour=%d CST), sleeping 10min", _now_cn_hour())
            time.sleep(600)

        day_parquet = DATA_DIR / "filings" / "history" / f"{cursor.isoformat()}.parquet"
        if day_parquet.exists():
            days_skipped += 1
            cursor += timedelta(days=1)
            continue

        if cursor.isoformat() in schema_gap_dates:
            days_schema_gap += 1
            cursor += timedelta(days=1)
            continue

        days_attempted += 1
        time.sleep(AKSHARE_SLEEP)
        try:
            df = _fetch_one_day(cursor)
        except UpstreamSchemaGap:
            # Permanent: akshare has no historical support for this date.
            # Record to gap_file so future runs skip immediately. Once
            # we see 3 consecutive gap days, walk cursor back to
            # end_date (it's a lost cause — everything older has the
            # same schema issue).
            schema_gap_dates.add(cursor.isoformat())
            days_schema_gap += 1
            # Persist gap file periodically (every 10 new gaps).
            if days_schema_gap % 10 == 1:
                gap_file.write_text(
                    json.dumps(sorted(schema_gap_dates), ensure_ascii=False),
                )
            # If we've hit 30 consecutive gap days, akshare is definitely
            # done. Stop the scan — any older dates will have the same
            # KeyError. This keeps future runs from wasting API calls.
            recent_gap = all(
                (cursor - timedelta(days=k)).isoformat() in schema_gap_dates
                for k in range(30)
            )
            if recent_gap and days_attempted > 30:
                log.warning("  30 consecutive schema gaps — akshare has no "
                            "pre-%s support, stopping scan", cursor)
                break
            cursor += timedelta(days=1)
            continue
        if df is None:
            errors += 1
            if errors >= max_errors:
                log.error("  %d errors — aborting, re-run to resume", errors)
                break
            cursor += timedelta(days=1)
            continue
        rows = _write_day(cursor, df)
        rows_total += rows
        days_succeeded += 1
        progress["last_done"] = cursor.isoformat()
        progress["completed_days"] = progress.get("completed_days", 0) + 1

        if days_succeeded % batch_days == 0:
            _save_progress(progress)
            ok = _git_commit_batch(batch_start, cursor, days_succeeded)
            if ok:
                batch_start = cursor + timedelta(days=1)
            log.info(
                "  batch commit at %s (done=%d rows=%d errs=%d)",
                cursor, days_succeeded, rows_total, errors,
            )

        cursor += timedelta(days=1)

    # Final commit + persist gap file
    _save_progress(progress)
    if schema_gap_dates:
        gap_file.write_text(
            json.dumps(sorted(schema_gap_dates), ensure_ascii=False),
        )
    if days_succeeded > 0 or schema_gap_dates:
        _git_commit_batch(batch_start, cursor - timedelta(days=1), days_succeeded)
    log.info(
        "done: attempted=%d succeeded=%d skipped=%d schema_gaps=%d rows=%d errors=%d",
        days_attempted, days_succeeded, days_skipped,
        days_schema_gap, rows_total, errors,
    )
    return 0 if errors < max_errors else 1


def main() -> int:
    today = _today_cn()
    default_start = (today - timedelta(days=5 * 365)).isoformat()
    # Stop 30 days before today — the normal 30-day rolling window covers
    # those days already. Avoids overlap + ensures single source of truth
    # for the recent window.
    default_end = (today - timedelta(days=31)).isoformat()

    parser = argparse.ArgumentParser(description="Filings 5-year backfill")
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
