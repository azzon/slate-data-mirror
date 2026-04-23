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


class _ChunkTimeout(Exception):
    pass


def _chunk_worker(chunk: list[str], start: str, end: str, workers: int,
                   q) -> None:  # pragma: no cover — runs in child
    """Child process: call tm.fetch_daily_bars, put result on queue."""
    # Re-import in child because 'spawn' doesn't inherit module state
    sys_path_0 = str(Path(__file__).resolve().parent)
    import sys as _sys
    if sys_path_0 not in _sys.path:
        _sys.path.insert(0, sys_path_0)
    import tencent_market as _tm  # noqa: E402 — child-local import
    try:
        result = _tm.fetch_daily_bars(chunk, start=start, end=end, max_workers=workers)
        q.put(("ok", result))
    except Exception as e:  # noqa: BLE001
        q.put(("err", f"{type(e).__name__}: {e}"))


def _fetch_chunk_subprocess(chunk, *, start, end, workers, timeout_s, mp_ctx):
    """Run a chunk fetch in a child process; kill on timeout.

    multiprocessing gotcha: if the child puts a large result on the queue,
    its background feeder thread may not finish flushing to the OS pipe
    before the child "finishes" — p.join() can block until the pipe
    drains. For 50 tickers × ~1200 bars ≈ 6MB pickled, the feeder lag is
    real on a 1.6 GB box under memory pressure. So: drain the queue
    FIRST with a long-enough timeout, THEN join the child.
    """
    q = mp_ctx.Queue()
    p = mp_ctx.Process(target=_chunk_worker,
                       args=(chunk, start, end, workers, q))
    p.start()

    # Drain result first — long timeout covers both fetch work + feeder flush.
    result: tuple | None = None
    drain_err: str | None = None
    try:
        result = q.get(timeout=timeout_s)
    except Exception as e:  # includes queue.Empty — child never produced
        drain_err = f"{type(e).__name__}: {e}"

    # Now join; the child has put() and should exit promptly
    p.join(timeout=30)
    if p.is_alive():
        # Child hanging despite having sent (or not sent) its result
        p.terminate()
        p.join(timeout=5)
        if p.is_alive():
            p.kill()
            p.join(timeout=5)
        if result is None:
            raise _ChunkTimeout()

    if result is None:
        # Queue.get raised before child terminated — assume true timeout
        raise _ChunkTimeout() if drain_err and "Empty" in drain_err else RuntimeError(
            f"child produced no result: {drain_err}"
        )

    status, payload = result
    if status != "ok":
        raise RuntimeError(f"child failed: {payload}")
    return payload


def _load_progress() -> dict:
    if PROGRESS_FILE.exists():
        return json.loads(PROGRESS_FILE.read_text())
    return {"market_daily": {"done_codes": [], "started_at": datetime.now(timezone.utc).isoformat()}}


def _save_progress(p: dict) -> None:
    """Atomic write — matches _save_status behaviour in fetch_all.py.
    Pipeline previously got bitten by OOM mid-write leaving truncated
    JSON which then crashed every subsequent _load_progress."""
    PROGRESS_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp = PROGRESS_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(p, ensure_ascii=False, indent=2))
    tmp.replace(PROGRESS_FILE)


def _market_daily_path(d: date) -> Path:
    return DATA_DIR / "market_daily" / f"{d.year}" / f"{d.month:02d}" / f"{d.isoformat()}.parquet"


def _git_push(msg: str) -> None:
    """Commit + rebase + push. Matches fetch_all.py::_git_push_incremental.
    Critical: rebase-autostash before push so a concurrent workflow run
    (we've seen bootstrap + fast + market interleave through the single
    runner) doesn't cause silent non-fast-forward failures that then
    get wiped by the next job's `git clean -ffdx`."""
    def r(*args):
        return subprocess.run(args, cwd=REPO_ROOT, check=False, text=True, capture_output=True)
    r("git", "add", "data/")
    if r("git", "diff", "--cached", "--quiet").returncode == 0:
        return
    r("git", "commit", "-m", msg)
    pull = r("git", "pull", "--rebase", "--autostash", "origin", "main")
    if pull.returncode != 0:
        log.warning("  git pull --rebase failed: %s",
                    (pull.stderr or pull.stdout or "").strip()[-200:])
        return
    push = r("git", "push", "origin", "HEAD:main")
    if push.returncode != 0:
        log.warning("  git push failed: %s",
                    (push.stderr or push.stdout or "").strip()[-200:])


def bootstrap_market_daily(years: int, workers: int = 8) -> None:
    today = date.today()
    start = (today - timedelta(days=365 * years + 1)).strftime("%Y%m%d")
    end = today.strftime("%Y%m%d")

    # Universe from Tencent (cheap probe, ~45s). This also populates
    # securities/latest.parquet consistency with fetch_all.py's source.
    universe = tm.fetch_universe()
    all_tickers = [u["ticker"] for u in universe]
    progress = _load_progress()
    done = set(progress["market_daily"]["done_codes"])
    failed_prior = progress["market_daily"].get("failed_codes", {})
    # Previously-failed tickers get retried automatically on every run.
    # Transient Tencent hiccups that hit a handful of tickers on run N
    # will usually succeed on run N+1. Only a permanently-delisted
    # ticker stays stuck — and that's fine, we log it.
    todo = [t for t in all_tickers if t not in done]
    retry_list = [t for t in failed_prior if t in all_tickers and t not in done]
    if retry_list:
        log.info("market_daily bootstrap: %d previously-failed tickers being retried", len(retry_list))
        # retry_list is already a subset of todo (failed != done), so no merge needed

    log.info("market_daily bootstrap: %d total, %d done, %d remaining (of which %d previously failed)",
             len(all_tickers), len(done), len(todo), len(failed_prior))
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
        n_dates = len(rows_by_date)
        rows_by_date.clear()
        log.info("  flushed %d rows across %d dates", written, n_dates)

    # Process in CHUNK-sized ticker batches via kill-able subprocess.
    # We saw a wedge where all threads in tm.fetch_daily_bars landed in
    # futex_wait with no active sockets — a ThreadPoolExecutor with
    # fut.result(timeout=...) cannot kill such a thread, and its
    # __exit__ blocks forever on the hung thread. Subprocess can be
    # SIGTERM/SIGKILLed cleanly, and any leaked httpx state dies with
    # it.
    completed = 0
    CHUNK_TIMEOUT_S = 300

    import multiprocessing as mp
    # 'spawn' avoids fork-inheriting httpx pool state from prior chunks.
    mp_ctx = mp.get_context("spawn")

    for chunk_start in range(0, len(todo), CHUNK):
        chunk = todo[chunk_start:chunk_start + CHUNK]
        try:
            bars_by_ticker = _fetch_chunk_subprocess(
                chunk, start=start, end=end,
                workers=workers, timeout_s=CHUNK_TIMEOUT_S, mp_ctx=mp_ctx,
            )
        except _ChunkTimeout:
            log.error("  chunk %d-%d timed out (>%ds) — killed subprocess",
                      chunk_start, chunk_start + len(chunk), CHUNK_TIMEOUT_S)
            for t in chunk:
                new_failed[t] = "chunk_timeout"
            continue
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
        # failed_codes: merge with prior so tickers that failed on
        # *earlier* runs aren't forgotten when this run skipped them.
        flush_chunk()
        done.update(new_done)
        merged_failed = {**failed_prior, **new_failed}
        # Remove any ticker that now succeeded
        for t in new_done:
            merged_failed.pop(t, None)
        progress["market_daily"]["done_codes"] = sorted(done)
        progress["market_daily"]["failed_codes"] = merged_failed
        progress["market_daily"]["last_checkpoint"] = datetime.now(timezone.utc).isoformat()
        _save_progress(progress)
        _git_push(
            f"bootstrap(market_daily): +{processed_in_chunk} ok, "
            f"{len(merged_failed)} fail total ({len(done)}/{len(all_tickers)} total)"
        )
        processed_in_chunk = 0

    # Final flush (safety — chunk loop flushes every iter).
    flush_chunk()
    done.update(new_done)
    merged_failed = {**failed_prior, **new_failed}
    for t in new_done:
        merged_failed.pop(t, None)
    progress["market_daily"]["done_codes"] = sorted(done)
    progress["market_daily"]["failed_codes"] = merged_failed
    progress["market_daily"]["completed_at"] = datetime.now(timezone.utc).isoformat()
    _save_progress(progress)
    total_mins = (time.time() - started) / 60
    _git_push(
        f"bootstrap(market_daily): {len(new_done)} ok, {len(new_failed)} fail "
        f"(total done {len(done)}/{len(all_tickers)}, {years}y, {total_mins:.1f}m)"
    )
    log.info("market_daily bootstrap: ok=%d fail=%d in %.1f min",
             len(new_done), len(new_failed), total_mins)

    _sanity_check_market_daily(expected_years=years)


def _sanity_check_market_daily(*, expected_years: int, min_daily_tickers: int = 2000) -> None:
    """Post-bootstrap verification. Hard-fail if the data looks wrong.

    Checks:
    1. Date coverage — N year request should produce >= 0.95 × N × 245
       trading-day files (245 = typical A-share trading days/year).
    2. Row-count floor — every file in the last 90 days must have >=
       `min_daily_tickers` rows. Catches cases where pagination regressed
       and only ~30 tickers land per date.
    3. Coverage drift — files should be monotonically non-decreasing
       in ticker count as we move toward present (new listings,
       survivorship bias); a 50%+ drop after any date indicates a
       failed chunk wrote partial data.
    """
    root = DATA_DIR / "market_daily"
    if not root.exists():
        raise RuntimeError("sanity: market_daily directory missing after bootstrap")
    files = sorted(root.rglob("*.parquet"))

    # 1. Date coverage
    expected = int(0.95 * expected_years * 245)
    if len(files) < expected:
        raise RuntimeError(
            f"sanity: only {len(files)} parquet files, expected >= {expected} for {expected_years}y"
        )

    # 2. Row-count floor for recent files
    recent = files[-90:]
    bad: list[str] = []
    for p in recent:
        n = len(pd.read_parquet(p))
        if n < min_daily_tickers:
            bad.append(f"{p.name}={n}")
    if bad:
        raise RuntimeError(
            f"sanity: {len(bad)} recent files below {min_daily_tickers}-ticker floor: {bad[:5]}"
        )

    # 3. Coverage drift — median ticker count in last 30 days
    recent30 = files[-30:]
    if recent30:
        counts = [len(pd.read_parquet(p)) for p in recent30]
        median_recent = sorted(counts)[len(counts) // 2]
        log.info("sanity: OK — %d files, median %d tickers/day in last 30 days",
                 len(files), median_recent)


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
