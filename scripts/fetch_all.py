#!/usr/bin/env python3
"""Fetch mirrored AKShare endpoints, write Parquet, commit per-endpoint.

Runs on a self-hosted GitHub Actions runner inside mainland China.

Core principles:
- **Historical append, not spot overwrite.** market_daily stores true OHLCV
  per ticker per trade_date; one day = one parquet file under
  market_daily/YYYY/MM/YYYY-MM-DD.parquet. We never overwrite a past
  day's file.
- **Gap self-healing.** Every run scans the last 30 calendar days and
  auto-backfills any missing trade dates that AKShare can still serve.
  One bad day doesn't become permanent data loss.
- **Cadence matches data rhythm.** Post-close data runs once/day at 16:00 CST;
  financials/concepts/industries/shareholders run once/week (quarterly data);
  news runs 4x/day.
- **Idempotent per endpoint.** Re-running the same hour is a no-op.
  Re-running with --force re-fetches and overwrites current-day files.
- **Incremental push.** Each successful endpoint commits + pushes its own
  files, so a late failure never loses earlier work.
- **Transient errors auto-retry.** RemoteDisconnected / 5xx wrapped in
  exponential backoff (3 tries, 5s/15s/45s).
"""

from __future__ import annotations

import json
import logging
import os
import subprocess
import sys
import time
import traceback
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from http.client import RemoteDisconnected
from pathlib import Path
from typing import Callable
from urllib.error import URLError

import akshare as ak
import pandas as pd
import requests

# Tencent gtimg is our primary source for market_daily OHLCV — after
# Eastmoney IP-banned us for the 16-worker akshare bulk run, we found
# Tencent's qt/fqkline endpoints are independently rate-limited and
# more tolerant. See project memory: project_akshare_rate_limit.md.
sys.path.insert(0, str(Path(__file__).resolve().parent))
import tencent_market as tm  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = REPO_ROOT / "data"
STATUS_FILE = DATA_DIR / "_status.json"
PERMAFAIL_FILE = DATA_DIR / "_market_daily_permafail.json"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("mirror")

CST = timezone(timedelta(hours=8))

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


def _today_cn() -> date:
    return datetime.now(CST).date()


def _retry(fn: Callable, *args, tries: int = 5, base_delay: float = 5.0, **kw):
    """Exponential backoff for transient upstream errors.

    Wave 13: tries 3→5. Mirror-slow audit showed concepts/industries
    at ~25% success rate due to RemoteDisconnected mid-sweep; 3 retries
    in a 3+9+27s window weren't enough to ride through upstream's
    60-90s mini-outages (common from ~19:30 UTC = 03:30 CST peak).
    5 retries with 3^n × 5s = 5s / 15s / 45s / 135s / 405s ≈ 10 min
    of total retry window. If upstream is dead for 10+ min, that's a
    real incident and failing fast is correct.
    """
    last = None
    for attempt in range(1, tries + 1):
        try:
            return fn(*args, **kw)
        except TRANSIENT as e:
            last = e
            if attempt == tries:
                raise
            delay = base_delay * (3 ** (attempt - 1))
            log.warning("  transient %s (try %d/%d) — retrying in %ds",
                        type(e).__name__, attempt, tries, int(delay))
            time.sleep(delay)
    raise last  # unreachable


# Per-call throttle for AKShare endpoints. Ultra-conservative: we'd
# rather take 3 hours on a 5500-ticker sweep than take 30s and burn
# another 24h in an Eastmoney ban. Caller can override per endpoint.
AKSHARE_SLEEP = float(os.environ.get("MIRROR_AKSHARE_SLEEP", "1.5"))


def _ak_call(fn: Callable, *args, **kw):
    """Retry-wrapped AKShare call with built-in throttle. tries=5 post-W13."""
    time.sleep(AKSHARE_SLEEP)
    return _retry(fn, *args, tries=5, base_delay=10.0, **kw)


def _write_parquet(df: pd.DataFrame, rel: str) -> int:
    path = DATA_DIR / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, compression="snappy", index=False)
    size_kb = path.stat().st_size // 1024
    log.info("  wrote %s (rows=%d, %dKB)", rel, len(df), size_kb)
    return size_kb


def _market_daily_path(d: date) -> Path:
    return DATA_DIR / "market_daily" / f"{d.year}" / f"{d.month:02d}" / f"{d.isoformat()}.parquet"


def _load_market_daily_permafail() -> set[str]:
    if not PERMAFAIL_FILE.exists():
        return set()
    try:
        return set(json.loads(PERMAFAIL_FILE.read_text()))
    except Exception:  # noqa: BLE001
        return set()


def _save_market_daily_permafail(dates: set[str]) -> None:
    """Atomically persist the set of dates the upstream source has
    permanently refused data for (e.g. holidays > 30 days old that
    Tencent's qfqday window no longer returns). Prevents the gap scan
    from re-attempting them every 4h forever."""
    PERMAFAIL_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp = PERMAFAIL_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(sorted(dates), ensure_ascii=False))
    tmp.replace(PERMAFAIL_FILE)


# ── per-endpoint fetchers ───────────────────────────────────────────────


SECURITIES_FLOOR = 4500  # A-share market has ~5500 (SH+SZ+BJ); <4500 = broken


def fetch_securities() -> dict:
    """Securities master list — the A-share universe reference.

    Prefer AKShare `stock_info_a_code_name` because it includes BJ
    (Beijing Stock Exchange, ~308 tickers with prefix 4/8/92) which
    Tencent's gtimg universe probe does NOT cover. Tencent is a fallback
    only if AKShare is unreachable — accept missing BJ in that degraded
    case but log it.

    Refuses to overwrite the existing parquet with < SECURITIES_FLOOR
    rows to prevent a single-source flake from wiping a healthy file.
    """
    chosen_df: pd.DataFrame | None = None
    source = None

    # Primary: AKShare (covers BJ)
    try:
        df = _ak_call(ak.stock_info_a_code_name)
        if df is not None and len(df) >= SECURITIES_FLOOR:
            chosen_df = df[["code", "name"]].copy()
            source = "akshare"
        elif df is not None:
            log.warning("  akshare universe only %d rows (< %d) — trying Tencent",
                        len(df), SECURITIES_FLOOR)
    except Exception as e:  # noqa: BLE001
        log.warning("  akshare universe failed (%s) — trying Tencent", e)

    # Fallback: Tencent (no BJ — degraded)
    if chosen_df is None:
        try:
            universe = tm.fetch_universe()
            if universe and len(universe) >= SECURITIES_FLOOR:
                chosen_df = pd.DataFrame([
                    {"code": u["ticker"].split(".")[0], "name": u["name"]}
                    for u in universe
                ])
                source = "tencent_fallback_no_bj"
                log.warning("  using Tencent fallback — BJ tickers will be missing")
        except Exception as e:  # noqa: BLE001
            log.warning("  tencent universe also failed (%s)", e)

    if chosen_df is None:
        raise RuntimeError(
            f"both sources returned < {SECURITIES_FLOOR} rows — refusing to clobber"
        )

    kb = _write_parquet(chosen_df, "securities/latest.parquet")
    return {"rows": len(chosen_df), "size_kb": kb, "source": source}


def _fetch_one_day_bars(d: date) -> pd.DataFrame | None:
    """Fetch OHLCV for all A-shares on a specific trade date via Tencent gtimg.

    Tencent's qt.gtimg.cn / web.ifzq.gtimg.cn endpoints are independently
    rate-limited from Eastmoney (which ban-hammered us for an akshare bulk
    run). Tencent also returns 5 years of qfq history per call so this
    function is reused for bootstrap — for a single-day fetch we just
    filter to the target date after the call.

    Also stamps `market_cap` from the Tencent universe probe's `total_mv_yi`
    (in 亿元, converted to CNY) so the consumer side doesn't need a second
    Tencent round-trip. This is TODAY's market cap for every ticker; for
    backfill days we leave it NULL because Tencent's qfq-history endpoint
    doesn't emit historical market cap.
    """
    # Universe via Tencent probe — carries total_mv_yi so we can stamp
    # today's market_cap into the parquet.
    universe = tm.fetch_universe()
    tickers = [u["ticker"] for u in universe]
    # Build a ticker → market_cap_cny map for today's fetch only.
    today = _today_cn()
    cap_by_ticker: dict[str, float | None] = {}
    if d == today:
        for u in universe:
            mv_yi = u.get("total_mv_yi")
            if mv_yi is not None:
                cap_by_ticker[u["ticker"]] = mv_yi * 1e8  # 亿元 → 元
    start = end = d.strftime("%Y%m%d")
    bars_by_ticker = tm.fetch_daily_bars(tickers, start=start, end=end, max_workers=8)

    rows: list[dict] = []
    for ticker, bars in bars_by_ticker.items():
        code = ticker.split(".")[0]
        for bar in bars:
            if bar["trade_date"] != d.isoformat():
                continue
            rows.append({
                "code": code, "ticker": ticker,
                "trade_date": bar["trade_date"],
                "open": bar["open"], "close": bar["close"],
                "high": bar["high"], "low": bar["low"],
                "volume": bar["volume"],
                "market_cap": cap_by_ticker.get(ticker),  # None for backfill days
            })
    if not rows:
        return None
    return pd.DataFrame(rows)


def _is_trading_day(d: date) -> bool:
    """Skip weekends. Holidays: we try anyway — AKShare returns empty for
    exchange holidays and _fetch_one_day_bars returns None, which the caller
    records as 'no bars for this date' without flagging it a failure."""
    return d.weekday() < 5


def _recent_row_median(today: date, *, window_days: int = 30) -> int:
    """Median tickers/day in recent parquets. Used as the adaptive
    floor — a hard-coded ROW_COUNT_FLOOR misbehaves on half-day sessions
    and pre-2020 history where the universe was smaller.

    Returns 0 if we can't sample (fresh clone). Caller should combine
    with a sane ABSOLUTE minimum so the first file written doesn't
    lock itself out.
    """
    counts: list[int] = []
    back = 1
    sampled = 0
    # Look back further to find enough samples
    while sampled < 10 and back <= 180:
        d = today - timedelta(days=back)
        back += 1
        if not _is_trading_day(d):
            continue
        p = _market_daily_path(d)
        if not p.exists():
            continue
        try:
            counts.append(len(pd.read_parquet(p)))
            sampled += 1
        except Exception:  # noqa: BLE001
            continue
    if not counts:
        return 0
    return sorted(counts)[len(counts) // 2]


def _merge_market_daily(df: pd.DataFrame, d: date) -> int:
    """Write `df` to today's parquet, merging with any existing file.

    Merge semantics — "new wins only when new is valid":
      * Drop rows in the new frame where `close` is null/NaN (broken fetch).
      * Concat prior + cleaned-new.
      * Dedupe on (ticker, trade_date) keeping LAST — now safe because
        any null-close new row was already stripped, so "last" is always
        a real row.

    Without the pre-strip, a Tencent CDN hiccup that returns `close=NaN`
    for a ticker would overwrite a healthy prior row. With it, prior
    stays until a real new close arrives.
    """
    path = _market_daily_path(d)
    path.parent.mkdir(parents=True, exist_ok=True)
    # Strip rows with null close BEFORE merge — broken rows must never
    # clobber healthy prior data via keep="last".
    df = df.dropna(subset=["close"])
    if df.empty:
        log.warning("  %s: new frame has zero rows with non-null close — skip write", d)
        return 0
    if path.exists():
        try:
            prior = pd.read_parquet(path)
            merged = pd.concat([prior, df], ignore_index=True)
            merged = merged.drop_duplicates(subset=["ticker", "trade_date"], keep="last")
            merged.to_parquet(path, compression="snappy", index=False)
            log.info("  merged %s (prior=%d, new=%d, total=%d)",
                     d, len(prior), len(df), len(merged))
            return len(merged)
        except Exception as e:  # noqa: BLE001
            log.warning("  merge read failed for %s (%s) — writing fresh", d, e)
    df.to_parquet(path, compression="snappy", index=False)
    log.info("  wrote %s (rows=%d, %dKB)",
             path.relative_to(DATA_DIR.parent),
             len(df), path.stat().st_size // 1024)
    return len(df)


def fetch_market_daily() -> dict:
    """Append today's OHLCV (post-close) + backfill any missing trading day.

    Responsibilities:
    1. If today's file doesn't exist and it's after 16:00 CST (post-close),
       fetch today.
    2. Scan the full history window (default 5 years) for any trading day
       whose parquet is missing and backfill it. Capped at MAX_GAP_FILL per
       run so a long outage doesn't produce an 8-hour workflow.
    3. Re-fetch files whose row count is < 0.5 × recent-median. Caught
       partial writes without over-reacting on genuinely thin days (half-
       day sessions, early history).

    We budget a bounded number of gap fills per run. A runner-lost-comms
    incident that leaves 30 days unfilled won't monopolise one workflow
    run — subsequent 4h cron triggers will chip away until healed.
    """
    today = _today_cn()
    now = datetime.now(CST)
    # Bounded per-run gap-fill budget. One cron pass shouldn't spend
    # more than ~20 date-fetches even if many dates are missing; the
    # next cron will chip away at the remainder. 5y history window.
    MAX_GAP_FILL = 20
    HISTORY_DAYS = 365 * 5 + 1

    # Adaptive floor: half of the recent median ticker count, or 500 as
    # an absolute minimum on fresh clones where no median exists.
    recent_median = _recent_row_median(today)
    floor = max(500, int(recent_median * 0.5)) if recent_median else 500

    pending: list[date] = []

    # Today — only after 16:00 CST to ensure close prices are final.
    # If today's file already exists but is below the recent-median floor
    # (the 16:30 run wrote ~4400 while 5200 tickers eventually publish by
    # 17:00), re-fetch to merge the late publishers in. Without this,
    # late-publisher tickers were permanently lost because mere
    # `exists()` short-circuited the 17:30 straggler run.
    if _is_trading_day(today) and now.hour >= 16:
        today_path = _market_daily_path(today)
        if not today_path.exists():
            pending.append(today)
        elif recent_median:
            try:
                n_today = len(pd.read_parquet(today_path))
                if n_today < int(recent_median * 0.95):
                    log.info("  today %s has %d rows (< 95%% of median %d) — re-fetching to merge",
                             today, n_today, recent_median)
                    pending.append(today)
            except Exception:  # noqa: BLE001
                pass

    # Full-history gap scan (bounded, with permanent-fail memory)
    permafail = _load_market_daily_permafail()
    skipped_permafail = 0
    for back in range(1, HISTORY_DAYS + 1):
        d = today - timedelta(days=back)
        if not _is_trading_day(d):
            continue
        d_iso = d.isoformat()
        if d_iso in permafail:
            skipped_permafail += 1
            continue
        path = _market_daily_path(d)
        if not path.exists():
            pending.append(d)
        else:
            # Detect partial writes using adaptive floor
            try:
                if path.stat().st_size < 40_000:
                    n = len(pd.read_parquet(path))
                    if n < floor:
                        log.info("  refilling partial %s (%d rows < %d floor)",
                                 d, n, floor)
                        pending.append(d)
            except Exception:  # noqa: BLE001
                pending.append(d)
        if len(pending) >= MAX_GAP_FILL:
            break

    if not pending:
        note = "no pending dates"
        if skipped_permafail:
            note += f" ({skipped_permafail} permafail skipped)"
        return {"rows": 0, "note": note, "no_work": True}

    pending.sort(reverse=True)
    log.info("  market_daily pending: %d (sample: %s), floor=%d",
             len(pending), [d.isoformat() for d in pending[:5]], floor)

    total_rows = 0
    filled = []
    new_permafails: list[str] = []
    for d in pending:
        log.info("  fetching market_daily for %s", d)
        df = _fetch_one_day_bars(d)
        if df is None or df.empty:
            # Permanently record this date as "source has no data here"
            # so we don't keep pounding it every run. Trading-day holidays
            # past 30 days never materialise so treat them as permafail.
            if (today - d).days > 30:
                new_permafails.append(d.isoformat())
            log.info("    (no data — likely holiday or pre-publish)")
            continue
        df["trade_date"] = d.isoformat()
        # Sanity: if upstream returned fewer rows than half the recent
        # median, do NOT write — we'd corrupt otherwise-healthy data on
        # transient CDN issues. Let the next 4h run retry.
        if recent_median and len(df) < max(500, int(recent_median * 0.5)):
            log.warning("  rejected %s: only %d rows (median=%d); retry next run",
                        d, len(df), recent_median)
            continue
        rows_written = _merge_market_daily(df, d)
        total_rows += rows_written
        filled.append(d.isoformat())

    if new_permafails:
        _save_market_daily_permafail(permafail | set(new_permafails))

    # no_work=True tells run_once to NOT increment fail_streak when
    # we legitimately had no new data (weekend early Saturday runs,
    # or a single pending date that was a pre-publish today-probe and
    # returned empty — not a fetch failure).
    no_work = (total_rows == 0 and not filled)
    return {
        "rows": total_rows,
        "filled_dates": filled,
        "pending_total": len(pending),
        "budget": MAX_GAP_FILL,
        "no_work": no_work,
    }


# Memory-watermark: trigger an early flush when free memory falls
# below this much (bytes). 200 MB headroom on the 2 GB ECS gives
# pandas enough room to do the final concat without OOM.
_LOW_MEM_THRESHOLD_BYTES = 200 * 1024 * 1024


def _available_memory_bytes() -> int | None:
    """Read MemAvailable from /proc/meminfo. Returns None on non-Linux."""
    try:
        with open("/proc/meminfo") as f:
            for line in f:
                if line.startswith("MemAvailable:"):
                    kb = int(line.split()[1])
                    return kb * 1024
    except OSError:
        return None
    return None


def _per_ticker_serial(
    codes: list[str],
    fetch_one: Callable[[str], pd.DataFrame | None],
    *,
    label: str,
    flush_every: int = 500,
) -> pd.DataFrame:
    """Single-threaded per-ticker loop with AKSHARE_SLEEP throttle.

    Memory discipline for 2G ECS runners (Wave 10):

    1. Bounded pending buffer — flush to parquet shard every
       ``flush_every`` tickers so pandas list<DataFrame> never grows
       unbounded.
    2. Memory watermark — after each fetch, check /proc/meminfo's
       ``MemAvailable``. Under 200 MB triggers an early flush + GC
       regardless of flush_every counter. This catches runaway cases
       where a single ticker returns an unexpectedly large DataFrame
       or upstream schema change balloons row count.
    3. On-disk shard → final concat reads shards one at a time.
       Peak RAM ≈ one shard + growing result DF, not 5500 DFs.

    Strict serial throttle: fewer requests per unit time *is* the
    point after the Eastmoney IP ban of 2026-04 taught us that.
    5500 × 1.5s ≈ 2.3h. Records failures with exception class at
    DEBUG level so a post-run investigation can tell what failed.
    """
    import gc
    import tempfile
    shard_dir = Path(tempfile.mkdtemp(prefix=f"mirror-{label}-", dir="/tmp"))
    shards: list[Path] = []
    pending: list[pd.DataFrame] = []
    failed_samples: list[str] = []
    failed = 0
    low_mem_flushes = 0

    def _flush() -> None:
        if not pending:
            return
        df = pd.concat(pending, ignore_index=True)
        shard_path = shard_dir / f"{len(shards):04d}.parquet"
        df.to_parquet(shard_path, compression="zstd", index=False)
        shards.append(shard_path)
        pending.clear()

    try:
        for i, code in enumerate(codes, 1):
            try:
                out = fetch_one(code)
                if out is not None and not out.empty:
                    pending.append(out)
                elif out is None or out.empty:
                    # not an exception, just no data for this ticker
                    pass
            except Exception as e:  # noqa: BLE001
                failed += 1
                if len(failed_samples) < 10:
                    failed_samples.append(f"{code}:{type(e).__name__}")
                log.debug("  %s failed %s: %s", label, code, e)
            if i % 200 == 0:
                log.info(
                    "  %s %d/%d (%d failed, %d shards, %d pending, "
                    "low_mem_flushes=%d, samples=%s)",
                    label, i, len(codes), failed, len(shards),
                    len(pending), low_mem_flushes, failed_samples[:3],
                )
            # Size-based flush
            if i % flush_every == 0:
                _flush()
                gc.collect()
                continue
            # Memory-watermark flush — cheap (reads /proc/meminfo).
            # Only check every 50 tickers to amortise the syscall cost.
            if i % 50 == 0:
                avail = _available_memory_bytes()
                if avail is not None and avail < _LOW_MEM_THRESHOLD_BYTES:
                    log.warning(
                        "  %s low-memory flush at i=%d (available=%dMB < %dMB)",
                        label, i, avail // (1024 * 1024),
                        _LOW_MEM_THRESHOLD_BYTES // (1024 * 1024),
                    )
                    _flush()
                    gc.collect()
                    low_mem_flushes += 1
        # Flush any leftover
        _flush()
        if failed:
            log.warning("  %s total failures: %d (first 10: %s)",
                        label, failed, failed_samples)
        if low_mem_flushes:
            log.warning("  %s triggered %d low-memory flushes", label, low_mem_flushes)
        if not shards:
            return pd.DataFrame()
        # Read shards back for final concat. At this point pending is
        # empty and peak memory = one shard at a time during parquet
        # read + the growing result DF. Much lower than holding all
        # 5500 DataFrames in `rows`.
        return pd.concat(
            [pd.read_parquet(p) for p in shards],
            ignore_index=True,
        )
    finally:
        # Clean up shards (result is fully in memory by now).
        for p in shards:
            try:
                p.unlink()
            except OSError:
                pass
        try:
            shard_dir.rmdir()
        except OSError:
            pass


def fetch_financials() -> dict:
    """Per-ticker full-history financial abstract — chunked over 7 days.

    Why chunked: a full 5500-ticker pass takes 2.5-3 hours on the
    2-core ECS runner and pushes memory close to the 2G cap. Worse,
    the GitHub Actions 4h timeout occasionally kills the job right
    before the final parquet write, so a whole week's refresh cycle
    is lost. And workers_serial (1 req / 1.5s) means a single flaky
    AKShare hour (e.g. 100 consecutive ticker timeouts × 30s = 50min
    lost) can push a run past 4h.

    New design — deterministic 7-day rotation:
    - Day-of-year modulo 7 picks the chunk. Chunk i = every 7th
      ticker starting at offset i when the universe is sorted.
    - Each day's chunk ≈ 5500/7 ≈ 785 tickers → ~20 min wall-clock,
      ~300 MB peak memory.
    - After 7 days every ticker has been refreshed once; quarterly
      data changes don't require faster.
    - On-disk accumulator: previous chunks' parquets stay in
      ``financials/chunks/<i>.parquet`` and latest.parquet is
      assembled from all 7 on write.

    The ``latest.parquet`` that SLATE reads is ALWAYS the union of
    every chunk — even before 7 days are complete, it has whichever
    6/7 (or 1/7 on day-one) that have been filled so far. No empty
    window; coverage grows monotonically.
    """
    from datetime import datetime
    sec = _ak_call(ak.stock_info_a_code_name)
    codes = sorted(sec["code"].astype(str).tolist())
    # Chunk index from the day of year. Modulo 7 (not modulo 30 or
    # whatever) gives one refresh per week per ticker — matches the
    # historical cadence of the weekly-slow workflow.
    chunk_idx = datetime.now(tz=timezone.utc).timetuple().tm_yday % 7
    chunk = [c for i, c in enumerate(codes) if i % 7 == chunk_idx]
    log.info("  financials: chunk %d/7 covers %d/%d tickers",
             chunk_idx, len(chunk), len(codes))

    def one(code: str):
        df = _ak_call(ak.stock_financial_abstract, symbol=code)
        if df is not None and not df.empty:
            df = df.copy()
            df["code"] = code
            return df
        return None

    df_chunk = _per_ticker_serial(
        chunk, one, label=f"financials-c{chunk_idx}",
    )
    if df_chunk.empty:
        raise RuntimeError(f"financials chunk {chunk_idx} returned 0 rows")
    # Persist chunk — becomes the authoritative source for these
    # tickers until the next 7-day rotation.
    _write_parquet(df_chunk, f"financials/chunks/{chunk_idx}.parquet")

    # Rebuild latest.parquet from all available chunks. If some chunks
    # haven't run yet, that's fine — latest just has fewer tickers,
    # still monotonic grows over the first week.
    chunks_dir = REPO_ROOT / "data" / "financials" / "chunks"
    chunk_dfs: list[pd.DataFrame] = []
    for p in sorted(chunks_dir.glob("*.parquet")):
        chunk_dfs.append(pd.read_parquet(p))
    full = pd.concat(chunk_dfs, ignore_index=True) if chunk_dfs else df_chunk
    # Defensive dedup — if two chunks accidentally overlap (shouldn't
    # happen with i%7 indexing but belt & suspenders for rewrites),
    # keep the latest write per (code, indicator).
    #
    # CRITICAL: the AKShare stock_financial_abstract DataFrame's
    # indicator column is "指标", NOT "item". An earlier version used
    # ("code", "item") and since "item" never existed in `full.columns`,
    # dedup fell back to ("code",) alone and slashed every ticker's
    # 70-indicator dataset down to 1 row. This silently reduced
    # latest.parquet from ~63,000 rows to 787 (one per ticker, whichever
    # indicator happened to be first), corrupting slate's
    # ingest_financials which then only saw 应付账款周转率.
    dedup_cols = [c for c in ("code", "指标") if c in full.columns]
    if len(dedup_cols) >= 2:
        full = full.drop_duplicates(subset=dedup_cols, keep="last")

    today = _today_cn().isoformat()
    _write_parquet(full, "financials/latest.parquet")
    kb = _write_parquet(df_chunk, f"financials/history/{today}-c{chunk_idx}.parquet")
    return {
        "rows": len(full),
        "chunk_rows": len(df_chunk),
        "chunk_idx": chunk_idx,
        "size_kb": kb,
    }


def fetch_macro() -> dict:
    calls = {
        "pmi": ak.macro_china_pmi_yearly,
        "cpi": ak.macro_china_cpi_yearly,
        "m2": ak.macro_china_money_supply,
        "shibor": ak.macro_china_shibor_all,
    }
    total = 0
    for name, fn in calls.items():
        try:
            df = _ak_call(fn)
            if df is None or df.empty:
                log.warning("  macro.%s empty", name)
                continue
            total += len(df)
            _write_parquet(df, f"macro/{name}.parquet")
        except Exception as e:  # noqa: BLE001
            log.warning("  macro.%s failed: %s", name, e)
    if total == 0:
        raise RuntimeError("all macro series failed")
    return {"rows": total}


def fetch_north_flow() -> dict:
    df = _ak_call(ak.stock_hsgt_hold_stock_em, market="北向", indicator="今日排行")
    today = _today_cn().isoformat()
    df["as_of"] = today
    _write_parquet(df, "north_flow/latest.parquet")
    kb = _write_parquet(df, f"north_flow/history/{today}.parquet")
    return {"rows": len(df), "size_kb": kb}


def fetch_lhb() -> dict:
    """Wave 12: 龙虎榜 historical backfill (30-day rolling + per-day cache).

    Previous: 7-day rolling only — any day missed was lost forever.
    New: 30-day rolling with per-day parquet cache, same pattern as
    fetch_filings. LHB data is immutable once published (day's trade
    flow is final), so cached days never need re-fetch.

    The backfill-lhb workflow pulls the deeper history (~5y) on the
    same cron slot as backfill-filings.
    """
    today = _today_cn()
    lhb_hist_dir = DATA_DIR / "lhb" / "history"
    lhb_hist_dir.mkdir(parents=True, exist_ok=True)

    frames: list[pd.DataFrame] = []
    window_days = 30
    per_day_ok = 0
    per_day_err = 0
    per_day_cached = 0

    for offset in range(window_days):
        d = today - timedelta(days=offset)
        ds_iso = d.isoformat()
        shard = lhb_hist_dir / f"{ds_iso}.parquet"
        # Today + yesterday: always re-fetch (late publication)
        # offset ≥ 2: cache if exists
        if offset >= 2 and shard.exists():
            try:
                df_c = pd.read_parquet(shard)
                if not df_c.empty:
                    frames.append(df_c)
                    per_day_cached += 1
                    continue
            except Exception:
                pass

        ds_ak = d.strftime("%Y%m%d")
        try:
            df_day = _ak_call(
                ak.stock_lhb_detail_em,
                start_date=ds_ak, end_date=ds_ak,
            )
        except Exception as e:  # noqa: BLE001
            log.warning("  lhb %s failed: %s", ds_ak, e)
            per_day_err += 1
            continue
        if df_day is None or df_day.empty:
            continue
        df_day = df_day.copy()
        df_day.to_parquet(shard, compression="zstd", index=False)
        frames.append(df_day)
        per_day_ok += 1

    if not frames:
        return {
            "rows": 0,
            "note": f"no lhb rows in {window_days}-day window",
        }
    out = pd.concat(frames, ignore_index=True)
    kb = _write_parquet(out, "lhb/latest.parquet")
    return {
        "rows": len(out),
        "size_kb": kb,
        "window_days": window_days,
        "days_ok": per_day_ok,
        "days_err": per_day_err,
        "days_cached": per_day_cached,
    }


def fetch_shareholders() -> dict:
    df = _ak_call(ak.stock_zh_a_gdhs, symbol="最新")
    today = _today_cn().isoformat()
    df["as_of"] = today
    _write_parquet(df, "shareholders/latest.parquet")
    kb = _write_parquet(df, f"shareholders/history/{today}.parquet")
    return {"rows": len(df), "size_kb": kb}


def fetch_yjyg() -> dict:
    """Wave 12: historical yjyg (业绩预告) covering last 20 quarters (5 years).

    Previous: 4 quarters. Reasoning for 20:
    - grade(scope='signal') needs historical earnings-surprise data to
      backtest the signal; 4 quarters is not enough statistical power.
    - Per-quarter parquet is cached under history/. Already-fetched
      quarters are skipped so runtime stays bounded at ~2-3 API calls
      (just the newest quarter + re-verify current quarter).
    - First full-run: 20 × 1.5s = 30s incremental.
    """
    today = _today_cn()
    # Walk back 20 CLOSED quarter-ends.
    quarters = []
    y, m = today.year, today.month
    q_end_months = [3, 6, 9, 12]
    for _ in range(20):
        candidates = [qm for qm in q_end_months if qm < m] or q_end_months
        qm = max(candidates)
        if qm >= m:
            y -= 1
        last_day = {3: 31, 6: 30, 9: 30, 12: 31}[qm]
        quarters.append(f"{y}{qm:02d}{last_day:02d}")
        m = qm

    yjyg_hist_dir = DATA_DIR / "yjyg" / "history"
    yjyg_hist_dir.mkdir(parents=True, exist_ok=True)
    frames: list[pd.DataFrame] = []
    fetched = 0
    cached = 0
    # The most recent 2 quarters may still get revisions (late filers);
    # always re-fetch those. Older quarters are immutable.
    for i, q in enumerate(quarters):
        shard = yjyg_hist_dir / f"{q}.parquet"
        if i >= 2 and shard.exists():
            try:
                df_c = pd.read_parquet(shard)
                if not df_c.empty:
                    frames.append(df_c)
                    cached += 1
                    continue
            except Exception as e:
                log.warning("  yjyg cached %s unreadable: %s", q, e)
        try:
            df = _ak_call(ak.stock_yjyg_em, date=q)
            if df is not None and not df.empty:
                df = df.copy()
                df["report_period"] = q
                df.to_parquet(shard, compression="zstd", index=False)
                frames.append(df)
                fetched += 1
        except Exception as e:  # noqa: BLE001
            log.warning("  yjyg %s failed: %s", q, e)
    if not frames:
        return {"rows": 0, "note": "no yjyg data"}
    out = pd.concat(frames, ignore_index=True)
    # Dedup across overlapping quarters (shouldn't happen but safe).
    if "股票代码" in out.columns and "report_period" in out.columns:
        out = out.drop_duplicates(subset=["股票代码", "report_period"], keep="last")
    kb = _write_parquet(out, "yjyg/latest.parquet")
    return {
        "rows": len(out),
        "size_kb": kb,
        "quarters": len(quarters),
        "fetched": fetched,
        "cached": cached,
    }


def fetch_margin() -> dict:
    """融资融券 daily aggregates, per exchange.

    SSE endpoint takes a date range — easy, always works.

    SZSE endpoint is single-date. Intraday runs (T-day before EOD) hit a
    column-length-mismatch error from AKShare because SZSE's API returns
    empty HTML for not-yet-published days. Walk backwards through recent
    trading days to pick up the freshest published snapshot.
    """
    today = _today_cn().strftime("%Y%m%d")
    start = (datetime.now(CST) - timedelta(days=30)).strftime("%Y%m%d")
    total = 0

    # SSE — date range, tolerates today being empty
    try:
        sse = _ak_call(ak.stock_margin_sse, start_date=start, end_date=today)
        if not sse.empty:
            _write_parquet(sse, "margin/sse_latest.parquet")
            total += len(sse)
    except Exception as e:  # noqa: BLE001
        log.warning("  margin.sse failed: %s", e)

    # SZSE — walk back up to 10 calendar days for the freshest published row.
    # Typical lag: 0-1 trading days; max 4 over a long weekend. 10 gives
    # generous headroom without looping forever on genuinely broken endpoint.
    frames: list[pd.DataFrame] = []
    szse_last_error: str | None = None
    for delta_days in range(11):
        probe = (datetime.now(CST) - timedelta(days=delta_days)).strftime("%Y%m%d")
        try:
            szse = _ak_call(ak.stock_margin_szse, date=probe)
            if szse is not None and not szse.empty:
                szse = szse.copy()
                szse["probe_date"] = probe
                frames.append(szse)
        except Exception as e:  # noqa: BLE001
            szse_last_error = f"{type(e).__name__}: {str(e)[:120]}"
            # Don't break — column-length error on today's empty payload
            # is specific to the latest date; earlier dates still work.
            continue
    if frames:
        szse_out = pd.concat(frames, ignore_index=True)
        # Dedup: the endpoint includes rows for the 5-day window anyway
        # (column: 信用交易日期), so multiple probes overlap. Keep the
        # newest probe_date per 信用交易日期.
        if "信用交易日期" in szse_out.columns:
            szse_out = szse_out.sort_values(
                ["信用交易日期", "probe_date"], ascending=[True, False]
            ).drop_duplicates(subset=["信用交易日期"], keep="first")
        szse_out = szse_out.drop(columns=["probe_date"], errors="ignore")
        _write_parquet(szse_out, "margin/szse_latest.parquet")
        total += len(szse_out)
    else:
        log.warning(
            "  margin.szse: no non-empty data in last 11 days (last_err=%s)",
            szse_last_error,
        )
    return {"rows": total}


def fetch_concepts() -> dict:
    """Concept boards + their constituent tickers.

    Wave 14: AKShare's Eastmoney concept endpoints (stock_board_concept_name_em /
    _cons_em) have been returning RemoteDisconnected for ~3 days (2026-04-22+).
    THS alternative (stock_board_concept_name_ths) works and returns 375 boards.
    There's no THS constituent-list API; EM is still the only source for members,
    so when EM is down we ship board names only and log the members gap.

    Downstream slate doesn't read concepts members today (no signal consumes
    it), so skipping is safe — operator can reach for concept data via board
    names alone.
    """
    boards = _fetch_board_names("concept", "concepts")
    names = boards["板块名称"].tolist() if "板块名称" in boards.columns else []

    def one(name: str):
        m = _ak_call(ak.stock_board_concept_cons_em, symbol=name)
        if m is not None and not m.empty:
            m = m.copy()
            m["board"] = name
            return m
        return None

    members = pd.DataFrame()
    members_source = "em"
    try:
        members = _per_ticker_serial(names, one, label="concept-members")
    except Exception as e:  # noqa: BLE001
        log.warning("  concepts.members (EM) failed: %s — skipping members write", e)
        members_source = "skipped_em_down"
    kb = 0
    if not members.empty:
        kb = _write_parquet(members, "concepts/members.parquet")
    return {"boards": len(boards), "members": len(members),
            "size_kb": kb, "members_source": members_source}


def fetch_industries() -> dict:
    """Industry boards + constituents — same THS-primary strategy as concepts."""
    boards = _fetch_board_names("industry", "industries")
    names = boards["板块名称"].tolist() if "板块名称" in boards.columns else []

    def one(name: str):
        m = _ak_call(ak.stock_board_industry_cons_em, symbol=name)
        if m is not None and not m.empty:
            m = m.copy()
            m["board"] = name
            return m
        return None

    members = pd.DataFrame()
    members_source = "em"
    try:
        members = _per_ticker_serial(names, one, label="industry-members")
    except Exception as e:  # noqa: BLE001
        log.warning("  industries.members (EM) failed: %s — skipping members write", e)
        members_source = "skipped_em_down"
    kb = 0
    if not members.empty:
        kb = _write_parquet(members, "industries/members.parquet")
    return {"boards": len(boards), "members": len(members),
            "size_kb": kb, "members_source": members_source}


def _fetch_board_names(kind: str, dir_label: str) -> pd.DataFrame:
    """Fetch board-names parquet with EM → THS fallback.

    ``kind``: "concept" or "industry".
    ``dir_label``: "concepts" or "industries" — target parquet subdirectory.

    Returns a DataFrame with ``板块名称`` column (normalised across sources
    so the rest of the fetcher can treat them identically). On THS fallback
    the frame has ``{板块名称, code, source="ths"}``; on EM it's the native
    EM columns with ``source="em"``.
    """
    em_fn = getattr(ak, f"stock_board_{kind}_name_em")
    ths_fn = getattr(ak, f"stock_board_{kind}_name_ths")
    try:
        df = _ak_call(em_fn)
        df = df.copy()
        df["source"] = "em"
        _write_parquet(df, f"{dir_label}/boards.parquet")
        log.info("  %s.boards via EM: %d", dir_label, len(df))
        return df
    except Exception as e:  # noqa: BLE001
        log.warning("  %s.boards EM failed: %s — falling back to THS", dir_label, e)
    # THS: columns are (name, code). Normalise to 板块名称.
    df = _ak_call(ths_fn)
    df = df.copy()
    df["板块名称"] = df["name"]
    df["source"] = "ths"
    _write_parquet(df, f"{dir_label}/boards.parquet")
    log.info("  %s.boards via THS: %d", dir_label, len(df))
    return df


def fetch_research() -> dict:
    """Research reports — Wave 14 restructure.

    Prior implementation iterated 5500 tickers × 1.5s serially (~2.3h),
    relying on per-ticker `stock_research_report_em(symbol=code)`. This
    was fragile: any mid-run Eastmoney throttle cascaded into "all
    symbol variants failed" for the batch, and a single weekly window
    missed = 7d stale.

    New approach:

      1. **Aggregate daily snapshot** — `stock_research_report_em()` with
         no symbol returns the 200-300 most recent reports across all
         tickers in a single call (~0.6s). This is what operators
         actually want for "最新研报" queries. Always written to
         `research/latest.parquet`.

      2. **Focused per-ticker top-up** (optional depth). For tickers
         that appear in at least one recent `filings/latest.parquet`
         row, run per-ticker deep fetch. Far smaller universe (maybe
         500 active filers) — 500 × 1.5s = 12.5 min rather than 2.3h.
         Appended to `research/latest.parquet` for the same day.

    Empty-result semantics: if the aggregate query returns < 20 rows,
    the endpoint is broken. Raise → fail_streak increments → healthcheck
    opens an issue after 3 such failures.
    """
    today = _today_cn().isoformat()
    # 1. Aggregate daily snapshot — one call.
    agg = _ak_call(ak.stock_research_report_em)
    if agg is None or len(agg) < 20:
        raise RuntimeError(
            f"research aggregate returned {0 if agg is None else len(agg)} rows — "
            "upstream contract break"
        )
    agg = agg.copy()
    # Normalise to a canonical "code" column for downstream joins.
    if "股票代码" in agg.columns:
        agg["code"] = agg["股票代码"].astype(str)

    # 2. Focused per-ticker top-up. Active filers = distinct tickers
    # seen in filings/latest.parquet within the last 30 days. If that
    # parquet isn't there yet, skip the top-up cleanly.
    top_up_rows = 0
    top_up_codes: list[str] = []
    filings_path = DATA / "filings" / "latest.parquet"
    if filings_path.exists():
        try:
            filings = pd.read_parquet(filings_path)
            if "code" in filings.columns and not filings.empty:
                top_up_codes = (
                    filings["code"].astype(str)
                    .dropna().unique().tolist()
                )
                # Hard cap — in degenerate cases filings could hold every
                # A-share code; 1000 × 1.5s = 25 min is still tolerable
                # but anything beyond that approaches the old 2.3h pain.
                if len(top_up_codes) > 1000:
                    top_up_codes = top_up_codes[:1000]
        except Exception as e:  # noqa: BLE001
            log.warning("  research top-up codes skipped: %s", e)

    if top_up_codes:
        def one(code: str):
            df = _ak_call(ak.stock_research_report_em, symbol=code)
            if df is not None and not df.empty:
                df = df.copy()
                df["code"] = code
                return df
            return None

        depth = _per_ticker_serial(top_up_codes, one, label="research-top-up")
        if not depth.empty:
            top_up_rows = len(depth)
            agg = pd.concat([agg, depth], ignore_index=True)
            # Dedup — aggregate + per-ticker will collide on same-day reports.
            if "报告名称" in agg.columns and "code" in agg.columns:
                agg = agg.drop_duplicates(
                    subset=["code", "报告名称"], keep="first"
                )

    _write_parquet(agg, "research/latest.parquet")
    kb = _write_parquet(agg, f"research/history/{today}.parquet")
    return {
        "rows": len(agg),
        "aggregate_rows": len(agg) - top_up_rows,
        "top_up_rows": top_up_rows,
        "top_up_tickers": len(top_up_codes),
        "size_kb": kb,
    }


def fetch_block_trades() -> dict:
    """大宗交易 (block trades) daily aggregate. One call per trading day.

    AKShare's `stock_dzjy_mrtj` returns rows only for tickers that had
    block-trade activity that day; typical daily volume is 30-150 rows.
    We keep a rolling 30-day history to support slate-side accumulation
    analysis (repeat block buys by the same institution over several
    weeks = conviction signal).
    """
    today = _today_cn()
    frames: list[pd.DataFrame] = []
    # Fetch up to the last 10 calendar days, skipping non-trading days.
    # Upstream rate-limits at ~2 req/s so 10 × 0.5s is trivial.
    for back in range(11):
        d = today - timedelta(days=back)
        if not _is_trading_day(d):
            continue
        ds = d.strftime("%Y%m%d")
        try:
            df = _ak_call(ak.stock_dzjy_mrtj, start_date=ds, end_date=ds)
            if df is not None and not df.empty:
                df = df.copy()
                df["trade_date"] = d.isoformat()
                frames.append(df)
        except Exception as e:  # noqa: BLE001
            # The endpoint returns a weird TypeError on today-before-publish
            # — benign; just try the next day.
            log.debug("  block_trades %s skipped: %s: %s", ds, type(e).__name__, str(e)[:80])
            continue
    if not frames:
        return {"rows": 0, "no_work": True}
    out = pd.concat(frames, ignore_index=True)
    _write_parquet(out, "block_trades/latest.parquet")
    kb = _write_parquet(out, f"block_trades/history/{today.isoformat()}.parquet")
    return {"rows": len(out), "trading_days": len(frames), "size_kb": kb}


def fetch_pledge() -> dict:
    """股权质押 (equity pledge) per-ticker ratio.

    AKShare's `stock_gpzy_pledge_ratio_em(date=YYYYMMDD)` returns ~2200
    tickers on any given trading day with columns including 质押比例
    (pledge ratio). This is the upstream for slate's pledge_snapshot
    table — previously declared in CLAUDE.md but never populated because
    mirror didn't fetch it.

    Walk back up to 10 days for the freshest published snapshot (same
    pattern as margin_szse).
    """
    today = _today_cn()
    for back in range(11):
        d = today - timedelta(days=back)
        if not _is_trading_day(d):
            continue
        ds = d.strftime("%Y%m%d")
        try:
            df = _ak_call(ak.stock_gpzy_pledge_ratio_em, date=ds)
            if df is not None and not df.empty:
                df = df.copy()
                df["snapshot_date"] = d.isoformat()
                _write_parquet(df, "pledge/latest.parquet")
                kb = _write_parquet(df, f"pledge/history/{d.isoformat()}.parquet")
                return {"rows": len(df), "snapshot_date": d.isoformat(), "size_kb": kb}
        except Exception as e:  # noqa: BLE001
            log.debug("  pledge %s skipped: %s: %s", ds, type(e).__name__, str(e)[:80])
            continue
    return {"rows": 0, "no_work": True}


def fetch_lockup_releases() -> dict:
    """限售解禁 (lockup expiry) upcoming queue — next 90 days.

    AKShare's `stock_restricted_release_summary_em` returns daily
    aggregated release events (date + total shares + market cap). Key
    signal: a large upcoming release = near-term supply pressure.

    We fetch 90-day forward window every 24h. Slate's ingest can
    later drill down per-ticker via stock_restricted_release_queue_em
    if needed.
    """
    today = _today_cn()
    start = today.strftime("%Y%m%d")
    end = (today + timedelta(days=90)).strftime("%Y%m%d")
    df = _ak_call(
        ak.stock_restricted_release_summary_em,
        symbol="全部股票", start_date=start, end_date=end,
    )
    if df is None or df.empty:
        return {"rows": 0, "no_work": True}
    df = df.copy()
    df["fetched_date"] = today.isoformat()
    _write_parquet(df, "lockup/latest.parquet")
    kb = _write_parquet(df, f"lockup/history/{today.isoformat()}.parquet")
    return {"rows": len(df), "size_kb": kb, "window": f"{start}..{end}"}


def fetch_news() -> dict:
    frames: list[pd.DataFrame] = []
    for label, fn in (
        ("cctv", lambda: ak.news_cctv(date=_today_cn().strftime("%Y%m%d"))),
        ("cls", lambda: ak.stock_info_global_cls(symbol="全部")),
    ):
        try:
            df = _retry(fn, tries=2)  # news doesn't hit eastmoney-hist — no throttle
            if df is not None and not df.empty:
                df = df.copy()
                df["source"] = label
                frames.append(df)
        except Exception as e:  # noqa: BLE001
            log.warning("  news.%s failed: %s", label, e)
    if not frames:
        return {"rows": 0}
    total = 0
    today = _today_cn().isoformat()
    for df in frames:
        src = df["source"].iloc[0]
        _write_parquet(df, f"news/{src}/{today}.parquet")
        total += len(df)
    return {"rows": total}


def fetch_filings() -> dict:
    """Mirror cninfo 全A announcement metadata, rolling 30-day window.

    Design evolution:
      W8k: single multi-day call (broken — cninfo truncates to today).
      W8o: 7-day loop, each call single-day (works, but 7d too narrow:
           runner outage >7d = permanent data gap).
      W11: 30-day rolling + per-day cache + peak-avoid:
        * Past days' parquets act as cache — announcements are
          immutable so re-fetching burns cninfo quota for zero value.
        * Today + yesterday always refreshed (more filings arrive
          throughout the day, late 公告 may cross UTC midnight).
        * Skip fetch 15:00-17:00 CST (cninfo publication flood,
          akshare gets 5xx during this window).
        * Net steady-state cost: 2 API calls/cron (today+yesterday).
        * Cold-start: 30 calls × 5s ≈ 2.5min, one-time.

    Slate consumer reads every history/*.parquet so growing the
    window costs nothing downstream.
    """
    now_cn = datetime.now(CST)
    # 15:00-17:00 CST = cninfo publication flood. AKShare hits the
    # same endpoint so we get hangs; skip and come back next slot.
    if 15 <= now_cn.hour < 17:
        return {
            "rows": 0,
            "note": f"skipped: within cninfo peak window (hour={now_cn.hour} CST)",
            "no_work": True,
        }

    today = _today_cn()
    frames: list[pd.DataFrame] = []
    window_days = 30
    per_day_ok = 0
    per_day_err = 0
    per_day_skip_cached = 0
    history_dir = DATA_DIR / "filings" / "history"
    history_dir.mkdir(parents=True, exist_ok=True)

    for offset in range(window_days):
        d = today - timedelta(days=offset)
        ds = d.strftime("%Y%m%d")
        ds_iso = d.isoformat()
        day_parquet = history_dir / f"{ds_iso}.parquet"

        # Cache policy:
        # - offset == 0 (today): always re-fetch
        # - offset == 1 (yesterday): always re-fetch (UTC-boundary 公告)
        # - offset >= 2: skip if history parquet already exists
        if offset >= 2 and day_parquet.exists():
            try:
                df_cached = pd.read_parquet(day_parquet)
                if not df_cached.empty:
                    frames.append(df_cached)
                    per_day_skip_cached += 1
                    continue
            except Exception as e:
                log.warning("  filings %s cached parquet unreadable: %s", ds, e)

        try:
            df_day = _retry(
                lambda ds=ds: ak.stock_zh_a_disclosure_report_cninfo(
                    symbol="",
                    market="沪深京",
                    keyword="",
                    category="",
                    start_date=ds,
                    end_date=ds,
                ),
                tries=3,
            )
        except KeyError as e:
            # akshare's column-rename fails on empty responses
            # (non-trading days: weekends, holidays). Not a real error
            # — write an empty marker parquet so we don't retry every
            # cron forever. Don't count against per_day_err.
            msg = str(e)
            if "代码" in msg or "announcementId" in msg:
                log.info("  filings %s non-trading day (empty response)", ds)
                pd.DataFrame(
                    columns=["代码", "简称", "公告标题", "公告时间",
                             "公告链接", "mirror_fetch_date"]
                ).to_parquet(day_parquet, compression="zstd", index=False)
                continue
            log.warning("  filings %s fetch failed: %s", ds, e)
            per_day_err += 1
            continue
        except Exception as e:  # noqa: BLE001
            log.warning("  filings %s fetch failed: %s", ds, e)
            per_day_err += 1
            continue
        if df_day is None or df_day.empty:
            # Empty response — also write marker (non-trading day).
            pd.DataFrame(
                columns=["代码", "简称", "公告标题", "公告时间",
                         "公告链接", "mirror_fetch_date"]
            ).to_parquet(day_parquet, compression="zstd", index=False)
            continue
        # Per-day dedup before writing (cninfo pagination overlaps).
        url_col = "公告链接" if "公告链接" in df_day.columns else "announcement_url"
        if url_col in df_day.columns:
            df_day = df_day.drop_duplicates(subset=[url_col], keep="first")
        df_day = df_day.copy()
        df_day["mirror_fetch_date"] = today.isoformat()
        df_day.to_parquet(day_parquet, compression="zstd", index=False)
        frames.append(df_day)
        per_day_ok += 1

    if not frames:
        return {
            "rows": 0,
            "note": (
                f"no filings in {window_days}-day window "
                f"(errors={per_day_err}, cached={per_day_skip_cached})"
            ),
        }
    df = pd.concat(frames, ignore_index=True)

    # Normalise Chinese column names → English (mirror convention: keep
    # one form, documented here). Downstream slate reader handles either
    # shape via a rename map.
    rename = {
        "代码": "code", "简称": "name",
        "公告标题": "title", "公告时间": "announcement_time",
        "announcementId": "announcement_id", "orgId": "org_id",
    }
    df = df.rename(columns=rename)
    # Dedup: cninfo's hisAnnouncement pagination emits overlapping windows
    # (in practice every 30-row page repeats across ~15 consecutive pages
    # — audit 2026-04-23: 3000 real filings observed as 44850 rows).
    # 公告链接 carries a unique announcementId per filing, so it's the
    # authoritative key. Keep first occurrence to preserve the earliest
    # announcement_time stamp.
    before = len(df)
    url_col = "公告链接" if "公告链接" in df.columns else "announcement_url"
    if url_col in df.columns:
        df = df.drop_duplicates(subset=[url_col], keep="first").reset_index(drop=True)
    else:
        df = df.drop_duplicates(subset=["code", "title", "announcement_time"], keep="first").reset_index(drop=True)
    after = len(df)
    if before != after:
        log.info("  filings dedup: %d → %d rows (%d duplicates dropped)",
                 before, after, before - after)
    # latest.parquet = union view across the 30-day window. Per-day
    # files already written inside the loop; no duplicate history
    # write here (was collapsing announcement-date detail pre-W11).
    if "mirror_fetch_date" not in df.columns:
        df["mirror_fetch_date"] = today.isoformat()
    kb = _write_parquet(df, "filings/latest.parquet")
    return {
        "rows": len(df),
        "size_kb": kb,
        "window_days": window_days,
        "dup_dropped": before - after,
        "days_ok": per_day_ok,
        "days_err": per_day_err,
        "days_skip_cached": per_day_skip_cached,
    }


# ── endpoint registry + cadence ─────────────────────────────────────────


@dataclass
class Endpoint:
    name: str
    cadence_h: int           # re-fetch if last success older than this many hours
    fetcher: Callable[[], dict]
    tags: list[str] = field(default_factory=list)


# Cadence rationale:
#   securities:    once/day — constituent changes are rare
#   market_daily:  4h — the fetcher itself gates on post-close time + gap-detect
#                  (re-running early just no-ops; keeps late-open retries cheap)
#   macro:         once/day — most series are monthly; cheap to re-fetch
#   news:          4h — intraday news flow
#   lhb/north_flow/yjyg/margin: once/day — post-close daily publish
#   financials/concepts/industries/shareholders: once/week — quarterly-ish
#   research:      once/day — daily publish
ENDPOINTS: list[Endpoint] = [
    Endpoint("securities",    cadence_h=20, fetcher=fetch_securities),
    Endpoint("market_daily",  cadence_h=4,  fetcher=fetch_market_daily),
    Endpoint("macro",         cadence_h=20, fetcher=fetch_macro),
    Endpoint("news",          cadence_h=4,  fetcher=fetch_news),
    # filings cadence = 6h: SZSE/SSE announcements trickle in through the
    # trading day, with a burst at post-close. 6h gives us 4 passes/day
    # so slate sees new filings within 6h of publication worst-case.
    Endpoint("filings",       cadence_h=6,  fetcher=fetch_filings),
    Endpoint("north_flow",    cadence_h=20, fetcher=fetch_north_flow),
    Endpoint("lhb",           cadence_h=20, fetcher=fetch_lhb),
    Endpoint("yjyg",          cadence_h=20, fetcher=fetch_yjyg),
    Endpoint("margin",        cadence_h=20, fetcher=fetch_margin),
    # Wave 14: high-value A-股 datasets
    Endpoint("block_trades",  cadence_h=20, fetcher=fetch_block_trades),
    Endpoint("pledge",        cadence_h=24, fetcher=fetch_pledge),
    Endpoint("lockup",        cadence_h=24, fetcher=fetch_lockup_releases),
    # research is per-ticker 5500× serial ≈ 2.3h — weekly only
    # Weekly (quarterly-paced data + per-ticker loops):
    # financials: chunked 1/7 per day — each pass is ~20min for 785 tickers.
    # cadence_h = 23 so a 24h-cycle cron isn't stopped by last-pass-at-23h58min.
    Endpoint("financials",    cadence_h=23,    fetcher=fetch_financials, tags=["slow"]),
    Endpoint("shareholders",  cadence_h=24 * 7, fetcher=fetch_shareholders),
    Endpoint("concepts",      cadence_h=24 * 7, fetcher=fetch_concepts,   tags=["slow"]),
    Endpoint("industries",    cadence_h=24 * 7, fetcher=fetch_industries, tags=["slow"]),
    Endpoint("research",      cadence_h=24 * 7, fetcher=fetch_research,   tags=["slow"]),
]


# ── status + scheduler ──────────────────────────────────────────────────


def _load_status() -> dict:
    if STATUS_FILE.exists():
        return json.loads(STATUS_FILE.read_text())
    return {"endpoints": {}}


def _save_status(status: dict) -> None:
    """Atomically persist _status.json.

    Previous version did a direct write_text which, if the process is
    OOM-killed mid-write, leaves a truncated JSON that every later
    _load_status() crashes on. Write to .tmp then os.replace.
    """
    status["updated_at"] = datetime.now(timezone.utc).isoformat()
    STATUS_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp = STATUS_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(status, ensure_ascii=False, indent=2))
    tmp.replace(STATUS_FILE)


def _is_due(ep: Endpoint, status: dict, *, force: bool) -> bool:
    """Cadence gate based on `last_attempt` (not last_success).

    Using last_attempt avoids the classic masking bug: a 'healthy no-op'
    that updates last_success would block re-runs for the full cadence
    even though nothing was fetched. last_attempt is always refreshed —
    so a no-work pass still counts for scheduling, but health signals
    (fail_streak, last_error) surface stalls.
    """
    if force:
        return True
    ep_status = status["endpoints"].get(ep.name, {})
    # Prefer last_attempt; fall back to last_success for rows written by
    # older versions of this script before last_attempt existed.
    last_ts = ep_status.get("last_attempt") or ep_status.get("last_success")
    if not last_ts:
        return True
    try:
        last_dt = datetime.fromisoformat(last_ts)
    except ValueError:
        return True
    age_h = (datetime.now(timezone.utc) - last_dt).total_seconds() / 3600
    return age_h >= ep.cadence_h


def _git_commit_local(ep_name: str, meta: dict) -> bool:
    """Commit this endpoint's diff locally WITHOUT pushing.

    Returns True if a commit was created, False if no diff to commit
    (also True) — False only on actual git command failure. The caller
    batches all commits and pushes once at the end via _git_push_all().
    """
    def run(*args: str, check: bool = True) -> subprocess.CompletedProcess:
        return subprocess.run(args, cwd=REPO_ROOT, check=check, text=True, capture_output=True)
    try:
        run("git", "add", "data/")
        if run("git", "diff", "--cached", "--quiet", check=False).returncode == 0:
            log.info("  (no diff to commit for %s)", ep_name)
            return True
        rows = meta.get("rows", meta.get("boards", meta.get("members", "?")))
        extra = ""
        if meta.get("filled_dates"):
            extra = f" [filled: {','.join(meta['filled_dates'][:5])}{'...' if len(meta['filled_dates']) > 5 else ''}]"
        run("git", "commit", "-m", f"data({ep_name}): {rows} rows{extra}")
        return True
    except subprocess.CalledProcessError as e:
        log.warning("  git commit failed for %s: %s", ep_name,
                    (e.stderr or e.stdout or "").strip()[-200:])
        return False


def _git_push_all() -> bool:
    """Batched push of every local commit from this pass.

    Pulls --rebase --autostash first so a concurrent writer (another
    workflow or human commit) doesn't force-fail us. Wave 13: retries
    the pull+push cycle up to 3 times with backoff, since concurrent
    workflow runs (fast + slow + backfill) all push to main through
    the same runner; a mid-push race can look like `failed to push
    some refs (non-fast-forward)` which fixes itself on a repeat.

    Returns True on success, False otherwise.
    """
    def run(*args: str, check: bool = True) -> subprocess.CompletedProcess:
        return subprocess.run(args, cwd=REPO_ROOT, check=check, text=True, capture_output=True)
    # Nothing to push? Skip fast.
    ahead = run("git", "rev-list", "--count", "@{upstream}..HEAD", check=False)
    if ahead.returncode == 0 and ahead.stdout.strip() == "0":
        log.info("  (no commits to push)")
        return True
    for attempt in range(1, 4):
        try:
            pull = run("git", "pull", "--rebase", "--autostash", "origin", "main", check=False)
            if pull.returncode != 0:
                log.warning("  pull --rebase failed (try %d/3): %s", attempt,
                            (pull.stderr or pull.stdout or "").strip()[-200:])
                if attempt < 3:
                    time.sleep(5 * attempt)
                    continue
                return False
            push = run("git", "push", "origin", "HEAD:main", check=False)
            if push.returncode != 0:
                log.warning("  push failed (try %d/3): %s", attempt,
                            (push.stderr or push.stdout or "").strip()[-200:])
                if attempt < 3:
                    time.sleep(5 * attempt)
                    continue
                return False
            log.info("  batched push successful (try %d)", attempt)
            return True
        except subprocess.CalledProcessError as e:
            log.warning("  git op failed (try %d/3): %s", attempt,
                        (e.stderr or e.stdout or "").strip()[-200:])
            if attempt < 3:
                time.sleep(5 * attempt)
                continue
            return False
    return False


def run_once(*, only: list[str] | None, force: bool, incremental_push: bool) -> int:
    """Run every due endpoint once.

    Commits: one commit per endpoint locally (preserves per-endpoint
    log granularity), but a SINGLE push at the end that carries all
    of them together. This cuts the repo's growth rate by ~6× vs the
    previous per-endpoint-push pattern — every fast cron used to mint
    8 separate commits × 5 runs/day = 40 network round-trips; now
    it's 5 pushes/day with 8 commits each. Same data granularity, far
    less repo bloat (each push is a single ref update).

    If a commit/push step fails, the pass continues and the final
    push step tries again at the end with all accumulated commits.
    """
    status = _load_status()
    started = time.time()
    succeeded = failed = skipped = 0
    committed_endpoints: list[str] = []

    for ep in ENDPOINTS:
        if only and ep.name not in only:
            continue
        if not _is_due(ep, status, force=force):
            log.info("skip %s (cadence not due)", ep.name)
            skipped += 1
            continue
        log.info("── %s ──", ep.name)
        ep_started = time.time()
        prior = status["endpoints"].get(ep.name, {})
        try:
            meta = ep.fetcher()
            elapsed = time.time() - ep_started
            now_iso = datetime.now(timezone.utc).isoformat()
            produced_data = meta.get("rows", 0) > 0 or bool(meta.get("filled_dates"))
            no_work = bool(meta.get("no_work"))
            new_streak = 0 if produced_data or no_work else prior.get("fail_streak", 0) + 1
            status["endpoints"][ep.name] = {
                **prior,
                "last_attempt": now_iso,
                "last_elapsed_s": round(elapsed, 1),
                "last_meta": meta,
                "last_error": None,
                "fail_streak": new_streak,
            }
            if produced_data or no_work:
                status["endpoints"][ep.name]["last_success"] = now_iso
            succeeded += 1
            log.info("  ✓ %s (%.1fs, rows=%s, streak=%d)",
                     ep.name, elapsed, meta.get("rows", "?"), new_streak)
            _save_status(status)
            if incremental_push:
                # Commit locally (no push yet) — batched push at the end.
                if _git_commit_local(ep.name, meta):
                    committed_endpoints.append(ep.name)
        except Exception as e:  # noqa: BLE001
            failed += 1
            status["endpoints"][ep.name] = {
                **prior,
                "last_attempt": datetime.now(timezone.utc).isoformat(),
                "last_error": f"{type(e).__name__}: {e}",
                "last_error_at": datetime.now(timezone.utc).isoformat(),
                "fail_streak": prior.get("fail_streak", 0) + 1,
            }
            log.error("  ✗ %s: %s", ep.name, e)
            log.debug(traceback.format_exc())
            _save_status(status)

    status["last_pass"] = {
        "at": datetime.now(timezone.utc).isoformat(),
        "succeeded": succeeded,
        "failed": failed,
        "skipped": skipped,
        "elapsed_s": round(time.time() - started, 1),
    }
    _save_status(status)
    # Final batched push — collects all endpoint commits + the status
    # update into one network round-trip.
    if incremental_push:
        _git_commit_local("_status", {"rows": "status"})
        pushed = _git_push_all()
        if not pushed:
            # Push failed — roll back last_attempt for every endpoint
            # we claimed success for, so cadence retries them next run.
            for ep_name in committed_endpoints:
                ep_status = status["endpoints"].get(ep_name, {})
                ep_status["last_attempt"] = (
                    ep_status.get("_prior_last_attempt")
                    or ep_status.get("last_attempt")
                )
                ep_status["last_push_error"] = datetime.now(timezone.utc).isoformat()
            _save_status(status)
    log.info("pass done: %d ok, %d fail, %d skip, %.1fs",
             succeeded, failed, skipped, time.time() - started)
    return 0 if succeeded > 0 or (succeeded + failed) == 0 else 1


if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--only", help="comma-separated endpoint names (implies --force for those)")
    p.add_argument("--force", action="store_true", help="ignore cadence")
    p.add_argument("--no-push", action="store_true",
                   help="skip per-endpoint git push")
    args = p.parse_args()
    only = [s.strip() for s in args.only.split(",")] if args.only else None
    sys.exit(run_once(only=only,
                      force=bool(args.only) or args.force,
                      incremental_push=not args.no_push))
