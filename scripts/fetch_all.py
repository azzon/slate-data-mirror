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


def _retry(fn: Callable, *args, tries: int = 3, base_delay: float = 5.0, **kw):
    """Exponential backoff for transient upstream errors."""
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
    """Retry-wrapped AKShare call with built-in throttle."""
    time.sleep(AKSHARE_SLEEP)
    return _retry(fn, *args, tries=3, base_delay=10.0, **kw)


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
    MAX_GAP_FILL = int(os.environ.get("MIRROR_MAX_GAP_FILL", "20"))
    HISTORY_DAYS = int(os.environ.get("MIRROR_HISTORY_DAYS", str(365 * 5 + 1)))

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

    return {
        "rows": total_rows,
        "filled_dates": filled,
        "pending_total": len(pending),
        "budget": MAX_GAP_FILL,
    }


def _per_ticker_serial(codes: list[str], fetch_one: Callable[[str], pd.DataFrame | None], *, label: str) -> pd.DataFrame:
    """Single-threaded per-ticker loop with AKSHARE_SLEEP throttle.

    Strict serial: fewer requests per unit time *is* the point after
    the Eastmoney IP ban of 2026-04 taught us that. 5500 × 1.5s ≈ 2.3h.
    Records failures with exception class at DEBUG level so a post-run
    investigation can tell what failed.
    """
    rows: list[pd.DataFrame] = []
    failed_samples: list[str] = []
    failed = 0
    for i, code in enumerate(codes, 1):
        try:
            out = fetch_one(code)
            if out is not None and not out.empty:
                rows.append(out)
            elif out is None or out.empty:
                # not an exception, just no data for this ticker
                pass
        except Exception as e:  # noqa: BLE001
            failed += 1
            if len(failed_samples) < 10:
                failed_samples.append(f"{code}:{type(e).__name__}")
            log.debug("  %s failed %s: %s", label, code, e)
        if i % 200 == 0:
            log.info("  %s %d/%d (%d failed, samples=%s)",
                     label, i, len(codes), failed, failed_samples[:3])
    if failed:
        log.warning("  %s total failures: %d (first 10: %s)",
                    label, failed, failed_samples)
    if not rows:
        return pd.DataFrame()
    return pd.concat(rows, ignore_index=True)


def fetch_financials() -> dict:
    """Per-ticker full-history financial abstract.

    Runs weekly. Strictly serial with AKSHARE_SLEEP between calls —
    5500 tickers × 1.5s ≈ 2.3 hours. The speed hit is worth it: any
    ban on this endpoint blocks financials for a week, and the data
    changes quarterly, so we never need speed.
    """
    sec = _ak_call(ak.stock_info_a_code_name)
    codes = sec["code"].astype(str).tolist()

    def one(code: str):
        df = _ak_call(ak.stock_financial_abstract, symbol=code)
        if df is not None and not df.empty:
            df = df.copy()
            df["code"] = code
            return df
        return None

    df = _per_ticker_serial(codes, one, label="financials")
    if df.empty:
        raise RuntimeError("financials returned 0 rows")
    today = _today_cn().isoformat()
    _write_parquet(df, "financials/latest.parquet")
    kb = _write_parquet(df, f"financials/history/{today}.parquet")
    return {"rows": len(df), "size_kb": kb}


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
    today = _today_cn()
    start = (today - timedelta(days=7)).strftime("%Y%m%d")
    end = today.strftime("%Y%m%d")
    df = _ak_call(ak.stock_lhb_detail_em, start_date=start, end_date=end)
    if df is None or df.empty:
        return {"rows": 0, "note": "no lhb rows in window"}
    _write_parquet(df, "lhb/latest.parquet")
    kb = _write_parquet(df, f"lhb/history/{today.isoformat()}.parquet")
    return {"rows": len(df), "size_kb": kb}


def fetch_shareholders() -> dict:
    df = _ak_call(ak.stock_zh_a_gdhs, symbol="最新")
    today = _today_cn().isoformat()
    df["as_of"] = today
    _write_parquet(df, "shareholders/latest.parquet")
    kb = _write_parquet(df, f"shareholders/history/{today}.parquet")
    return {"rows": len(df), "size_kb": kb}


def fetch_yjyg() -> dict:
    today = _today_cn()
    # Last 4 CLOSED quarter-ends. A quarter-end on or equal to today is
    # not yet reported; start from the most recent strictly-past one.
    quarters = []
    y, m = today.year, today.month
    q_end_months = [3, 6, 9, 12]
    for _ in range(4):
        # Find largest quarter-end strictly before today (m-1 so March
        # doesn't pretend 2026-03-31 is closed)
        candidates = [qm for qm in q_end_months if qm < m] or q_end_months
        qm = max(candidates)
        if qm >= m:  # rolled back to previous year
            y -= 1
        last_day = {3: 31, 6: 30, 9: 30, 12: 31}[qm]
        quarters.append(f"{y}{qm:02d}{last_day:02d}")
        m = qm  # next iter finds the quarter-end before this one
    frames = []
    for q in quarters:
        try:
            df = _ak_call(ak.stock_yjyg_em, date=q)
            if df is not None and not df.empty:
                df = df.copy()
                df["report_period"] = q
                frames.append(df)
        except Exception as e:  # noqa: BLE001
            log.warning("  yjyg %s failed: %s", q, e)
    if not frames:
        return {"rows": 0, "note": "no yjyg data"}
    out = pd.concat(frames, ignore_index=True)
    kb = _write_parquet(out, "yjyg/latest.parquet")
    return {"rows": len(out), "size_kb": kb, "quarters": quarters}


def fetch_margin() -> dict:
    today = _today_cn().strftime("%Y%m%d")
    start = (datetime.now(CST) - timedelta(days=30)).strftime("%Y%m%d")
    total = 0
    try:
        sse = _ak_call(ak.stock_margin_sse, start_date=start, end_date=today)
        if not sse.empty:
            _write_parquet(sse, "margin/sse_latest.parquet")
            total += len(sse)
    except Exception as e:  # noqa: BLE001
        log.warning("  margin.sse failed: %s", e)
    try:
        szse = _ak_call(ak.stock_margin_szse, date=today)
        if not szse.empty:
            _write_parquet(szse, "margin/szse_latest.parquet")
            total += len(szse)
    except Exception as e:  # noqa: BLE001
        log.warning("  margin.szse failed: %s", e)
    return {"rows": total}


def fetch_concepts() -> dict:
    """Concept boards + their constituent tickers. Serial to avoid bans."""
    boards = _ak_call(ak.stock_board_concept_name_em)
    _write_parquet(boards, "concepts/boards.parquet")
    names = boards["板块名称"].tolist() if "板块名称" in boards.columns else []

    def one(name: str):
        m = _ak_call(ak.stock_board_concept_cons_em, symbol=name)
        if m is not None and not m.empty:
            m = m.copy()
            m["board"] = name
            return m
        return None

    members = _per_ticker_serial(names, one, label="concept-members")
    kb = 0
    if not members.empty:
        kb = _write_parquet(members, "concepts/members.parquet")
    return {"boards": len(boards), "members": len(members), "size_kb": kb}


def fetch_industries() -> dict:
    boards = _ak_call(ak.stock_board_industry_name_em)
    _write_parquet(boards, "industries/boards.parquet")
    names = boards["板块名称"].tolist() if "板块名称" in boards.columns else []

    def one(name: str):
        m = _ak_call(ak.stock_board_industry_cons_em, symbol=name)
        if m is not None and not m.empty:
            m = m.copy()
            m["board"] = name
            return m
        return None

    members = _per_ticker_serial(names, one, label="industry-members")
    kb = 0
    if not members.empty:
        kb = _write_parquet(members, "industries/members.parquet")
    return {"boards": len(boards), "members": len(members), "size_kb": kb}


def fetch_research() -> dict:
    """Per-ticker research reports. AKShare's API changed — `symbol="全部"`
    no longer works; must query per ticker. Serial with AKSHARE_SLEEP;
    5500 × 1.5s ≈ 2.3h — only runs in the weekly slow workflow.

    Empty-result semantics: individual tickers legitimately have no
    research reports (small caps, BJ-listed), so per-ticker None is
    normal. But if EVERY ticker returns empty across 5500 calls, the
    upstream API contract is broken (prior Eastmoney schema change
    that lost the `infoCode` field is the canonical example) — raise
    so `run_once` records an error AND increments fail_streak, so
    healthcheck opens an issue after 3 such failures instead of
    sitting on rows=0 forever looking healthy-ish.
    """
    sec = _ak_call(ak.stock_info_a_code_name)
    codes = sec["code"].astype(str).tolist()

    def one(code: str):
        df = _ak_call(ak.stock_research_report_em, symbol=code)
        if df is not None and not df.empty:
            df = df.copy()
            df["code"] = code
            return df
        return None

    df = _per_ticker_serial(codes, one, label="research")
    if df.empty:
        raise RuntimeError(
            f"research returned zero rows across {len(codes)} tickers — "
            "upstream schema break likely (AKShare / Eastmoney contract change)"
        )
    today = _today_cn().isoformat()
    _write_parquet(df, "research/latest.parquet")
    kb = _write_parquet(df, f"research/history/{today}.parquet")
    return {"rows": len(df), "size_kb": kb}


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
    Endpoint("north_flow",    cadence_h=20, fetcher=fetch_north_flow),
    Endpoint("lhb",           cadence_h=20, fetcher=fetch_lhb),
    Endpoint("yjyg",          cadence_h=20, fetcher=fetch_yjyg),
    Endpoint("margin",        cadence_h=20, fetcher=fetch_margin),
    # research is per-ticker 5500× serial ≈ 2.3h — weekly only
    # Weekly (quarterly-paced data + per-ticker loops):
    Endpoint("financials",    cadence_h=24 * 7, fetcher=fetch_financials, tags=["slow"]),
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
    workflow or human commit) doesn't force-fail us. Returns True on
    success, False otherwise.
    """
    def run(*args: str, check: bool = True) -> subprocess.CompletedProcess:
        return subprocess.run(args, cwd=REPO_ROOT, check=check, text=True, capture_output=True)
    try:
        # Nothing to push? Skip fast.
        ahead = run("git", "rev-list", "--count", "@{upstream}..HEAD", check=False)
        if ahead.returncode == 0 and ahead.stdout.strip() == "0":
            log.info("  (no commits to push)")
            return True
        pull = run("git", "pull", "--rebase", "--autostash", "origin", "main", check=False)
        if pull.returncode != 0:
            log.warning("  pull --rebase failed: %s",
                        (pull.stderr or pull.stdout or "").strip()[-200:])
            return False
        push = run("git", "push", "origin", "HEAD:main", check=False)
        if push.returncode != 0:
            log.warning("  push failed: %s",
                        (push.stderr or push.stdout or "").strip()[-200:])
            return False
        log.info("  batched push successful")
        return True
    except subprocess.CalledProcessError as e:
        log.warning("  git op failed: %s",
                    (e.stderr or e.stdout or "").strip()[-200:])
        return False


def _git_push_incremental(ep_name: str, meta: dict) -> bool:
    """DEPRECATED — kept for bootstrap_history compatibility. New code
    should use _git_commit_local + _git_push_all for batched pushes."""
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
        # Pull-rebase-autostash — accounts for two workflows writing
        # concurrently, or for a human making a repo-level commit.
        pull = run("git", "pull", "--rebase", "--autostash", "origin", "main", check=False)
        if pull.returncode != 0:
            log.warning("  pull --rebase failed for %s: %s", ep_name,
                        (pull.stderr or pull.stdout or "").strip()[-200:])
            return False
        push = run("git", "push", "origin", "HEAD:main", check=False)
        if push.returncode != 0:
            log.warning("  push failed for %s: %s", ep_name,
                        (push.stderr or push.stdout or "").strip()[-200:])
            return False
        log.info("  pushed %s", ep_name)
        return True
    except subprocess.CalledProcessError as e:
        log.warning("  git op failed for %s: %s", ep_name,
                    (e.stderr or e.stdout or "").strip()[-200:])
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
