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


# ── per-endpoint fetchers ───────────────────────────────────────────────


def fetch_securities() -> dict:
    """Securities master list.

    Prefer Tencent universe probe (same source that seeds market_daily so
    we never get (ticker, trade_date) rows for a ticker missing from
    securities). Falls back to akshare if Tencent is down.
    """
    try:
        universe = tm.fetch_universe()
        if universe:
            df = pd.DataFrame([
                {"code": u["ticker"].split(".")[0], "name": u["name"]}
                for u in universe
            ])
            kb = _write_parquet(df, "securities/latest.parquet")
            return {"rows": len(df), "size_kb": kb, "source": "tencent"}
    except Exception as e:  # noqa: BLE001
        log.warning("  tencent universe failed (%s) — falling back to akshare", e)
    df = _ak_call(ak.stock_info_a_code_name)
    kb = _write_parquet(df, "securities/latest.parquet")
    return {"rows": len(df), "size_kb": kb, "source": "akshare"}


def _fetch_one_day_bars(d: date) -> pd.DataFrame | None:
    """Fetch OHLCV for all A-shares on a specific trade date via Tencent gtimg.

    Tencent's qt.gtimg.cn / web.ifzq.gtimg.cn endpoints are independently
    rate-limited from Eastmoney (which ban-hammered us for an akshare bulk
    run). Tencent also returns 5 years of qfq history per call so this
    function is reused for bootstrap — for a single-day fetch we just
    filter to the target date after the call.
    """
    # Universe via Tencent probe (cheap, ~45s for full A-share)
    universe = tm.fetch_universe()
    tickers = [u["ticker"] for u in universe]
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
            })
    if not rows:
        return None
    return pd.DataFrame(rows)


def _is_trading_day(d: date) -> bool:
    """Skip weekends. Holidays: we try anyway — AKShare returns empty for
    exchange holidays and _fetch_one_day_bars returns None, which the caller
    records as 'no bars for this date' without flagging it a failure."""
    return d.weekday() < 5


def fetch_market_daily() -> dict:
    """Append today's OHLCV (post-close) + backfill any missing recent days.

    Two responsibilities:
    1. If today's file doesn't exist and it's after 16:00 CST (post-close),
       fetch today.
    2. Scan the last 30 calendar days for any trading day whose parquet is
       missing and backfill it.
    """
    today = _today_cn()
    now = datetime.now(CST)

    pending: list[date] = []

    # Today — only after 16:00 CST to ensure close prices are final
    if _is_trading_day(today) and now.hour >= 16:
        if not _market_daily_path(today).exists():
            pending.append(today)

    # Last 30 days — fill gaps
    for back in range(1, 31):
        d = today - timedelta(days=back)
        if not _is_trading_day(d):
            continue
        if not _market_daily_path(d).exists():
            pending.append(d)

    if not pending:
        return {"rows": 0, "note": "no pending dates (all recent days already mirrored)"}

    pending.sort(reverse=True)
    log.info("  market_daily pending dates: %s", [d.isoformat() for d in pending])

    total_rows = 0
    filled = []
    for d in pending:
        log.info("  fetching market_daily for %s", d)
        df = _fetch_one_day_bars(d)
        if df is None or df.empty:
            log.info("    (no data — likely holiday or pre-publish)")
            continue
        df["trade_date"] = d.isoformat()
        path_rel = f"market_daily/{d.year}/{d.month:02d}/{d.isoformat()}.parquet"
        _write_parquet(df, path_rel)
        total_rows += len(df)
        filled.append(d.isoformat())

    return {
        "rows": total_rows,
        "filled_dates": filled,
        "pending_dates": [d.isoformat() for d in pending],
    }


def _per_ticker_serial(codes: list[str], fetch_one: Callable[[str], pd.DataFrame | None], *, label: str) -> pd.DataFrame:
    """Single-threaded per-ticker loop with AKSHARE_SLEEP throttle.

    We used ThreadPoolExecutor at 4-6 workers originally. After seeing a
    full-universe Eastmoney IP ban during a bulk run we reverted to
    strict serial: fewer requests per unit time *is* the point. 5500
    tickers × 1.5s ≈ 2.3h. Ample, and ban-proof.
    """
    rows: list[pd.DataFrame] = []
    failed = 0
    for i, code in enumerate(codes, 1):
        try:
            out = fetch_one(code)
            if out is not None and not out.empty:
                rows.append(out)
        except Exception:  # noqa: BLE001
            failed += 1
        if i % 200 == 0:
            log.info("  %s %d/%d (%d failed)", label, i, len(codes), failed)
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
    # last 4 quarter-ends
    quarters = []
    y, m = today.year, today.month
    q_end_months = [3, 6, 9, 12]
    for _ in range(4):
        qm = max([qm for qm in q_end_months if qm <= m], default=None)
        if qm is None:
            y -= 1
            qm = 12
        last_day = {3: 31, 6: 30, 9: 30, 12: 31}[qm]
        quarters.append(f"{y}{qm:02d}{last_day:02d}")
        m = qm - 1
        if m < 1:
            y -= 1
            m = 12
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
    for sym in ("全部", "今日"):
        try:
            df = _ak_call(ak.stock_research_report_em, symbol=sym)
            if df is not None and not df.empty:
                today = _today_cn().isoformat()
                _write_parquet(df, "research/latest.parquet")
                kb = _write_parquet(df, f"research/history/{today}.parquet")
                return {"rows": len(df), "size_kb": kb, "symbol": sym}
        except Exception as e:  # noqa: BLE001
            log.warning("  research symbol=%s failed: %s", sym, e)
    return {"rows": 0, "note": "all symbol variants failed — likely upstream schema change"}


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
    Endpoint("research",      cadence_h=20, fetcher=fetch_research),
    # Weekly (quarterly-paced data):
    Endpoint("financials",    cadence_h=24 * 7, fetcher=fetch_financials, tags=["slow"]),
    Endpoint("shareholders",  cadence_h=24 * 7, fetcher=fetch_shareholders),
    Endpoint("concepts",      cadence_h=24 * 7, fetcher=fetch_concepts,   tags=["slow"]),
    Endpoint("industries",    cadence_h=24 * 7, fetcher=fetch_industries, tags=["slow"]),
]


# ── status + scheduler ──────────────────────────────────────────────────


def _load_status() -> dict:
    if STATUS_FILE.exists():
        return json.loads(STATUS_FILE.read_text())
    return {"endpoints": {}}


def _save_status(status: dict) -> None:
    status["updated_at"] = datetime.now(timezone.utc).isoformat()
    STATUS_FILE.parent.mkdir(parents=True, exist_ok=True)
    STATUS_FILE.write_text(json.dumps(status, ensure_ascii=False, indent=2))


def _is_due(ep: Endpoint, status: dict, *, force: bool) -> bool:
    if force:
        return True
    ep_status = status["endpoints"].get(ep.name, {})
    last_ok = ep_status.get("last_success")
    if not last_ok:
        return True
    try:
        last_dt = datetime.fromisoformat(last_ok)
    except ValueError:
        return True
    age_h = (datetime.now(timezone.utc) - last_dt).total_seconds() / 3600
    return age_h >= ep.cadence_h


def _git_push_incremental(ep_name: str, meta: dict) -> None:
    def run(*args: str, check: bool = True) -> subprocess.CompletedProcess:
        return subprocess.run(args, cwd=REPO_ROOT, check=check, text=True, capture_output=True)
    try:
        run("git", "add", "data/")
        if run("git", "diff", "--cached", "--quiet", check=False).returncode == 0:
            log.info("  (no diff to commit for %s)", ep_name)
            return
        rows = meta.get("rows", meta.get("boards", meta.get("members", "?")))
        extra = ""
        if meta.get("filled_dates"):
            extra = f" [filled: {','.join(meta['filled_dates'][:5])}{'...' if len(meta['filled_dates']) > 5 else ''}]"
        run("git", "commit", "-m", f"data({ep_name}): {rows} rows{extra}")
        run("git", "push", "origin", "HEAD:main")
        log.info("  pushed %s", ep_name)
    except subprocess.CalledProcessError as e:
        log.warning("  push failed for %s: %s — will retry on next endpoint", ep_name,
                    (e.stderr or e.stdout or "").strip()[-200:])


def run_once(*, only: list[str] | None, force: bool, incremental_push: bool) -> int:
    status = _load_status()
    started = time.time()
    succeeded = failed = skipped = 0

    for ep in ENDPOINTS:
        if only and ep.name not in only:
            continue
        if not _is_due(ep, status, force=force):
            log.info("skip %s (cadence not due)", ep.name)
            skipped += 1
            continue
        log.info("── %s ──", ep.name)
        ep_started = time.time()
        try:
            meta = ep.fetcher()
            elapsed = time.time() - ep_started
            status["endpoints"][ep.name] = {
                "last_success": datetime.now(timezone.utc).isoformat(),
                "last_elapsed_s": round(elapsed, 1),
                "last_meta": meta,
                "last_error": None,
                "fail_streak": 0,
            }
            succeeded += 1
            log.info("  ✓ %s (%.1fs)", ep.name, elapsed)
            _save_status(status)
            if incremental_push:
                _git_push_incremental(ep.name, meta)
        except Exception as e:  # noqa: BLE001
            failed += 1
            prior = status["endpoints"].get(ep.name, {})
            status["endpoints"][ep.name] = {
                **prior,
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
    if incremental_push:
        _git_push_incremental("_status", {"rows": "status"})
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
