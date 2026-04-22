#!/usr/bin/env python3
"""Fetch every mirrored AKShare endpoint once, write Parquet, update status.

Runs on a self-hosted GitHub Actions runner inside mainland China (AKShare
sources geo-block overseas IPs, so GitHub-hosted runners are out).

Design:
- One entry point, one pass. Scheduler = GitHub Actions cron (5 triggers
  per day; each endpoint decides if *it* is due based on its cadence).
- Each endpoint is independent: one failure never aborts the pass.
- Outputs are Parquet under data/<group>/. Daily-shaped datasets are
  written as YYYY-MM-DD.parquet (append-only); snapshot-shaped are
  latest.parquet (overwrite). Both idempotent — re-running the same day
  produces the same commit.
- Every pass writes data/_status.json with per-endpoint freshness so the
  slate consumer can caveat stale data.
"""

from __future__ import annotations

import json
import logging
import os
import subprocess
import sys
import time
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Callable

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
log = logging.getLogger("mirror")

CST = timezone(timedelta(hours=8))


def _today_cn() -> date:
    return datetime.now(CST).date()


def _write_parquet(df: pd.DataFrame, rel: str) -> int:
    path = DATA_DIR / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, compression="snappy", index=False)
    size_kb = path.stat().st_size // 1024
    log.info("  wrote %s (rows=%d, %dKB)", rel, len(df), size_kb)
    return size_kb


# ── per-endpoint fetchers ───────────────────────────────────────────────


def fetch_securities() -> dict:
    df = ak.stock_info_a_code_name()
    kb = _write_parquet(df, "securities/latest.parquet")
    return {"rows": len(df), "size_kb": kb}


def fetch_market_daily() -> dict:
    """Whole-market snapshot via stock_zh_a_spot_em (single cheap call)."""
    today = _today_cn().isoformat()
    df = ak.stock_zh_a_spot_em()
    df["trade_date"] = today
    kb = _write_parquet(df, f"market_daily/{today[:4]}/{today}.parquet")
    return {"rows": len(df), "size_kb": kb, "trade_date": today}


def _per_ticker(codes: list[str], fetch_one: Callable[[str], pd.DataFrame | None], *, workers: int, label: str) -> pd.DataFrame:
    rows: list[pd.DataFrame] = []
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(fetch_one, c): c for c in codes}
        for i, fut in enumerate(as_completed(futures), 1):
            out = fut.result()
            if out is not None and not out.empty:
                rows.append(out)
            if i % 500 == 0:
                log.info("  %s %d/%d", label, i, len(codes))
    if not rows:
        return pd.DataFrame()
    return pd.concat(rows, ignore_index=True)


def fetch_financials() -> dict:
    sec = ak.stock_info_a_code_name()
    codes = sec["code"].astype(str).tolist()

    def one(code: str):
        try:
            df = ak.stock_financial_abstract(symbol=code)
            if df is not None and not df.empty:
                df = df.copy()
                df["code"] = code
                return df
        except Exception:  # noqa: BLE001
            return None

    df = _per_ticker(codes, one, workers=6, label="financials")
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
        df = fn()
        if df is None or df.empty:
            log.warning("  macro.%s empty", name)
            continue
        total += len(df)
        _write_parquet(df, f"macro/{name}.parquet")
    return {"rows": total}


def fetch_north_flow() -> dict:
    """Northbound (HK→mainland) per-ticker holdings, latest day."""
    df = ak.stock_hsgt_hold_stock_em(market="北向", indicator="今日排行")
    today = _today_cn().isoformat()
    df["as_of"] = today
    _write_parquet(df, "north_flow/latest.parquet")
    kb = _write_parquet(df, f"north_flow/history/{today}.parquet")
    return {"rows": len(df), "size_kb": kb}


def fetch_lhb() -> dict:
    """Dragon-Tiger list for the latest published day.

    sina LHB is published T+0 late evening; we pull the most recent 7 days
    and overwrite to avoid gaps if we miss one run.
    """
    today = _today_cn()
    start = (today - timedelta(days=7)).strftime("%Y%m%d")
    end = today.strftime("%Y%m%d")
    df = ak.stock_lhb_detail_em(start_date=start, end_date=end)
    if df is None or df.empty:
        return {"rows": 0, "note": "no lhb rows in window"}
    _write_parquet(df, "lhb/latest.parquet")
    kb = _write_parquet(df, f"lhb/history/{today.isoformat()}.parquet")
    return {"rows": len(df), "size_kb": kb}


def fetch_shareholders() -> dict:
    """Shareholder-count snapshots (not per-ticker — a cross-sectional tool)."""
    df = ak.stock_zh_a_gdhs(symbol="最新")
    today = _today_cn().isoformat()
    df["as_of"] = today
    _write_parquet(df, "shareholders/latest.parquet")
    kb = _write_parquet(df, f"shareholders/history/{today}.parquet")
    return {"rows": len(df), "size_kb": kb}


def fetch_yjyg() -> dict:
    """Earnings pre-announcements for the most recent 4 quarters."""
    today = _today_cn()
    # stock_yjyg_em expects YYYYMMDD of a quarter-end
    quarters = []
    y, m = today.year, today.month
    # last 4 quarter-ends
    q_end_months = [3, 6, 9, 12]
    for _ in range(4):
        qm = max([qm for qm in q_end_months if qm <= m], default=None)
        if qm is None:
            y -= 1
            qm = 12
        last_day = {3: 31, 6: 30, 9: 30, 12: 31}[qm]
        quarters.append(f"{y}{qm:02d}{last_day:02d}")
        # step back one quarter
        m = qm - 1
        if m < 1:
            y -= 1
            m = 12
    frames = []
    for q in quarters:
        try:
            df = ak.stock_yjyg_em(date=q)
            if df is not None and not df.empty:
                df = df.copy()
                df["report_period"] = q
                frames.append(df)
        except Exception:  # noqa: BLE001
            continue
    if not frames:
        return {"rows": 0, "note": "no yjyg data"}
    out = pd.concat(frames, ignore_index=True)
    kb = _write_parquet(out, "yjyg/latest.parquet")
    return {"rows": len(out), "size_kb": kb, "quarters": quarters}


def fetch_margin() -> dict:
    """Margin trading balance — SSE + SZSE aggregate."""
    today = _today_cn().strftime("%Y%m%d")
    try:
        sse = ak.stock_margin_sse(start_date=(datetime.now(CST) - timedelta(days=30)).strftime("%Y%m%d"), end_date=today)
    except Exception:  # noqa: BLE001
        sse = pd.DataFrame()
    try:
        szse = ak.stock_margin_szse(date=today)
    except Exception:  # noqa: BLE001
        szse = pd.DataFrame()
    total = 0
    if not sse.empty:
        _write_parquet(sse, "margin/sse_latest.parquet")
        total += len(sse)
    if not szse.empty:
        _write_parquet(szse, "margin/szse_latest.parquet")
        total += len(szse)
    return {"rows": total}


def fetch_concepts() -> dict:
    """Concept boards + their constituent tickers. Two calls: list + members."""
    boards = ak.stock_board_concept_name_em()
    _write_parquet(boards, "concepts/boards.parquet")
    names = boards["板块名称"].tolist() if "板块名称" in boards.columns else []

    def one(name: str):
        try:
            m = ak.stock_board_concept_cons_em(symbol=name)
            if m is not None and not m.empty:
                m = m.copy()
                m["board"] = name
                return m
        except Exception:  # noqa: BLE001
            return None

    members = _per_ticker(names, one, workers=8, label="concept-members")
    kb = 0
    if not members.empty:
        kb = _write_parquet(members, "concepts/members.parquet")
    return {"boards": len(boards), "members": len(members), "size_kb": kb}


def fetch_industries() -> dict:
    boards = ak.stock_board_industry_name_em()
    _write_parquet(boards, "industries/boards.parquet")
    names = boards["板块名称"].tolist() if "板块名称" in boards.columns else []

    def one(name: str):
        try:
            m = ak.stock_board_industry_cons_em(symbol=name)
            if m is not None and not m.empty:
                m = m.copy()
                m["board"] = name
                return m
        except Exception:  # noqa: BLE001
            return None

    members = _per_ticker(names, one, workers=8, label="industry-members")
    kb = 0
    if not members.empty:
        kb = _write_parquet(members, "industries/members.parquet")
    return {"boards": len(boards), "members": len(members), "size_kb": kb}


def fetch_research() -> dict:
    df = ak.stock_research_report_em(symbol="全部")
    if df is None or df.empty:
        return {"rows": 0, "note": "empty research report dump"}
    today = _today_cn().isoformat()
    _write_parquet(df, "research/latest.parquet")
    kb = _write_parquet(df, f"research/history/{today}.parquet")
    return {"rows": len(df), "size_kb": kb}


def fetch_news() -> dict:
    """Aggregate financial news streams."""
    frames = []
    for label, fn in (
        ("cctv", lambda: ak.news_cctv(date=_today_cn().strftime("%Y%m%d"))),
        ("cls", lambda: ak.stock_info_global_cls(symbol="全部")),
    ):
        try:
            df = fn()
            if df is not None and not df.empty:
                df = df.copy()
                df["source"] = label
                frames.append(df)
        except Exception as e:  # noqa: BLE001
            log.warning("  news.%s failed: %s", label, e)
    if not frames:
        return {"rows": 0}
    # news schemas differ per source — keep as separate files.
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


ENDPOINTS: list[Endpoint] = [
    # Daily bars + universe — cheap, run every pass
    Endpoint("securities",    cadence_h=20, fetcher=fetch_securities),
    Endpoint("market_daily",  cadence_h=4,  fetcher=fetch_market_daily),
    Endpoint("macro",         cadence_h=20, fetcher=fetch_macro),
    Endpoint("north_flow",    cadence_h=20, fetcher=fetch_north_flow),
    Endpoint("lhb",           cadence_h=20, fetcher=fetch_lhb),
    Endpoint("yjyg",          cadence_h=20, fetcher=fetch_yjyg),
    Endpoint("margin",        cadence_h=20, fetcher=fetch_margin),
    Endpoint("research",      cadence_h=20, fetcher=fetch_research),
    Endpoint("news",          cadence_h=4,  fetcher=fetch_news),
    # Slow (5-40 min): run once a day max
    Endpoint("financials",    cadence_h=20, fetcher=fetch_financials,  tags=["slow"]),
    Endpoint("concepts",      cadence_h=48, fetcher=fetch_concepts,    tags=["slow"]),
    Endpoint("industries",    cadence_h=48, fetcher=fetch_industries,  tags=["slow"]),
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
    """Commit this endpoint's new data + status, push to origin.

    Per-endpoint commits mean: (a) progress is visible live in the Actions
    UI and on the repo, (b) a late-endpoint failure never loses earlier
    work. Failure here logs + continues — a push hiccup shouldn't abort
    the pass; the *next* endpoint's push will catch it up.
    """
    def run(*args: str, check: bool = True) -> subprocess.CompletedProcess:
        return subprocess.run(args, cwd=REPO_ROOT, check=check, text=True, capture_output=True)
    try:
        run("git", "add", "data/")
        if run("git", "diff", "--cached", "--quiet", check=False).returncode == 0:
            log.info("  (no diff to commit for %s)", ep_name)
            return
        rows = meta.get("rows", meta.get("boards", meta.get("members", "?")))
        run("git", "commit", "-m", f"data({ep_name}): {rows} rows")
        run("git", "push", "origin", "HEAD:main")
        log.info("  pushed %s", ep_name)
    except subprocess.CalledProcessError as e:  # noqa: BLE001
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
            # Persist status every endpoint so a later crash doesn't lose
            # the "last_success" entries we just earned.
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
    log.info("pass done: %d ok, %d fail, %d skip, %.1fs",
             succeeded, failed, skipped, time.time() - started)
    # Exit non-zero only if *every* attempted endpoint failed (catastrophic),
    # so partial AKShare outages don't kill the workflow run.
    return 0 if succeeded > 0 or (succeeded + failed) == 0 else 1


if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--only", help="comma-separated endpoint names to run (skip cadence check implied)")
    p.add_argument("--force", action="store_true", help="ignore cadence, run everything")
    p.add_argument("--no-push", action="store_true",
                   help="skip per-endpoint git push (let caller push once at the end)")
    args = p.parse_args()
    only = [s.strip() for s in args.only.split(",")] if args.only else None
    sys.exit(run_once(only=only,
                      force=bool(args.only) or args.force,
                      incremental_push=not args.no_push))
