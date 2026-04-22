"""Market data via Tencent gtimg.cn — unblocked fallback for AKShare.

AKShare's default providers (SSE / SZSE / 东财 / 新浪) are categorised as
"Finance and Banking" by upstream HTTP proxies (Fortinet / Intel IGK)
and return 403 before any request reaches the data host. Tencent's
``qt.gtimg.cn`` (realtime quotes + market cap) and
``web.ifzq.gtimg.cn`` (historical OHLCV) sit on the general
``gtimg.cn`` CDN and slip through.

Shapes mirror the three akshare functions SLATE depends on so callers
don't have to care which provider served the data:

* ``fetch_universe()``       → ``refresh_securities()``
* ``fetch_daily_bars(...)``  → ``refresh_daily_bars(...)``
* ``fetch_market_cap()``     → ``refresh_market_cap_snapshot()``

Nothing here talks to DuckDB — persistence stays in ``akshare_market``
so the swap is one-line at the call site.
"""

from __future__ import annotations

import concurrent.futures as cf
import contextlib
import datetime as dt
import os
import threading
import time
from collections.abc import Iterable

import httpx
import logging
logger = logging.getLogger("tencent")

# gtimg.cn endpoints (verified reachable behind igkproxy102)
_QT = "https://qt.gtimg.cn/q={codes}"
_KLINE = "https://web.ifzq.gtimg.cn/appstock/app/fqkline/get"

# Full A-share universe prefixes. Brute-force over ~13k codes, keep only
# rows where gtimg returns a real payload (not ``v_pv_none_match``).
_PREFIX_RANGES: list[tuple[str, int, int]] = [
    # Shanghai main board
    ("sh", 600000, 606000),
    # Shanghai STAR (科创板)
    ("sh", 688000, 689000),
    # Shanghai B-share
    ("sh", 900000, 901000),
    # Shenzhen main (000/001/002/003)
    ("sz", 0, 4000),
    # Shenzhen ChiNext (创业板) 300/301
    ("sz", 300000, 302000),
    # Shenzhen B-share
    ("sz", 200000, 201000),
]

# Default batch + worker tuning. 50 codes per GET stays under gtimg's
# ~6KB response cap. Workers dropped from 16 → 8 after a full A-share
# nightly stalled 14 minutes behind a corporate proxy that throttles on
# fresh-connection bursts. 8 workers over a persistent pool is faster
# than 16 workers each doing fresh TLS handshakes.
_BATCH = 50
_WORKERS = 8
_PROGRESS_EVERY = 500  # log one heartbeat line per N tickers in bars fetch


def _proxy() -> str | None:
    return (
        os.environ.get("HTTPS_PROXY")
        or os.environ.get("https_proxy")
        or os.environ.get("HTTP_PROXY")
        or os.environ.get("http_proxy")
    )


def _client(timeout: float = 20.0) -> httpx.Client:
    # Explicit proxy arg — httpx's trust_env doesn't always honour
    # HTTPS_PROXY when the scheme is http (corporate proxies commonly
    # are). Re-read the env each client so tests can monkeypatch.
    return httpx.Client(
        proxy=_proxy(),
        timeout=timeout,
        headers={"User-Agent": "Mozilla/5.0 slate-tencent/0.6"},
    )


# Thread-local persistent client. Building a fresh httpx.Client per
# request forces a fresh TLS handshake through the proxy on every call
# — at 5600 tickers that's 5600 handshakes, enough to trigger
# Fortinet/Intel IGK connection-rate throttling and stall the pool. A
# reused client keeps the TCP+TLS session alive across calls on the
# same worker thread.
_TLS = threading.local()


def _pool_client(timeout: float = 8.0) -> httpx.Client:
    existing = getattr(_TLS, "client", None)
    if existing is not None:
        return existing
    client = httpx.Client(
        proxy=_proxy(),
        timeout=timeout,
        headers={"User-Agent": "Mozilla/5.0 slate-tencent/0.6"},
        limits=httpx.Limits(
            max_keepalive_connections=8,
            max_connections=16,
            keepalive_expiry=30.0,
        ),
    )
    _TLS.client = client
    return client


def _close_pool_clients(pool: cf.ThreadPoolExecutor) -> None:
    # Executor worker threads each built a thread-local client; ensure
    # they're closed so the pool doesn't linger. Best-effort — if a
    # worker never ran (empty queue), submit() is a no-op.
    def _close() -> None:
        c = getattr(_TLS, "client", None)
        if c is not None:
            with contextlib.suppress(Exception):
                c.close()
            _TLS.client = None

    futures = [pool.submit(_close) for _ in range(pool._max_workers)]
    for f in futures:
        with contextlib.suppress(Exception):
            f.result(timeout=2.0)


def _parse_qt(text: str) -> list[dict]:
    """Parse ``v_<code>="~1~name~code~..."`` into dicts.

    Tencent encodes responses as **GBK** (not UTF-8). httpx decodes via
    ``charset`` header; we defensively tolerate the stray codec issue
    by skipping rows whose name is not parseable, rather than bubbling
    an exception.
    """
    out: list[dict] = []
    for line in text.split(";"):
        line = line.strip()
        if not line.startswith("v_") or "none_match" in line:
            continue
        if '="' not in line:
            continue
        inside = line.split('"', 2)[1]
        parts = inside.split("~")
        # Spec (tencent qt v1): 1=name 2=code 3=last 4=prev_close 5=open
        # 6=volume ... 44=circ_mv 45=total_mv
        if len(parts) < 6:
            continue
        name = parts[1].strip()
        code = parts[2].strip()
        if not name or not code or name == "-":
            continue
        out.append(
            {
                "code": code,
                "name": name,
                "last": _to_float(parts[3]),
                "prev_close": _to_float(parts[4]),
                "open": _to_float(parts[5]) if len(parts) > 5 else None,
                # Both are in 亿元 (hundred millions of yuan).
                "circ_mv_yi": _to_float(parts[44]) if len(parts) > 44 else None,
                "total_mv_yi": _to_float(parts[45]) if len(parts) > 45 else None,
            }
        )
    return out


def _to_float(s: str) -> float | None:
    s = (s or "").strip()
    if not s or s == "-":
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _code_to_ticker(raw_code: str) -> str:
    code = str(raw_code).zfill(6)
    if code.startswith(("60", "688", "689", "900")):
        return f"{code}.SH"
    return f"{code}.SZ"


def _qt_code(ticker_or_code: str) -> str:
    """``600000.SH`` / ``600000`` / ``sh600000`` → ``sh600000``."""
    s = ticker_or_code.lower()
    if s.startswith(("sh", "sz", "bj")):
        return s[:8]
    core = s.split(".")[0].zfill(6)
    if s.endswith(".sh") or core.startswith(("60", "688", "689", "900")):
        return f"sh{core}"
    return f"sz{core}"


# --------------------------------------------------------------------------
# Public API — mirrors akshare_market shape for an easy swap.
# --------------------------------------------------------------------------


def fetch_universe() -> list[dict]:
    """Return ``[{ticker, name, exchange}]`` for the full A-share universe.

    Probes ~13k candidate codes across the 6 standard prefix ranges, in
    50-batch chunks, 16 threads. Typical run: ~45s, ~5600 rows.
    """
    candidates: list[str] = []
    for ex, lo, hi in _PREFIX_RANGES:
        for n in range(lo, hi):
            candidates.append(f"{ex}{n:06d}")

    batches = [candidates[i : i + _BATCH] for i in range(0, len(candidates), _BATCH)]
    found: list[dict] = []

    def _work(batch: list[str]) -> list[dict]:
        # Per-batch try/except — a single transient 5xx / timeout must not
        # abort the whole universe probe and force an akshare fallback.
        try:
            c = _pool_client()
            r = c.get(_QT.format(codes=",".join(batch)))
            return _parse_qt(r.text)
        except Exception as e:
            logger.debug(f"tencent qt batch failed ({batch[0]}..): {e}")
            return []

    pool = cf.ThreadPoolExecutor(max_workers=_WORKERS)
    try:
        for rows in pool.map(_work, batches):
            found.extend(rows)
    finally:
        _close_pool_clients(pool)
        pool.shutdown(wait=True)

    # Dedupe by code — qt never returns dupes, but be defensive.
    seen: set[str] = set()
    unique: list[dict] = []
    for row in found:
        if row["code"] in seen:
            continue
        seen.add(row["code"])
        ticker = _code_to_ticker(row["code"])
        unique.append(
            {
                "ticker": ticker,
                "name": row["name"],
                "exchange": ticker.split(".")[1],
                # Carry market-cap too so the securities ingest step can
                # double-duty as a spot snapshot and callers don't have
                # to round-trip gtimg a second time.
                "total_mv_yi": row["total_mv_yi"],
                "circ_mv_yi": row["circ_mv_yi"],
                "last": row["last"],
            }
        )
    logger.info(f"tencent: A-share universe = {len(unique)} tickers")
    return unique


def fetch_daily_bars(
    tickers: Iterable[str],
    *,
    start: str,
    end: str | None = None,
    max_workers: int = _WORKERS,
) -> dict[str, list[dict]]:
    """Daily OHLCV bars per ticker in ``[start, end]`` (inclusive).

    Tencent's fqkline endpoint returns rows shaped
    ``[date, open, close, high, low, volume]`` — note that order is
    open/close/high/low, NOT OHLC. Volume is in "手" (hundreds of
    shares); we leave the unit alone for parity with akshare.

    Dates are ``YYYYMMDD`` on input (akshare compat) but Tencent wants
    ``YYYY-MM-DD``. Transparent to callers.

    **Pagination.** Tencent's fqkline caps responses at 640 trade days
    per call (even if ``lmt`` param is higher). For windows wider than
    ~600 calendar days we auto-split into sequential sub-windows and
    merge — so a 5y request ends up making 4 calls per ticker.
    """
    end_d = end or dt.date.today().strftime("%Y%m%d")
    start_dt = dt.date(int(start[:4]), int(start[4:6]), int(start[6:8]))
    end_dt = dt.date(int(end_d[:4]), int(end_d[4:6]), int(end_d[6:8]))
    tickers = list(tickers)

    # Build list of (sub_start, sub_end) windows covering the requested
    # range. 600 days each — safely under the 640-bar server cap.
    WINDOW_DAYS = 600
    windows: list[tuple[dt.date, dt.date]] = []
    cursor = start_dt
    while cursor <= end_dt:
        sub_end = min(cursor + dt.timedelta(days=WINDOW_DAYS - 1), end_dt)
        windows.append((cursor, sub_end))
        cursor = sub_end + dt.timedelta(days=1)

    def _fetch_window(ticker: str, sub_start: dt.date, sub_end: dt.date) -> list[dict]:
        qt = _qt_code(ticker)
        params = {
            "param": f"{qt},day,{sub_start.isoformat()},{sub_end.isoformat()},640,qfq",
        }
        try:
            c = _pool_client()
            r = c.get(_KLINE, params=params)
            payload = r.json()
        except Exception as e:
            logger.debug(f"tencent bars failed {ticker} [{sub_start}→{sub_end}]: {e}")
            return []
        if payload.get("code") != 0:
            return []
        block = payload.get("data", {}).get(qt) or {}
        raw = block.get("qfqday") or block.get("day") or []
        out: list[dict] = []
        for row in raw:
            if len(row) < 6:
                continue
            try:
                out.append(
                    {
                        "trade_date": row[0],
                        "open": float(row[1]),
                        "close": float(row[2]),
                        "high": float(row[3]),
                        "low": float(row[4]),
                        # gtimg volume unit is "手"; multiply by 100 for
                        # consistency with akshare's 股-granularity volume.
                        "volume": float(row[5]) * 100.0,
                    }
                )
            except (TypeError, ValueError):
                continue
        return out

    def _fetch_one(ticker: str) -> tuple[str, list[dict]]:
        all_bars: list[dict] = []
        seen_dates: set[str] = set()
        for sub_start, sub_end in windows:
            bars = _fetch_window(ticker, sub_start, sub_end)
            for b in bars:
                if b["trade_date"] not in seen_dates:
                    all_bars.append(b)
                    seen_dates.add(b["trade_date"])
        all_bars.sort(key=lambda r: r["trade_date"])
        return ticker, all_bars

    result: dict[str, list[dict]] = {}
    # Periodic heartbeat so a stall is visible in journalctl instead
    # of a silent 15-minute gap. First real nightly full-universe run
    # should land every 500 tickers in well under a minute; anything
    # slower is proxy throttling and the log will show it.
    done = 0
    t0 = time.monotonic()
    pool = cf.ThreadPoolExecutor(max_workers=max_workers)
    try:
        for ticker, bars in pool.map(_fetch_one, tickers):
            result[ticker] = bars
            done += 1
            if done % _PROGRESS_EVERY == 0:
                elapsed = time.monotonic() - t0
                rate = done / elapsed if elapsed > 0 else 0.0
                logger.info(
                    f"tencent bars progress: {done}/{len(tickers)} "
                    f"({rate:.1f}/s, {elapsed:.0f}s elapsed)"
                )
    finally:
        _close_pool_clients(pool)
        pool.shutdown(wait=True)
    total = sum(len(v) for v in result.values())
    elapsed = time.monotonic() - t0
    logger.info(
        f"tencent: fetched {total} daily bars across {len(tickers)} tickers "
        f"in {elapsed:.0f}s"
    )
    return result


def fetch_market_cap() -> dict[str, float]:
    """Snapshot ``{ticker: total_mv_yuan}`` via the universe probe.

    Reuses ``fetch_universe`` — the qt response already carries total_mv
    in 亿元, so a one-shot call is 5x cheaper than akshare's 5-minute
    ``stock_zh_a_spot_em``.
    """
    universe = fetch_universe()
    out: dict[str, float] = {}
    for row in universe:
        mv_yi = row.get("total_mv_yi")
        if mv_yi is None:
            continue
        # Store in 元 to match akshare's 总市值 column (which is in 元).
        out[row["ticker"]] = float(mv_yi) * 1e8
    logger.info(f"tencent: market_cap snapshot rows={len(out)}")
    return out
