"""Tests for fetch_all.py pure helpers.

No network, no subprocess — every test runs in a tmp DATA_DIR sandbox.
"""
from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import pytest


def _sandbox(monkeypatch, tmp_path: Path):
    """Redirect fetch_all's DATA_DIR to tmp_path so _write_parquet /
    _save_status / _load_status don't touch the real repo."""
    import fetch_all as fa
    data_dir = tmp_path / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(fa, "DATA_DIR", data_dir)
    monkeypatch.setattr(fa, "STATUS_FILE", data_dir / "_status.json")
    monkeypatch.setattr(fa, "PERMAFAIL_FILE", data_dir / "_market_daily_permafail.json")
    return fa


# ── permafail round-trip ──────────────────────────────────────────────


def test_permafail_round_trip(tmp_path, monkeypatch):
    fa = _sandbox(monkeypatch, tmp_path)
    assert fa._load_market_daily_permafail() == set()
    fa._save_market_daily_permafail({"2024-01-01", "2024-05-01"})
    assert fa._load_market_daily_permafail() == {"2024-01-01", "2024-05-01"}
    # Idempotent overwrite
    fa._save_market_daily_permafail({"2024-02-01"})
    assert fa._load_market_daily_permafail() == {"2024-02-01"}


def test_permafail_atomic_write(tmp_path, monkeypatch):
    fa = _sandbox(monkeypatch, tmp_path)
    fa._save_market_daily_permafail({"2024-01-01"})
    # Make sure no .tmp leaks
    tmps = list((tmp_path / "data").glob("*.tmp"))
    assert tmps == []


# ── status round-trip ─────────────────────────────────────────────────


def test_save_load_status_atomic(tmp_path, monkeypatch):
    fa = _sandbox(monkeypatch, tmp_path)
    s0 = fa._load_status()
    assert s0 == {"endpoints": {}}
    s0["endpoints"]["market_daily"] = {"last_success": "2026-04-23T00:00:00+00:00"}
    fa._save_status(s0)
    s1 = fa._load_status()
    assert s1["endpoints"]["market_daily"]["last_success"] == "2026-04-23T00:00:00+00:00"
    # No stray .tmp
    assert list((tmp_path / "data").glob("*.tmp")) == []


# ── _is_due cadence gate ──────────────────────────────────────────────


def test_is_due_first_ever_run(tmp_path, monkeypatch):
    fa = _sandbox(monkeypatch, tmp_path)
    ep = fa.Endpoint("market_daily", cadence_h=4, fetcher=lambda: {})
    assert fa._is_due(ep, {"endpoints": {}}, force=False) is True


def test_is_due_force_overrides(tmp_path, monkeypatch):
    fa = _sandbox(monkeypatch, tmp_path)
    ep = fa.Endpoint("ep", cadence_h=4, fetcher=lambda: {})
    # Even if attempted 1 min ago, force=True returns True
    recent = (datetime.now(timezone.utc) - timedelta(minutes=1)).isoformat()
    status = {"endpoints": {"ep": {"last_attempt": recent}}}
    assert fa._is_due(ep, status, force=True) is True


def test_is_due_respects_cadence(tmp_path, monkeypatch):
    fa = _sandbox(monkeypatch, tmp_path)
    ep = fa.Endpoint("ep", cadence_h=4, fetcher=lambda: {})
    # 2h ago — should skip (< 4h cadence)
    recent = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat()
    status = {"endpoints": {"ep": {"last_attempt": recent}}}
    assert fa._is_due(ep, status, force=False) is False


def test_is_due_triggers_after_cadence(tmp_path, monkeypatch):
    fa = _sandbox(monkeypatch, tmp_path)
    ep = fa.Endpoint("ep", cadence_h=4, fetcher=lambda: {})
    old = (datetime.now(timezone.utc) - timedelta(hours=5)).isoformat()
    status = {"endpoints": {"ep": {"last_attempt": old}}}
    assert fa._is_due(ep, status, force=False) is True


def test_is_due_falls_back_to_last_success(tmp_path, monkeypatch):
    """Status rows written by OLD versions had last_success but no
    last_attempt — should still work."""
    fa = _sandbox(monkeypatch, tmp_path)
    ep = fa.Endpoint("ep", cadence_h=4, fetcher=lambda: {})
    old = (datetime.now(timezone.utc) - timedelta(hours=5)).isoformat()
    status = {"endpoints": {"ep": {"last_success": old}}}
    assert fa._is_due(ep, status, force=False) is True


# ── _is_trading_day ───────────────────────────────────────────────────


def test_is_trading_day_weekday(tmp_path, monkeypatch):
    fa = _sandbox(monkeypatch, tmp_path)
    # Monday 2026-04-20
    assert fa._is_trading_day(date(2026, 4, 20)) is True


def test_is_trading_day_saturday(tmp_path, monkeypatch):
    fa = _sandbox(monkeypatch, tmp_path)
    # Saturday 2026-04-18
    assert fa._is_trading_day(date(2026, 4, 18)) is False


# ── _market_daily_path shape ──────────────────────────────────────────


def test_market_daily_path_shape(tmp_path, monkeypatch):
    fa = _sandbox(monkeypatch, tmp_path)
    p = fa._market_daily_path(date(2026, 4, 22))
    # Always YYYY/MM/YYYY-MM-DD.parquet relative to DATA_DIR/market_daily
    assert p.parent.name == "04"
    assert p.parent.parent.name == "2026"
    assert p.name == "2026-04-22.parquet"


# ── _merge_market_daily — the critical P0-2 semantics ─────────────────


def test_merge_drops_null_close_new(tmp_path, monkeypatch):
    fa = _sandbox(monkeypatch, tmp_path)
    d = date(2026, 4, 20)
    path = fa._market_daily_path(d)
    path.parent.mkdir(parents=True, exist_ok=True)
    # Prior has 3 healthy rows
    prior = pd.DataFrame([
        {"code": "600000", "ticker": "600000.SH", "trade_date": "2026-04-20",
         "open": 1.0, "close": 10.5, "high": 11.0, "low": 9.5, "volume": 1000.0},
        {"code": "600001", "ticker": "600001.SH", "trade_date": "2026-04-20",
         "open": 2.0, "close": 20.5, "high": 21.0, "low": 19.5, "volume": 2000.0},
        {"code": "600002", "ticker": "600002.SH", "trade_date": "2026-04-20",
         "open": 3.0, "close": 30.5, "high": 31.0, "low": 29.5, "volume": 3000.0},
    ])
    prior.to_parquet(path, compression="snappy", index=False)
    # New batch: one row with null close (broken) for 600000
    new = pd.DataFrame([
        {"code": "600000", "ticker": "600000.SH", "trade_date": "2026-04-20",
         "open": 1.0, "close": None, "high": 11.0, "low": 9.5, "volume": 1000.0},
        {"code": "600003", "ticker": "600003.SH", "trade_date": "2026-04-20",
         "open": 4.0, "close": 40.5, "high": 41.0, "low": 39.5, "volume": 4000.0},
    ])
    rows = fa._merge_market_daily(new, d)
    merged = pd.read_parquet(path)
    # 600000's HEALTHY prior row preserved (not overwritten by null-close new)
    assert len(merged) == 4  # 3 prior + 1 new 600003
    assert rows == 4
    # Ensure the 600000 row has close=10.5 (prior), NOT null
    row600000 = merged[merged["ticker"] == "600000.SH"].iloc[0]
    assert row600000["close"] == 10.5


def test_merge_new_row_wins_when_valid(tmp_path, monkeypatch):
    fa = _sandbox(monkeypatch, tmp_path)
    d = date(2026, 4, 20)
    path = fa._market_daily_path(d)
    path.parent.mkdir(parents=True, exist_ok=True)
    prior = pd.DataFrame([{
        "code": "600000", "ticker": "600000.SH", "trade_date": "2026-04-20",
        "open": 1.0, "close": 10.0, "high": 11.0, "low": 9.0, "volume": 100.0,
    }])
    prior.to_parquet(path, compression="snappy", index=False)
    # Same ticker, different close — new should win (it's the fresh fetch)
    new = pd.DataFrame([{
        "code": "600000", "ticker": "600000.SH", "trade_date": "2026-04-20",
        "open": 1.1, "close": 10.5, "high": 11.1, "low": 9.1, "volume": 105.0,
    }])
    fa._merge_market_daily(new, d)
    merged = pd.read_parquet(path)
    assert len(merged) == 1
    assert merged.iloc[0]["close"] == 10.5


def test_merge_empty_new_frame_is_noop(tmp_path, monkeypatch):
    fa = _sandbox(monkeypatch, tmp_path)
    d = date(2026, 4, 20)
    new = pd.DataFrame({
        "code": [], "ticker": [], "trade_date": [],
        "open": [], "close": [], "high": [], "low": [], "volume": [],
    })
    rows = fa._merge_market_daily(new, d)
    assert rows == 0
    # No parquet written
    assert not fa._market_daily_path(d).exists()


def test_git_helpers_noop_when_no_diff(tmp_path, monkeypatch):
    """_git_commit_local should return True (not raise) when there's
    nothing staged to commit."""
    fa = _sandbox(monkeypatch, tmp_path)
    # Set REPO_ROOT to a fake git repo
    repo = tmp_path / "repo"
    repo.mkdir()
    import subprocess
    subprocess.run(["git", "init", "-q", "-b", "main"], cwd=str(repo), check=True)
    subprocess.run(["git", "config", "user.email", "t@t"], cwd=str(repo), check=True)
    subprocess.run(["git", "config", "user.name", "t"], cwd=str(repo), check=True)
    # Empty commit so HEAD exists
    subprocess.run(["git", "commit", "--allow-empty", "-m", "init"],
                   cwd=str(repo), check=True)
    monkeypatch.setattr(fa, "REPO_ROOT", repo)
    (repo / "data").mkdir()
    assert fa._git_commit_local("test_ep", {"rows": 0}) is True


def test_git_commit_local_commits_diff(tmp_path, monkeypatch):
    """When there IS a data/ diff, commit should be created."""
    fa = _sandbox(monkeypatch, tmp_path)
    repo = tmp_path / "repo"
    repo.mkdir()
    import subprocess
    subprocess.run(["git", "init", "-q", "-b", "main"], cwd=str(repo), check=True)
    subprocess.run(["git", "config", "user.email", "t@t"], cwd=str(repo), check=True)
    subprocess.run(["git", "config", "user.name", "t"], cwd=str(repo), check=True)
    subprocess.run(["git", "commit", "--allow-empty", "-m", "init"], cwd=str(repo), check=True)
    monkeypatch.setattr(fa, "REPO_ROOT", repo)
    # Create a tracked data/ diff
    (repo / "data").mkdir()
    (repo / "data" / "endpoint.txt").write_text("hello")
    assert fa._git_commit_local("test_ep", {"rows": 42}) is True
    # Verify the commit exists
    log = subprocess.run(
        ["git", "log", "--oneline", "-1"],
        cwd=str(repo), capture_output=True, text=True
    )
    assert "test_ep" in log.stdout
    assert "42" in log.stdout


def test_merge_all_null_close_skipped(tmp_path, monkeypatch):
    """If the entire new batch has null close, we write nothing and
    return 0 — don't clobber prior."""
    fa = _sandbox(monkeypatch, tmp_path)
    d = date(2026, 4, 20)
    path = fa._market_daily_path(d)
    path.parent.mkdir(parents=True, exist_ok=True)
    prior = pd.DataFrame([{
        "code": "600000", "ticker": "600000.SH", "trade_date": "2026-04-20",
        "open": 1.0, "close": 10.0, "high": 11.0, "low": 9.0, "volume": 100.0,
    }])
    prior.to_parquet(path, compression="snappy", index=False)
    new = pd.DataFrame([{
        "code": "600000", "ticker": "600000.SH", "trade_date": "2026-04-20",
        "open": 1.0, "close": None, "high": 11.0, "low": 9.0, "volume": 100.0,
    }])
    rows = fa._merge_market_daily(new, d)
    assert rows == 0
    # Prior UNCHANGED
    after = pd.read_parquet(path)
    assert len(after) == 1
    assert after.iloc[0]["close"] == 10.0


# ── fetch_filings dedup ───────────────────────────────────────────────


def test_fetch_filings_dedups_pagination_overlap(tmp_path, monkeypatch):
    """Wave 10: fetch_filings loops over 7 days (cninfo truncates
    multi-day queries). Each per-day call returns 15 rows with 3
    unique, then the final dedup across all 7 days' responses keeps
    3 unique (same announcementId ⇒ same 公告链接).

    dup_dropped = 7×15 - 3 = 102.
    """
    fa = _sandbox(monkeypatch, tmp_path)

    unique_rows = []
    for ann_id in ("1001", "1002", "1003"):
        unique_rows.append({
            "代码": "605108", "简称": "同庆楼",
            "公告标题": f"公告标题{ann_id}",
            "公告时间": f"2026-04-23 10:{int(ann_id[-1]):02d}:00",
            "公告链接": f"http://www.cninfo.com.cn/new/disclosure/detail?stockCode=605108&announcementId={ann_id}&orgId=x",
        })
    raw = pd.DataFrame(unique_rows * 5)  # 15 rows per call, 3 unique
    assert len(raw) == 15

    monkeypatch.setattr(
        fa.ak, "stock_zh_a_disclosure_report_cninfo",
        lambda **kw: raw,
    )

    result = fa.fetch_filings()
    assert result["rows"] == 3
    # 7-day loop: 7 calls × 15 rows = 105, unique = 3, dropped = 102
    assert result["dup_dropped"] == 7 * 15 - 3
    assert result["days_ok"] == 7
    assert result["days_err"] == 0

    parquet = pd.read_parquet(fa.DATA_DIR / "filings" / "latest.parquet")
    assert len(parquet) == 3
    ann_ids = sorted(parquet["公告链接"].str.extract(r"announcementId=(\d+)")[0].tolist())
    assert ann_ids == ["1001", "1002", "1003"]


def test_fetch_filings_no_dupes_passthrough(tmp_path, monkeypatch):
    """Wave 10: 7-day loop × 2 clean unique rows per call = 14 rows
    before dedup, 2 after. dup_dropped = 12 (7-1 duplicates of the
    same two rows collapsing)."""
    fa = _sandbox(monkeypatch, tmp_path)
    clean = pd.DataFrame([
        {"代码": "000001", "简称": "平安银行",
         "公告标题": "公告A", "公告时间": "2026-04-23 09:00:00",
         "公告链接": "http://www.cninfo.com.cn/new/disclosure/detail?announcementId=2001"},
        {"代码": "600000", "简称": "浦发银行",
         "公告标题": "公告B", "公告时间": "2026-04-23 09:05:00",
         "公告链接": "http://www.cninfo.com.cn/new/disclosure/detail?announcementId=2002"},
    ])
    monkeypatch.setattr(
        fa.ak, "stock_zh_a_disclosure_report_cninfo",
        lambda **kw: clean,
    )
    result = fa.fetch_filings()
    assert result["rows"] == 2
    # 7 days × 2 rows = 14 raw; 2 unique; dropped = 12
    assert result["dup_dropped"] == 7 * 2 - 2
    assert result["days_ok"] == 7


def test_fetch_filings_empty_response(tmp_path, monkeypatch):
    """Empty upstream response is a clean no-op, not an error."""
    fa = _sandbox(monkeypatch, tmp_path)
    monkeypatch.setattr(
        fa.ak, "stock_zh_a_disclosure_report_cninfo",
        lambda **kw: pd.DataFrame(),
    )
    result = fa.fetch_filings()
    assert result["rows"] == 0
    assert "note" in result


def test_fetch_filings_iterates_seven_days(tmp_path, monkeypatch):
    """Wave 10: fetch_filings calls akshare 7 times (once per day)
    with matching start_date==end_date. This verifies cninfo's
    multi-day truncation workaround actually iterates."""
    from datetime import timedelta
    fa = _sandbox(monkeypatch, tmp_path)
    calls_by_date: list[tuple[str, str]] = []

    def _capture(**kw):
        calls_by_date.append((kw.get("start_date"), kw.get("end_date")))
        return pd.DataFrame([{
            "代码": "000001", "简称": "平安银行",
            "公告标题": f"公告-{kw.get('start_date')}",
            "公告时间": f"{kw.get('start_date')[:4]}-{kw.get('start_date')[4:6]}-{kw.get('start_date')[6:]} 09:00:00",
            "公告链接": f"http://example.com/d?announcementId={kw.get('start_date')}",
        }])

    monkeypatch.setattr(
        fa.ak, "stock_zh_a_disclosure_report_cninfo",
        _capture,
    )
    result = fa.fetch_filings()
    # Each call is single-day (start == end)
    assert len(calls_by_date) == 7
    for s, e in calls_by_date:
        assert s == e, f"single-day call expected, got start={s} end={e}"
    # 7 distinct dates covered
    dates = {s for s, _ in calls_by_date}
    assert len(dates) == 7
    # All 7 unique announcement_links preserved
    assert result["rows"] == 7
    assert result["days_ok"] == 7
    assert result["days_err"] == 0


def test_fetch_filings_partial_day_failures_still_succeed(tmp_path, monkeypatch):
    """If some days' fetches fail, remaining days still produce
    parquet. days_err counter surfaces partial health."""
    fa = _sandbox(monkeypatch, tmp_path)
    call_count = {"n": 0}

    def _flaky(**kw):
        call_count["n"] += 1
        # Fail every 3rd call (days 3, 6 in 1-indexed terms)
        if call_count["n"] % 3 == 0:
            raise RuntimeError("simulated akshare flake")
        return pd.DataFrame([{
            "代码": "000001", "简称": "平安银行",
            "公告标题": f"ok{call_count['n']}",
            "公告时间": "2026-04-23 09:00:00",
            "公告链接": f"http://example.com/d?announcementId={call_count['n']}",
        }])

    monkeypatch.setattr(
        fa.ak, "stock_zh_a_disclosure_report_cninfo",
        _flaky,
    )
    result = fa.fetch_filings()
    # Tenacity retries 3× per call — a day that always fails produces 3 errors
    # that share 1 day_err bucket (the per-day except catches after retries).
    # Of 7 days, days 3 and 6 raise after 3 retries each; 5 succeed with 1
    # unique row each.
    assert result["rows"] == 5
    assert result["days_ok"] == 5
    assert result["days_err"] == 2
