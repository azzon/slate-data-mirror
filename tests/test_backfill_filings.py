"""Tests for scripts/backfill_filings.py — resume + peak-skip logic.

Network-free: we stub out the akshare call and /proc time access.
"""
from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pandas as pd


def _fresh(monkeypatch, tmp_path: Path):
    """Import backfill_filings with DATA_DIR redirected to tmp_path."""
    import backfill_filings as bf
    data_dir = tmp_path / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(bf, "DATA_DIR", data_dir)
    monkeypatch.setattr(bf, "PROGRESS_FILE", data_dir / "_filings_backfill_progress.json")
    # Pin clock outside peak window
    monkeypatch.setattr(bf, "_is_peak_window", lambda: False)
    return bf


def _stub_fetcher(monkeypatch, bf, *, rows_per_call: int = 3,
                  fail_dates: set | None = None):
    """Replace bf.ak.stock_zh_a_disclosure_report_cninfo.

    Returns rows_per_call rows per successful day. Dates in fail_dates
    raise RuntimeError (triggers _fetch_one_day's retry + give-up path).
    """
    fail_dates = fail_dates or set()

    def _fake(**kw):
        ds = kw.get("start_date")
        if ds in fail_dates:
            raise RuntimeError(f"simulated failure for {ds}")
        return pd.DataFrame([
            {
                "代码": f"{i:06d}",
                "简称": f"T{i}",
                "公告标题": f"ann-{ds}-{i}",
                "公告时间": f"{ds[:4]}-{ds[4:6]}-{ds[6:]} 09:00:00",
                "公告链接": f"http://x.com/d?announcementId={ds}-{i}",
            }
            for i in range(rows_per_call)
        ])
    monkeypatch.setattr(bf.ak, "stock_zh_a_disclosure_report_cninfo", _fake)
    # Also stub subprocess.run so git calls are no-ops
    monkeypatch.setattr(bf.subprocess, "run",
                        lambda *a, **kw: type("R", (), {"returncode": 0, "stderr": ""})())


def test_backfill_writes_per_day_parquets(tmp_path, monkeypatch):
    bf = _fresh(monkeypatch, tmp_path)
    _stub_fetcher(monkeypatch, bf, rows_per_call=2)
    monkeypatch.setattr(bf, "AKSHARE_SLEEP", 0.0)

    start = date(2026, 4, 1)
    end = date(2026, 4, 5)
    rc = bf.backfill(start, end, batch_days=100)
    assert rc == 0

    # Each day (5 days) gets a parquet
    history = tmp_path / "data" / "filings" / "history"
    files = sorted(p.stem for p in history.glob("*.parquet"))
    assert files == [
        "2026-04-01", "2026-04-02", "2026-04-03",
        "2026-04-04", "2026-04-05",
    ]

    # Progress file records last_done
    progress = json.loads((tmp_path / "data" / "_filings_backfill_progress.json").read_text())
    assert progress["last_done"] == "2026-04-05"
    assert progress["completed_days"] == 5


def test_backfill_resumes_from_progress(tmp_path, monkeypatch):
    bf = _fresh(monkeypatch, tmp_path)
    monkeypatch.setattr(bf, "AKSHARE_SLEEP", 0.0)

    # Pre-seed progress to "we already did through 2026-04-10"
    (tmp_path / "data").mkdir(parents=True, exist_ok=True)
    (tmp_path / "data" / "_filings_backfill_progress.json").write_text(
        json.dumps({"last_done": "2026-04-10", "start": "2026-04-01",
                    "completed_days": 10})
    )
    # Also pre-create the parquets up to 2026-04-10 so the skip branch fires
    history = tmp_path / "data" / "filings" / "history"
    history.mkdir(parents=True, exist_ok=True)
    for d in pd.date_range("2026-04-01", "2026-04-10"):
        pd.DataFrame([{"x": 1}]).to_parquet(
            history / f"{d.strftime('%Y-%m-%d')}.parquet", index=False,
        )

    calls = []

    def _tracking(**kw):
        calls.append(kw.get("start_date"))
        return pd.DataFrame([{
            "代码": "000001", "简称": "T",
            "公告标题": f"ann-{kw.get('start_date')}",
            "公告时间": f"2026-04-{kw.get('start_date')[-2:]} 09:00:00",
            "公告链接": f"http://x.com/d?id={kw.get('start_date')}",
        }])
    monkeypatch.setattr(bf.ak, "stock_zh_a_disclosure_report_cninfo", _tracking)
    monkeypatch.setattr(bf.subprocess, "run",
                        lambda *a, **kw: type("R", (), {"returncode": 0, "stderr": ""})())

    rc = bf.backfill(date(2026, 4, 1), date(2026, 4, 15), batch_days=100)
    assert rc == 0
    # Only days 2026-04-11 through 2026-04-15 should have been fetched
    fetched_dates = sorted(calls)
    assert fetched_dates == [
        "20260411", "20260412", "20260413", "20260414", "20260415",
    ]


def test_backfill_aborts_on_too_many_errors(tmp_path, monkeypatch):
    bf = _fresh(monkeypatch, tmp_path)
    monkeypatch.setattr(bf, "AKSHARE_SLEEP", 0.0)
    # Also neutralise retry delays
    import time as _time
    monkeypatch.setattr(_time, "sleep", lambda s: None)
    # Every day fails
    _stub_fetcher(monkeypatch, bf, fail_dates={
        "20260401", "20260402", "20260403", "20260404", "20260405",
    })
    # With max_errors=3 we should stop after ~3 failures
    rc = bf.backfill(date(2026, 4, 1), date(2026, 4, 10),
                     batch_days=100, max_errors=3)
    assert rc == 1
    # Progress should still exist (so next run can try again)
    assert (tmp_path / "data" / "_filings_backfill_progress.json").exists()
