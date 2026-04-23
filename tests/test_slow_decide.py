"""Tests for scripts/slow_decide.py.

The retry-cron logic decides whether to run the weekly slow workflow
mid-week based on financials.last_success age. A regression here means
the retry either never fires (missed weekly run goes unhealed) or
fires every day (rate-limit risk + wasted compute).
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest


def _patch_status(tmp_path: Path, monkeypatch, payload: dict | None) -> None:
    import slow_decide
    status_path = tmp_path / "data" / "_status.json"
    status_path.parent.mkdir(parents=True, exist_ok=True)
    if payload is not None:
        status_path.write_text(json.dumps(payload))
    monkeypatch.setattr(slow_decide, "STATUS", status_path)


def _capture(capsys):
    """Return {key: value} from slow_decide's stdout."""
    out = capsys.readouterr().out.strip()
    return {line.split("=", 1)[0]: line.split("=", 1)[1]
            for line in out.splitlines() if "=" in line}


def test_missing_status_file_runs(tmp_path, monkeypatch, capsys):
    import slow_decide
    _patch_status(tmp_path, monkeypatch, None)
    slow_decide.main()
    got = _capture(capsys)
    assert got["run"] == "yes"
    assert "no_status_file" in got["reason"]


def test_bad_json_runs(tmp_path, monkeypatch, capsys):
    import slow_decide
    status_path = tmp_path / "data" / "_status.json"
    status_path.parent.mkdir(parents=True, exist_ok=True)
    status_path.write_text("{ this is not json")
    monkeypatch.setattr(slow_decide, "STATUS", status_path)
    slow_decide.main()
    got = _capture(capsys)
    assert got["run"] == "yes"
    assert got["reason"].startswith("bad_status_")


def test_no_financials_entry_runs(tmp_path, monkeypatch, capsys):
    _patch_status(tmp_path, monkeypatch, {"endpoints": {"market_daily": {"last_success": "2026-04-23T00:00:00+00:00"}}})
    import slow_decide
    slow_decide.main()
    got = _capture(capsys)
    assert got["run"] == "yes"
    assert got["reason"] == "no_financials_success"


def test_fresh_financials_skips(tmp_path, monkeypatch, capsys):
    fresh = (datetime.now(timezone.utc) - timedelta(days=3)).isoformat()
    _patch_status(tmp_path, monkeypatch, {"endpoints": {"financials": {"last_success": fresh}}})
    import slow_decide
    slow_decide.main()
    got = _capture(capsys)
    assert got["run"] == "no"
    assert got["reason"].startswith("fresh_")


def test_stale_financials_runs(tmp_path, monkeypatch, capsys):
    old = (datetime.now(timezone.utc) - timedelta(days=8)).isoformat()
    _patch_status(tmp_path, monkeypatch, {"endpoints": {"financials": {"last_success": old}}})
    import slow_decide
    slow_decide.main()
    got = _capture(capsys)
    assert got["run"] == "yes"
    assert got["reason"].startswith("stale_")


def test_exactly_six_days_is_fresh(tmp_path, monkeypatch, capsys):
    """Boundary: exactly 6d should be FRESH (not > 6). Retry cron shouldn't
    fire when the primary Sunday just successfully ran."""
    boundary = (datetime.now(timezone.utc) - timedelta(days=5, hours=23)).isoformat()
    _patch_status(tmp_path, monkeypatch, {"endpoints": {"financials": {"last_success": boundary}}})
    import slow_decide
    slow_decide.main()
    got = _capture(capsys)
    assert got["run"] == "no"


def test_bad_timestamp_runs(tmp_path, monkeypatch, capsys):
    _patch_status(tmp_path, monkeypatch, {"endpoints": {"financials": {"last_success": "not a date"}}})
    import slow_decide
    slow_decide.main()
    got = _capture(capsys)
    assert got["run"] == "yes"
    assert got["reason"] == "bad_timestamp"
