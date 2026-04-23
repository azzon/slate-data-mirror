"""Tests for scripts/slow_decide.py.

Slow is now chunked (Sun/Wed/Fri each run a different subset). The
retry cron picks the stalest chunk. Regression here = wrong chunk
runs or retry doesn't fire when it should.
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path


def _patch_status(tmp_path: Path, monkeypatch, payload: dict | None) -> None:
    import slow_decide
    status_path = tmp_path / "data" / "_status.json"
    status_path.parent.mkdir(parents=True, exist_ok=True)
    if payload is not None:
        status_path.write_text(json.dumps(payload))
    monkeypatch.setattr(slow_decide, "STATUS", status_path)


def _capture(capsys):
    out = capsys.readouterr().out.strip()
    return {line.split("=", 1)[0]: line.split("=", 1)[1]
            for line in out.splitlines() if "=" in line}


def test_missing_status_picks_first_chunk(tmp_path, monkeypatch, capsys):
    import slow_decide
    _patch_status(tmp_path, monkeypatch, None)
    slow_decide.main()
    got = _capture(capsys)
    assert got["run"] == "yes"
    # Falls back to first CHUNK
    assert "financials" in got["endpoints"]


def test_bad_json_picks_first_chunk(tmp_path, monkeypatch, capsys):
    import slow_decide
    status_path = tmp_path / "data" / "_status.json"
    status_path.parent.mkdir(parents=True, exist_ok=True)
    status_path.write_text("{ invalid")
    monkeypatch.setattr(slow_decide, "STATUS", status_path)
    slow_decide.main()
    got = _capture(capsys)
    assert got["run"] == "yes"
    assert got["reason"].startswith("bad_status_")
    assert "financials" in got["endpoints"]


def test_all_chunks_fresh_skips(tmp_path, monkeypatch, capsys):
    fresh = (datetime.now(timezone.utc) - timedelta(days=3)).isoformat()
    _patch_status(tmp_path, monkeypatch, {"endpoints": {
        "financials": {"last_success": fresh},
        "research":   {"last_success": fresh},
        "concepts":   {"last_success": fresh},
    }})
    import slow_decide
    slow_decide.main()
    got = _capture(capsys)
    assert got["run"] == "no"


def test_stale_research_picks_research(tmp_path, monkeypatch, capsys):
    fresh = (datetime.now(timezone.utc) - timedelta(days=3)).isoformat()
    old = (datetime.now(timezone.utc) - timedelta(days=8)).isoformat()
    _patch_status(tmp_path, monkeypatch, {"endpoints": {
        "financials": {"last_success": fresh},
        "research":   {"last_success": old},
        "concepts":   {"last_success": fresh},
    }})
    import slow_decide
    slow_decide.main()
    got = _capture(capsys)
    assert got["run"] == "yes"
    assert got["endpoints"] == "research"


def test_multiple_stale_picks_stalest(tmp_path, monkeypatch, capsys):
    three_d = (datetime.now(timezone.utc) - timedelta(days=3)).isoformat()
    eight_d = (datetime.now(timezone.utc) - timedelta(days=8)).isoformat()
    twenty_d = (datetime.now(timezone.utc) - timedelta(days=20)).isoformat()
    _patch_status(tmp_path, monkeypatch, {"endpoints": {
        "financials": {"last_success": eight_d},   # 8 days stale
        "research":   {"last_success": twenty_d},  # 20 days — stalest
        "concepts":   {"last_success": three_d},   # fresh
    }})
    import slow_decide
    slow_decide.main()
    got = _capture(capsys)
    assert got["run"] == "yes"
    # Stalest is research
    assert got["endpoints"] == "research"


def test_never_run_anchor_picks_that_chunk(tmp_path, monkeypatch, capsys):
    fresh = (datetime.now(timezone.utc) - timedelta(days=3)).isoformat()
    # research has no last_success at all → age 999d (stalest)
    _patch_status(tmp_path, monkeypatch, {"endpoints": {
        "financials": {"last_success": fresh},
        "concepts":   {"last_success": fresh},
    }})
    import slow_decide
    slow_decide.main()
    got = _capture(capsys)
    assert got["run"] == "yes"
    assert got["endpoints"] == "research"


def test_boundary_six_days_skips(tmp_path, monkeypatch, capsys):
    """Exactly 5d23h ago = fresh. 6d0h = stale."""
    fresh = (datetime.now(timezone.utc) - timedelta(days=5, hours=23)).isoformat()
    _patch_status(tmp_path, monkeypatch, {"endpoints": {
        "financials": {"last_success": fresh},
        "research":   {"last_success": fresh},
        "concepts":   {"last_success": fresh},
    }})
    import slow_decide
    slow_decide.main()
    got = _capture(capsys)
    assert got["run"] == "no"
