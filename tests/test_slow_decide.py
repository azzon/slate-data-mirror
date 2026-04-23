"""Tests for scripts/slow_decide.py.

Wave 10 schema: CHUNKS is a 4-tuple (anchor, endpoints, label,
stale_threshold_d). Four anchors with distinct thresholds:

  financials     1.1d  (daily chunked — 1/7 universe per day)
  research       6.0d  (weekly)
  concepts       6.0d  (weekly — anchors concepts+industries)
  shareholders   6.0d  (weekly)

Regression here = wrong chunk runs or retry doesn't fire when it
should, especially the daily financials slot vs weekly siblings.
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
    """Every anchor under its own threshold → skip.

    Financials' threshold is 1.1d (daily); research/concepts/
    shareholders are 6d. So 'fresh' here means <1.1d for financials
    and <6d for the rest — use a half-day-old timestamp which passes
    BOTH thresholds.
    """
    fresh = (datetime.now(timezone.utc) - timedelta(hours=12)).isoformat()
    _patch_status(tmp_path, monkeypatch, {"endpoints": {
        "financials":   {"last_success": fresh},
        "research":     {"last_success": fresh},
        "concepts":     {"last_success": fresh},
        "shareholders": {"last_success": fresh},
    }})
    import slow_decide
    slow_decide.main()
    got = _capture(capsys)
    assert got["run"] == "no"


def test_stale_research_picks_research(tmp_path, monkeypatch, capsys):
    """Only research is stale (>6d). financials under its 1.1d
    threshold, others fresh."""
    fresh = (datetime.now(timezone.utc) - timedelta(hours=12)).isoformat()
    old = (datetime.now(timezone.utc) - timedelta(days=8)).isoformat()
    _patch_status(tmp_path, monkeypatch, {"endpoints": {
        "financials":   {"last_success": fresh},
        "research":     {"last_success": old},
        "concepts":     {"last_success": fresh},
        "shareholders": {"last_success": fresh},
    }})
    import slow_decide
    slow_decide.main()
    got = _capture(capsys)
    assert got["run"] == "yes"
    assert got["endpoints"] == "research"


def test_multiple_stale_picks_stalest(tmp_path, monkeypatch, capsys):
    """Research at 20d beats financials at 8d (even though financials'
    1.1d threshold is exceeded 8× vs research's 6d threshold exceeded
    3×, the comparison is on raw age_d not ratio-to-threshold)."""
    half_d = (datetime.now(timezone.utc) - timedelta(hours=12)).isoformat()
    eight_d = (datetime.now(timezone.utc) - timedelta(days=8)).isoformat()
    twenty_d = (datetime.now(timezone.utc) - timedelta(days=20)).isoformat()
    _patch_status(tmp_path, monkeypatch, {"endpoints": {
        "financials":   {"last_success": eight_d},   # 8d stale (>>1.1d threshold)
        "research":     {"last_success": twenty_d},  # 20d — stalest (>>6d threshold)
        "concepts":     {"last_success": half_d},    # fresh
        "shareholders": {"last_success": half_d},    # fresh
    }})
    import slow_decide
    slow_decide.main()
    got = _capture(capsys)
    assert got["run"] == "yes"
    # Stalest raw age is research (20d)
    assert got["endpoints"] == "research"


def test_never_run_anchor_picks_that_chunk(tmp_path, monkeypatch, capsys):
    fresh = (datetime.now(timezone.utc) - timedelta(hours=12)).isoformat()
    # research has no last_success at all → age 999d (stalest)
    _patch_status(tmp_path, monkeypatch, {"endpoints": {
        "financials":   {"last_success": fresh},
        "concepts":     {"last_success": fresh},
        "shareholders": {"last_success": fresh},
    }})
    import slow_decide
    slow_decide.main()
    got = _capture(capsys)
    assert got["run"] == "yes"
    assert got["endpoints"] == "research"


def test_boundary_six_days_skips(tmp_path, monkeypatch, capsys):
    """Exactly 5d23h ago = fresh for weekly anchors. financials'
    threshold is 1.1d so it uses a different fresh value."""
    weekly_fresh = (datetime.now(timezone.utc) - timedelta(days=5, hours=23)).isoformat()
    daily_fresh = (datetime.now(timezone.utc) - timedelta(hours=12)).isoformat()
    _patch_status(tmp_path, monkeypatch, {"endpoints": {
        "financials":   {"last_success": daily_fresh},
        "research":     {"last_success": weekly_fresh},
        "concepts":     {"last_success": weekly_fresh},
        "shareholders": {"last_success": weekly_fresh},
    }})
    import slow_decide
    slow_decide.main()
    got = _capture(capsys)
    assert got["run"] == "no"


def test_financials_1_day_stale_triggers(tmp_path, monkeypatch, capsys):
    """Wave 10 daily financials: 1d7h stale > 1.1d threshold → fire."""
    financials_stale = (datetime.now(timezone.utc) - timedelta(days=1, hours=7)).isoformat()
    fresh = (datetime.now(timezone.utc) - timedelta(hours=12)).isoformat()
    _patch_status(tmp_path, monkeypatch, {"endpoints": {
        "financials":   {"last_success": financials_stale},
        "research":     {"last_success": fresh},
        "concepts":     {"last_success": fresh},
        "shareholders": {"last_success": fresh},
    }})
    import slow_decide
    slow_decide.main()
    got = _capture(capsys)
    assert got["run"] == "yes"
    assert got["endpoints"] == "financials"
