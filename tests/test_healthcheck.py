"""Tests for scripts/healthcheck.py.

Healthcheck must correctly distinguish healthy state from three failure
modes: fail_streak, stale last_success, missing/corrupt status file.
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path


def _patch_status(tmp_path: Path, monkeypatch, payload: dict | None) -> None:
    import healthcheck
    p = tmp_path / "data" / "_status.json"
    p.parent.mkdir(parents=True, exist_ok=True)
    if payload is not None:
        p.write_text(json.dumps(payload))
    monkeypatch.setattr(healthcheck, "STATUS", p)


def test_missing_status_alerts(tmp_path, monkeypatch, capsys):
    _patch_status(tmp_path, monkeypatch, None)
    import healthcheck
    healthcheck.main()
    out = capsys.readouterr().out
    assert out.startswith("ALERT")


def test_corrupt_status_alerts(tmp_path, monkeypatch, capsys):
    import healthcheck
    p = tmp_path / "data" / "_status.json"
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text("{ broken")
    monkeypatch.setattr(healthcheck, "STATUS", p)
    healthcheck.main()
    out = capsys.readouterr().out
    assert out.startswith("ALERT")
    assert "unreadable" in out


def test_empty_endpoints_alerts(tmp_path, monkeypatch, capsys):
    _patch_status(tmp_path, monkeypatch, {"endpoints": {}})
    import healthcheck
    healthcheck.main()
    out = capsys.readouterr().out
    assert out.startswith("ALERT")
    assert "never ran" in out


def test_all_healthy_reports_ok(tmp_path, monkeypatch, capsys):
    fresh = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat()
    eps = {name: {"last_success": fresh, "fail_streak": 0}
           for name in ("securities", "market_daily", "macro", "news",
                        "north_flow", "lhb", "yjyg", "margin")}
    _patch_status(tmp_path, monkeypatch, {"endpoints": eps})
    import healthcheck
    healthcheck.main()
    out = capsys.readouterr().out
    assert not out.startswith("ALERT")
    assert "OK:" in out
    assert "8/8" in out


def test_fail_streak_triggers_alert(tmp_path, monkeypatch, capsys):
    fresh = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat()
    eps = {
        "market_daily": {"last_success": fresh, "fail_streak": 0},
        "news":         {"last_success": fresh, "fail_streak": 5,
                         "last_error": "ConnectionError"},
    }
    _patch_status(tmp_path, monkeypatch, {"endpoints": eps})
    import healthcheck
    healthcheck.main()
    out = capsys.readouterr().out
    assert out.startswith("ALERT")
    assert "news" in out
    assert "fail_streak=5" in out


def test_stale_endpoint_triggers_alert(tmp_path, monkeypatch, capsys):
    # market_daily cadence=4h → threshold = max(4*2, 48) = 48h. 72h > 48h.
    fresh = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat()
    stale = (datetime.now(timezone.utc) - timedelta(hours=72)).isoformat()
    eps = {
        "market_daily": {"last_success": stale, "fail_streak": 0},
        "news":         {"last_success": fresh, "fail_streak": 0},
    }
    _patch_status(tmp_path, monkeypatch, {"endpoints": eps})
    import healthcheck
    healthcheck.main()
    out = capsys.readouterr().out
    assert out.startswith("ALERT")
    assert "market_daily" in out
    assert "stale" in out


def test_slow_endpoint_generous_threshold(tmp_path, monkeypatch, capsys):
    """financials cadence=168h (weekly), threshold=336h=14d. A 10d stale
    financials should NOT trigger (weekly is allowed to lag).
    market_daily 10h old is fresh (< 48h)."""
    ten_d = (datetime.now(timezone.utc) - timedelta(days=10)).isoformat()
    ten_h = (datetime.now(timezone.utc) - timedelta(hours=10)).isoformat()
    eps = {
        "financials":   {"last_success": ten_d, "fail_streak": 0},
        "market_daily": {"last_success": ten_h, "fail_streak": 0},
    }
    _patch_status(tmp_path, monkeypatch, {"endpoints": eps})
    import healthcheck
    healthcheck.main()
    out = capsys.readouterr().out
    assert not out.startswith("ALERT")


def test_slow_endpoint_very_stale_alerts(tmp_path, monkeypatch, capsys):
    """financials 20d stale > 336h threshold → ALERT."""
    twenty_d = (datetime.now(timezone.utc) - timedelta(days=20)).isoformat()
    fresh = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
    eps = {
        "financials":   {"last_success": twenty_d, "fail_streak": 0},
        "market_daily": {"last_success": fresh,    "fail_streak": 0},
    }
    _patch_status(tmp_path, monkeypatch, {"endpoints": eps})
    import healthcheck
    healthcheck.main()
    out = capsys.readouterr().out
    assert out.startswith("ALERT")
    assert "financials" in out


def test_missing_last_success_alerts(tmp_path, monkeypatch, capsys):
    eps = {"market_daily": {"fail_streak": 0}}  # No last_success key
    _patch_status(tmp_path, monkeypatch, {"endpoints": eps})
    import healthcheck
    healthcheck.main()
    out = capsys.readouterr().out
    assert out.startswith("ALERT")
    assert "no last_success" in out


def test_row_count_floor_trips(tmp_path, monkeypatch, capsys):
    """Fresh last_success but rows below floor → ALERT."""
    fresh = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat()
    eps = {
        "securities": {
            "last_success": fresh, "fail_streak": 0,
            "last_meta": {"rows": 42},  # way below 4500 floor
        },
    }
    _patch_status(tmp_path, monkeypatch, {"endpoints": eps})
    import healthcheck
    healthcheck.main()
    out = capsys.readouterr().out
    assert out.startswith("ALERT")
    assert "rows=42" in out
    assert "floor=4500" in out


def test_row_count_floor_passes(tmp_path, monkeypatch, capsys):
    """Healthy row counts don't trip the floor."""
    fresh = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat()
    eps = {
        "securities": {
            "last_success": fresh, "fail_streak": 0,
            "last_meta": {"rows": 5648},
        },
    }
    _patch_status(tmp_path, monkeypatch, {"endpoints": eps})
    import healthcheck
    healthcheck.main()
    out = capsys.readouterr().out
    assert not out.startswith("ALERT")


def test_row_count_floor_absent_endpoint_no_alert(tmp_path, monkeypatch, capsys):
    """Endpoints without a MIN_ROWS entry (news, lhb) don't alert on low rows."""
    fresh = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat()
    eps = {
        "news": {
            "last_success": fresh, "fail_streak": 0,
            "last_meta": {"rows": 0},
        },
    }
    _patch_status(tmp_path, monkeypatch, {"endpoints": eps})
    import healthcheck
    healthcheck.main()
    out = capsys.readouterr().out
    assert not out.startswith("ALERT")


def test_stale_last_pass_alerts(tmp_path, monkeypatch, capsys):
    """Global heartbeat: if last_pass.at > 8h ago, alert regardless of
    individual endpoint states. Catches the 'single long workflow hogging
    the runner' scenario where individual endpoints look healthy but no
    cron pass actually fires."""
    fresh_ep = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat()
    old_pass = (datetime.now(timezone.utc) - timedelta(hours=12)).isoformat()
    _patch_status(tmp_path, monkeypatch, {
        "endpoints": {"news": {"last_success": fresh_ep, "fail_streak": 0}},
        "last_pass": {"at": old_pass, "succeeded": 8, "failed": 0},
    })
    import healthcheck
    healthcheck.main()
    out = capsys.readouterr().out
    assert out.startswith("ALERT")
    assert "_heartbeat" in out
    assert "12.0h" in out


def test_fresh_last_pass_no_alert(tmp_path, monkeypatch, capsys):
    """Recent last_pass → no heartbeat alert."""
    fresh = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat()
    _patch_status(tmp_path, monkeypatch, {
        "endpoints": {"news": {"last_success": fresh, "fail_streak": 0}},
        "last_pass": {"at": fresh, "succeeded": 8},
    })
    import healthcheck
    healthcheck.main()
    out = capsys.readouterr().out
    assert not out.startswith("ALERT")


def test_no_work_passes_exempt_from_row_floor(tmp_path, monkeypatch, capsys):
    """market_daily gap-heal legitimately returns rows=0 with
    no_work=True — must NOT trigger the rows-floor alert."""
    fresh = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat()
    eps = {
        "market_daily": {
            "last_success": fresh, "fail_streak": 0,
            "last_meta": {"rows": 0, "no_work": True},
        },
    }
    _patch_status(tmp_path, monkeypatch, {"endpoints": eps, "last_pass": {"at": fresh}})
    import healthcheck
    healthcheck.main()
    out = capsys.readouterr().out
    assert not out.startswith("ALERT")
