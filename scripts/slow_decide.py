#!/usr/bin/env python3
"""Decide whether mirror-slow.yml should run now.

Reads data/_status.json and emits `run=yes|no reason=...` on stdout.
Called from the workflow's 'Decide whether to run' step.

Decision policy:
  * primary cron (Sun)          — always run (workflow handles that via env)
  * manual dispatch / force     — always run
  * retry cron (Mon-Sat)        — run iff financials last_success > 6 days
                                   or missing entirely
"""
from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

STATUS = Path("data/_status.json")


def main() -> int:
    def emit(run: str, reason: str) -> None:
        print(f"run={run}")
        print(f"reason={reason}")

    if not STATUS.exists():
        emit("yes", "no_status_file")
        return 0
    try:
        s = json.loads(STATUS.read_text())
    except Exception as e:  # noqa: BLE001
        emit("yes", f"bad_status_{type(e).__name__}")
        return 0
    ep = s.get("endpoints", {}).get("financials", {})
    t = ep.get("last_success")
    if not t:
        emit("yes", "no_financials_success")
        return 0
    try:
        last = datetime.fromisoformat(t)
    except ValueError:
        emit("yes", "bad_timestamp")
        return 0
    age_d = (datetime.now(timezone.utc) - last).total_seconds() / 86400
    # One key=value per line so the bash caller's while-read loop is
    # whitespace-safe. Don't change the "run=yes|no" key — the workflow
    # greps for it.
    if age_d > 6:
        print("run=yes")
        print(f"reason=stale_{age_d:.1f}d")
    else:
        print("run=no")
        print(f"reason=fresh_{age_d:.1f}d")
    return 0


if __name__ == "__main__":
    sys.exit(main())
