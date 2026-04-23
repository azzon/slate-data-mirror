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
    if not STATUS.exists():
        print("run=yes reason=no_status_file")
        return 0
    try:
        s = json.loads(STATUS.read_text())
    except Exception as e:  # noqa: BLE001
        print(f"run=yes reason=bad_status_{type(e).__name__}")
        return 0
    ep = s.get("endpoints", {}).get("financials", {})
    t = ep.get("last_success")
    if not t:
        print("run=yes reason=no_financials_success")
        return 0
    try:
        last = datetime.fromisoformat(t)
    except ValueError:
        print("run=yes reason=bad_timestamp")
        return 0
    age_d = (datetime.now(timezone.utc) - last).total_seconds() / 86400
    if age_d > 6:
        print(f"run=yes reason=stale_{age_d:.1f}d")
    else:
        print(f"run=no reason=fresh_{age_d:.1f}d")
    return 0


if __name__ == "__main__":
    sys.exit(main())
