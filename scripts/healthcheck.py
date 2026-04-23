#!/usr/bin/env python3
"""Analyse data/_status.json and emit an alert block if unhealthy.

Called from .github/workflows/healthcheck.yml. Prints a human-readable
report to stdout. If any endpoint is unhealthy, the FIRST line starts
with 'ALERT' so the bash caller can tell whether to open an issue.

Health rules:
  * fail_streak >= 3           → ALERT
  * age > 2× cadence_h         → ALERT
  * no endpoints at all        → ALERT (status file empty)
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

STATUS = Path("data/_status.json")

# Cadence per endpoint in hours. Must stay in sync with fetch_all.py::ENDPOINTS.
CADENCE_H = {
    "securities": 20,
    "market_daily": 4,
    "macro": 20,
    "news": 4,
    "north_flow": 20,
    "lhb": 20,
    "yjyg": 20,
    "margin": 20,
    "financials": 24 * 7,
    "shareholders": 24 * 7,
    "concepts": 24 * 7,
    "industries": 24 * 7,
    "research": 24 * 7,
}


def main() -> int:
    if not STATUS.exists():
        print("ALERT: data/_status.json missing entirely")
        return 0
    try:
        data = json.loads(STATUS.read_text())
    except Exception as e:  # noqa: BLE001
        print(f"ALERT: _status.json unreadable — {type(e).__name__}: {e}")
        return 0

    eps = data.get("endpoints", {})
    if not eps:
        print("ALERT: _status.json has no endpoints — mirror never ran successfully")
        return 0

    issues: list[str] = []
    ok_count = 0
    now = datetime.now(timezone.utc)

    for name, ep in sorted(eps.items()):
        cadence_h = CADENCE_H.get(name, 24)
        fail_streak = ep.get("fail_streak", 0)
        last_success = ep.get("last_success")
        last_error = ep.get("last_error")

        if fail_streak >= 3:
            issues.append(
                f"- `{name}`: fail_streak={fail_streak}, last_error=`{last_error}`"
            )
            continue

        if not last_success:
            issues.append(f"- `{name}`: no last_success recorded")
            continue

        try:
            age_h = (now - datetime.fromisoformat(last_success)).total_seconds() / 3600
        except ValueError:
            issues.append(f"- `{name}`: bad last_success timestamp `{last_success}`")
            continue

        # Alert when >2× cadence AND ≥48h — the 48h floor avoids alerting
        # on fast endpoints that legitimately go 9h between cron triggers.
        threshold_h = max(cadence_h * 2, 48)
        if age_h > threshold_h:
            issues.append(
                f"- `{name}`: stale {age_h:.0f}h (> {threshold_h}h threshold, cadence {cadence_h}h)"
            )
            continue

        ok_count += 1

    if issues:
        print("ALERT: mirror health degraded")
        print()
        print(f"Healthy: {ok_count}/{len(eps)} endpoints")
        print(f"Issues: {len(issues)}")
        print()
        for line in issues:
            print(line)
        print()
        last_pass = data.get("last_pass", {})
        if last_pass:
            print(f"Last pass: {last_pass}")
    else:
        print(f"OK: all {ok_count}/{len(eps)} endpoints healthy")

    return 0


if __name__ == "__main__":
    sys.exit(main())
