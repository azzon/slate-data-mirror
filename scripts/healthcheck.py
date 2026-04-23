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

# Row-count floor per endpoint — catches "silently empty" cases that
# have fresh last_success but near-zero rows (upstream schema break
# returning empty DFs without raising, or a partial fetch the row
# guards in fetch_all didn't catch). Keys match the `rows` key in
# _status.json.endpoints[X].last_meta. None = no floor.
MIN_ROWS = {
    "securities": 4500,       # A-share market should be >5000
    "market_daily": 4000,     # daily bars; 16:30 run has all publishers
    "macro": 100,             # 4 series × historical depth
    "north_flow": 100,        # northbound holdings — always a few hundred
    "research": 50,           # per-ticker reports; 0 = schema break
    "financials": 50000,      # 5500 tickers × ~12 indicators each
    "concepts": 50,           # board count floor
    "industries": 50,
    "shareholders": 100,
    # news/lhb/yjyg/margin legitimately have empty days — no floor
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

    # Global heartbeat check — `last_pass.at` is when ANY mirror pass
    # finished. If it's been >8h since ANY pass (fast, market, slow)
    # touched the repo, the runner or workflow scheduling itself is
    # the problem, not a specific endpoint. Catches the case where a
    # single long-running workflow (e.g. slow) is hogging the runner
    # and all the fast crons are queueing/cancelling.
    last_pass = data.get("last_pass", {})
    last_pass_at = last_pass.get("at")
    if last_pass_at:
        try:
            pass_age_h = (now - datetime.fromisoformat(last_pass_at)).total_seconds() / 3600
            if pass_age_h > 8:
                issues.append(
                    f"- `_heartbeat`: last_pass was {pass_age_h:.1f}h ago "
                    f"(>8h threshold) — check runner / slow-workflow contention"
                )
        except ValueError:
            issues.append(f"- `_heartbeat`: bad last_pass.at `{last_pass_at}`")

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

        # Row-count floor — catches "fresh timestamp but empty/near-empty
        # rows" silent failure where upstream schema breaks returned DFs
        # with no data. Rows live under last_meta.rows.
        #
        # Exempt no_work passes: a market_daily gap-heal that finds
        # nothing pending legitimately returns rows=0. Those set
        # last_meta.no_work=True. Don't alert on legitimate no-ops.
        last_meta = ep.get("last_meta") or {}
        if last_meta.get("no_work"):
            ok_count += 1
            continue
        floor = MIN_ROWS.get(name)
        if floor is not None:
            rows = last_meta.get("rows")
            if rows is not None and rows < floor:
                issues.append(
                    f"- `{name}`: rows={rows} < floor={floor} (upstream schema change?)"
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
