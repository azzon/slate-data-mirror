#!/usr/bin/env python3
"""Decide which slow chunk (if any) the daily retry cron should run.

Reads data/_status.json and emits on stdout, one key=value per line:
    run=yes|no
    reason=<reason>
    endpoints=<comma-separated list>  (only if run=yes)

Called from the workflow's 'Decide whether to run' step.

Chunk definitions — each is a weekly cadence with a different primary
cron day. The retry cron picks whichever chunk has the stalest anchor
endpoint (>6 days since last success). Running one chunk per trigger
keeps each workflow job under 4h (was 7h serial).

  Daily primary: financials chunk i=day%7       (anchor: financials)
  Wed primary:   research                       (anchor: research)
  Fri primary:   concepts + industries          (anchor: concepts)
  Sat primary:   shareholders                   (anchor: shareholders)

Wave 10: financials moved from weekly to daily (chunked 1/7 universe
per day) after the 3h weekly run kept timing out on the 2-core / 2G
ECS runner. Each chunk is ~20min, well within the 4h workflow cap.

The retry cron inspects each anchor's staleness and re-runs the most
stale chunk. financials anchor is now 1d stale-threshold (not 6d)
so a missed day gets picked up the next morning instead of waiting
until the following week.
"""
from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

STATUS = Path("data/_status.json")

# anchor_endpoint → (endpoints_to_run, human_label, stale_threshold_days).
# Daily anchors use a 1-day stale threshold; weekly anchors use 6 days
# (allowing a primary cron to have fired plus one day of slack).
#
# concepts + industries are intentionally omitted from the retry-cron
# rotation: Eastmoney's 79.push2 endpoint consistently RemoteDisconnects
# on the ECS runner IP (classic IP-category block for
# financial-aggregator endpoints — we hit the same pattern before on
# stock_zh_a_hist before switching to Tencent gtimg). Slate doesn't
# actually consume concepts/industries parquets today, so keeping them
# in the retry queue just wastes 2-3× daily cron slots on a guaranteed
# failure. The Friday primary cron still attempts them once/week, which
# is fine if Eastmoney later unblocks the IP. If you want to re-enable
# the retry, add ("concepts", "concepts,industries", ..., 6.0) back.
CHUNKS = [
    ("financials",   "financials",            "financials",          1.1),
    ("research",     "research",              "research",            6.0),
    ("shareholders", "shareholders",          "shareholders",        6.0),
]


def main() -> int:
    def emit(run: str, reason: str, endpoints: str = "") -> None:
        print(f"run={run}")
        print(f"reason={reason}")
        if endpoints:
            print(f"endpoints={endpoints}")

    if not STATUS.exists():
        # Fresh clone — run everything we can fit. Pick research since
        # it's the longest and will block other chunks until done.
        emit("yes", "no_status_file", CHUNKS[0][1])
        return 0
    try:
        s = json.loads(STATUS.read_text())
    except Exception as e:  # noqa: BLE001
        emit("yes", f"bad_status_{type(e).__name__}", CHUNKS[0][1])
        return 0

    eps = s.get("endpoints", {})
    now = datetime.now(timezone.utc)
    # Find stalest chunk (by anchor-endpoint last_success age).
    # Compare each chunk's age to its OWN stale threshold (days):
    # daily anchors trigger at 1d stale, weekly anchors at 6d.
    stalest: tuple[float, str, str] | None = None  # (age_d, endpoints, label)
    for anchor, endpoints, label, threshold_d in CHUNKS:
        ep = eps.get(anchor, {})
        t = ep.get("last_success")
        if not t:
            age_d = 999.0  # Never run → infinitely stale
        else:
            try:
                age_d = (now - datetime.fromisoformat(t)).total_seconds() / 86400
            except ValueError:
                age_d = 999.0
        if age_d > threshold_d and (stalest is None or age_d > stalest[0]):
            stalest = (age_d, endpoints, label)

    if stalest is None:
        emit("no", "all_chunks_fresh")
    else:
        age_d, endpoints, label = stalest
        emit("yes", f"stale_{label}_{age_d:.1f}d", endpoints)
    return 0


if __name__ == "__main__":
    sys.exit(main())
