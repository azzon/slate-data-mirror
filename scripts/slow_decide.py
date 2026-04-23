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

  Sun primary:  financials + shareholders     (anchor: financials)
  Wed primary:  research                      (anchor: research)
  Fri primary:  concepts + industries         (anchor: concepts)
"""
from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

STATUS = Path("data/_status.json")

# anchor_endpoint → (endpoints_to_run, human_label)
CHUNKS = [
    ("financials",   "financials,shareholders", "fin+shareholders"),
    ("research",     "research",                "research"),
    ("concepts",     "concepts,industries",     "concepts+industries"),
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
    stalest: tuple[float, str, str] | None = None  # (age_d, endpoints, label)
    for anchor, endpoints, label in CHUNKS:
        ep = eps.get(anchor, {})
        t = ep.get("last_success")
        if not t:
            age_d = 999.0  # Never run → infinitely stale
        else:
            try:
                age_d = (now - datetime.fromisoformat(t)).total_seconds() / 86400
            except ValueError:
                age_d = 999.0
        if age_d > 6 and (stalest is None or age_d > stalest[0]):
            stalest = (age_d, endpoints, label)

    if stalest is None:
        emit("no", "all_chunks_fresh")
    else:
        age_d, endpoints, label = stalest
        emit("yes", f"stale_{label}_{age_d:.1f}d", endpoints)
    return 0


if __name__ == "__main__":
    sys.exit(main())
