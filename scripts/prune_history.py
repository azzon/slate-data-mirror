#!/usr/bin/env python3
"""Prune history/ snapshots to bounded retention.

Each group's `latest.parquet` is always kept (that's the live view).
`history/YYYY-MM-DD.parquet` snapshots accumulate over time — without
pruning they'd push repo size past a few GB / year.

Retention policy:
  * Last 14 days: keep all daily snapshots
  * 14-120 days: keep Sundays only (weekly)
  * >120 days: delete

Run from the slow workflow after all slow endpoints have re-fetched.
"""

from __future__ import annotations

import logging
import subprocess
from datetime import date, datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = REPO_ROOT / "data"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("prune")

KEEP_DAILY_DAYS = 14
KEEP_WEEKLY_DAYS = 120


def _keep(d: date, today: date) -> bool:
    age_d = (today - d).days
    if age_d <= KEEP_DAILY_DAYS:
        return True
    if age_d <= KEEP_WEEKLY_DAYS:
        return d.weekday() == 6  # Sunday
    return False


def prune_group(group_dir: Path, today: date) -> int:
    """Return number of files deleted.

    Recognised filename shapes:
      * ``YYYY-MM-DD.parquet``          — plain daily snapshot
      * ``YYYY-MM-DD-cN.parquet``       — chunked endpoint (Wave 10
        financials rotation writes one per chunk, same date multiple
        files). The leading 10 chars are the date for retention.
    """
    history = group_dir / "history"
    if not history.exists():
        return 0
    deleted = 0
    for p in history.glob("*.parquet"):
        # First 10 chars = YYYY-MM-DD; ignore suffix (e.g. "-c3" on
        # chunked endpoints) so chunk files rotate out on the same
        # retention schedule as plain dailies.
        stem_prefix = p.stem[:10]
        try:
            d = datetime.strptime(stem_prefix, "%Y-%m-%d").date()
        except ValueError:
            continue
        if not _keep(d, today):
            p.unlink()
            deleted += 1
    return deleted


def main() -> int:
    today = date.today()
    total_deleted = 0
    for group in ("financials", "research", "news", "shareholders",
                  "north_flow", "lhb"):
        g = DATA_DIR / group
        if not g.exists():
            continue
        n = prune_group(g, today)
        if n:
            log.info("%s: deleted %d old snapshots", group, n)
            total_deleted += n

    if total_deleted == 0:
        log.info("nothing to prune")
        return 0

    r = subprocess.run(["git", "add", "data/"], cwd=REPO_ROOT, check=False, capture_output=True)
    if r.returncode != 0:
        log.warning("git add failed: %s", r.stderr.decode()[:200])
        return 1
    if subprocess.run(["git", "diff", "--cached", "--quiet"],
                      cwd=REPO_ROOT, check=False).returncode == 0:
        return 0
    subprocess.run(
        ["git", "commit", "-m", f"data: prune {total_deleted} old history snapshots"],
        cwd=REPO_ROOT, check=True
    )
    subprocess.run(["git", "push", "origin", "HEAD:main"], cwd=REPO_ROOT, check=False)
    log.info("pruned %d files total, pushed", total_deleted)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
