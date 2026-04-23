#!/usr/bin/env bash
# Shallow-clone checkout with automatic re-shallow when .git grows
# past the budget. The mirror repo writes ~70MB/day of new pack data;
# without this, every self-hosted runner fills its disk in ~6 months.
#
# Strategy:
#   1. If .git doesn't exist → fresh shallow clone.
#   2. If .git exists and is UNDER the budget → normal fetch --depth=1.
#   3. If .git exists and is OVER the budget → delete .git entirely,
#      re-clone fresh with depth=1. Preserves working-copy files
#      (.venv, scripts, data/) via a temp-dir dance.
#
# Usage:
#   WORK=${GITHUB_WORKSPACE:-$(pwd)}
#   REPO_URL=git@github.com:azzon/slate-data-mirror.git
#   GIT_BUDGET_MB=300  # over this, re-shallow
#   bash /home/runner/mirror-checkout.sh

set -euo pipefail

WORK="${WORK:-${GITHUB_WORKSPACE:-$(pwd)}}"
REPO_URL="${REPO_URL:-git@github.com:azzon/slate-data-mirror.git}"
GIT_BUDGET_MB="${GIT_BUDGET_MB:-300}"

mkdir -p "$WORK"
cd "$WORK"

reshallow() {
    # Force a fresh shallow clone while preserving untracked working
    # files (.venv / data / caches / scripts etc). We move .git aside,
    # clone into a throwaway dir, and swap the .git back.
    echo "[checkout] re-shallowing: .git size exceeds ${GIT_BUDGET_MB}MB"
    local tmpdir
    tmpdir=$(mktemp -d "${TMPDIR:-/tmp}/reshallow-XXXXXX")
    # Clone fresh depth=1 into tmpdir
    git clone --depth 1 "$REPO_URL" "$tmpdir/fresh"
    # Swap the .git directories atomically
    rm -rf .git
    mv "$tmpdir/fresh/.git" .git
    # The fresh clone also has files — we already have them in place,
    # but pick up any that the old shallow clone was missing from HEAD.
    # Use git restore to bring tracked paths in sync with HEAD.
    git checkout -- . 2>/dev/null || true
    rm -rf "$tmpdir"
    echo "[checkout] re-shallow done; .git=$(du -sh .git | cut -f1)"
}

if [[ -d .git ]]; then
    git remote set-url origin "$REPO_URL"
    # Check .git size BEFORE fetching (fetch inflates pack)
    git_size_mb=$(du -sm .git 2>/dev/null | cut -f1)
    if (( git_size_mb > GIT_BUDGET_MB )); then
        reshallow
    else
        echo "[checkout] .git is ${git_size_mb}MB (budget ${GIT_BUDGET_MB}MB); fast-fetch"
        git fetch --depth=1 origin main
        git checkout --detach FETCH_HEAD
        # Preserve .venv and caches across clean
        git clean -fd -e .venv -e .cache -e __pycache__ -e .pytest_cache
        # Auto-gc prunes loose objects. Doesn't help pack bloat much
        # but keeps loose-object count sane.
        git gc --auto --quiet || true
    fi
else
    echo "[checkout] first clone (depth=1)"
    git clone --depth 1 "$REPO_URL" .
fi

# Final size report — diagnoses disk-budget incidents
echo "[checkout] final .git=$(du -sh .git | cut -f1) working-tree=$(du -sh --exclude=.git . | cut -f1)"
