#!/usr/bin/env bash
# One-click installer for the AKShare -> GitHub mirror.
#
# Idempotent: safe to re-run after editing .env or pulling the repo.
# Runs as the invoking user (no sudo) — installs systemd *user* units,
# so the machine needs `loginctl enable-linger $USER` if unattended
# reboots are required (install.sh runs this for you).
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

echo "==> repo at $REPO_ROOT"

if [[ ! -f .env ]]; then
    echo "!! .env not found. Copy .env.example to .env and fill GITHUB_TOKEN first." >&2
    exit 1
fi
chmod 600 .env

# shellcheck disable=SC1091
source .env
if [[ -z "${GITHUB_TOKEN:-}" ]]; then
    echo "!! GITHUB_TOKEN empty in .env" >&2
    exit 1
fi

# --- venv -------------------------------------------------------------
if [[ ! -x .venv/bin/python ]]; then
    echo "==> creating .venv"
    python3 -m venv .venv
fi
echo "==> installing requirements"
.venv/bin/pip install --quiet --upgrade pip
.venv/bin/pip install --quiet -r requirements.txt

# --- systemd user units ----------------------------------------------
UNIT_DIR="$HOME/.config/systemd/user"
mkdir -p "$UNIT_DIR"

for unit in akshare-mirror.service akshare-mirror.timer; do
    sed "s|__REPO_ROOT__|$REPO_ROOT|g" "systemd/$unit" > "$UNIT_DIR/$unit"
done
echo "==> installed units into $UNIT_DIR"

# Enable lingering so timers fire without an active login session.
if command -v loginctl >/dev/null 2>&1; then
    if ! loginctl show-user "$USER" 2>/dev/null | grep -q "Linger=yes"; then
        echo "==> enabling lingering for $USER (may prompt for sudo)"
        sudo loginctl enable-linger "$USER" || echo "   (non-fatal: enable-linger failed — timer needs active login)"
    fi
fi

systemctl --user daemon-reload
systemctl --user enable --now akshare-mirror.timer

echo
echo "==> timer status:"
systemctl --user list-timers akshare-mirror.timer --no-pager || true
echo
echo "Done. Tail logs with:"
echo "  journalctl --user -u akshare-mirror.service -f"
echo "Run immediately with:"
echo "  systemctl --user start akshare-mirror.service"
