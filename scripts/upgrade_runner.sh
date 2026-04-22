#!/usr/bin/env bash
# Upgrade the self-hosted runner to a specific version.
set -euo pipefail

VERSION="${1:-}"
if [ -z "$VERSION" ]; then
    echo "usage: $0 <runner-version, e.g. 2.333.1>" >&2
    exit 1
fi

RUNNER_DIR="/home/runner/actions-runner"
RUNNER_USER="runner"
RUNNER_SVC="actions.runner.azzon-slate-data-mirror.aliyun-cn-runner.service"
TARBALL="actions-runner-linux-x64-${VERSION}.tar.gz"

echo "==> stopping runner service"
systemctl stop "$RUNNER_SVC" 2>/dev/null || true

echo "==> downloading v${VERSION} via CN proxy"
cd "$RUNNER_DIR"
rm -f "$TARBALL"
for base in \
    "https://ghfast.top/https://github.com" \
    "https://gh-proxy.com/https://github.com" \
    "https://mirror.ghproxy.com/https://github.com" \
    "https://github.com"; do
    url="${base}/actions/runner/releases/download/v${VERSION}/${TARBALL}"
    echo "   trying ${base}"
    if sudo -u "$RUNNER_USER" curl -fL --connect-timeout 15 --max-time 600 \
            --retry 3 --progress-bar -o "$TARBALL" "$url"; then
        if sudo -u "$RUNNER_USER" tar -tzf "$TARBALL" >/dev/null 2>&1; then
            echo "   ok via $base"
            break
        fi
    fi
    rm -f "$TARBALL"
done
[ -f "$TARBALL" ] || { echo "all mirrors failed" >&2; exit 1; }

echo "==> extracting over existing install"
sudo -u "$RUNNER_USER" tar xzf "$TARBALL"
rm -f "$TARBALL"

echo "==> starting service"
systemctl start "$RUNNER_SVC"
sleep 5
journalctl -u "$RUNNER_SVC" -n 10 --no-pager | tail -6
