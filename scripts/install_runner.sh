#!/usr/bin/env bash
# One-shot installer for the self-hosted GitHub Actions runner.
# Run as root on the China-side server:
#
#   curl -fsSL https://raw.githubusercontent.com/azzon/slate-data-mirror/main/scripts/install_runner.sh \
#     | REG_TOKEN=<token> bash
#
# REG_TOKEN comes from `gh api -X POST /repos/azzon/slate-data-mirror/actions/runners/registration-token`
# (expires 1h). Re-run is safe — removes any previous registration first.
set -euo pipefail

REPO_OWNER="${REPO_OWNER:-azzon}"
REPO_NAME="${REPO_NAME:-slate-data-mirror}"
RUNNER_VERSION="${RUNNER_VERSION:-2.321.0}"
RUNNER_USER="${RUNNER_USER:-runner}"
RUNNER_HOME="/home/${RUNNER_USER}"
RUNNER_DIR="${RUNNER_HOME}/actions-runner"

if [ -z "${REG_TOKEN:-}" ]; then
    echo "!! REG_TOKEN not set. Get one with:"
    echo "   gh api -X POST /repos/${REPO_OWNER}/${REPO_NAME}/actions/runners/registration-token"
    exit 1
fi

echo "==> apt deps"
export DEBIAN_FRONTEND=noninteractive
apt-get update -q >/dev/null
apt-get install -y -q python3-venv python3-pip python3-dev libssl-dev libffi-dev \
    ca-certificates curl jq tar >/dev/null

echo "==> runner user"
id "$RUNNER_USER" >/dev/null 2>&1 || useradd -m -s /bin/bash "$RUNNER_USER"

echo "==> ssh-to-github-over-443 config"
install -d -o "$RUNNER_USER" -g "$RUNNER_USER" -m 700 "$RUNNER_HOME/.ssh"
cat > "$RUNNER_HOME/.ssh/config" <<'EOF'
Host github.com
    HostName ssh.github.com
    Port 443
    User git
    IdentityFile ~/.ssh/id_ed25519_github
    StrictHostKeyChecking accept-new
EOF
chown "$RUNNER_USER:$RUNNER_USER" "$RUNNER_HOME/.ssh/config"
chmod 600 "$RUNNER_HOME/.ssh/config"

if [ ! -f "$RUNNER_HOME/.ssh/id_ed25519_github" ]; then
    sudo -u "$RUNNER_USER" ssh-keygen -t ed25519 -N "" \
        -f "$RUNNER_HOME/.ssh/id_ed25519_github" \
        -C "${REPO_NAME} runner" -q
    echo
    echo "!! NEW DEPLOY KEY GENERATED — register this pubkey on the repo"
    echo "   (Settings -> Deploy keys -> Add; allow write access):"
    echo
    cat "$RUNNER_HOME/.ssh/id_ed25519_github.pub"
    echo
fi

# Warm the SSH known_hosts so the runner's first git fetch doesn't hang on prompt
sudo -u "$RUNNER_USER" ssh-keyscan -p 443 ssh.github.com 2>/dev/null \
    | sed 's/\[ssh.github.com\]:443/github.com/' \
    >> "$RUNNER_HOME/.ssh/known_hosts" 2>/dev/null || true
sort -u "$RUNNER_HOME/.ssh/known_hosts" -o "$RUNNER_HOME/.ssh/known_hosts" 2>/dev/null || true
chown "$RUNNER_USER:$RUNNER_USER" "$RUNNER_HOME/.ssh/known_hosts"

echo "==> download actions-runner v${RUNNER_VERSION}"
mkdir -p "$RUNNER_DIR"
chown "$RUNNER_USER:$RUNNER_USER" "$RUNNER_DIR"
if [ ! -f "$RUNNER_DIR/run.sh" ]; then
    TARBALL="actions-runner-linux-x64-${RUNNER_VERSION}.tar.gz"
    sudo -u "$RUNNER_USER" bash -c "cd '$RUNNER_DIR' && \
        curl -fsSL -o '$TARBALL' 'https://github.com/actions/runner/releases/download/v${RUNNER_VERSION}/${TARBALL}' && \
        tar xzf '$TARBALL' && rm -f '$TARBALL'"
fi

echo "==> (re)register runner"
cd "$RUNNER_DIR"
# Unregister any stale registration (ignores errors on first install).
sudo -u "$RUNNER_USER" ./config.sh remove --token "$REG_TOKEN" >/dev/null 2>&1 || true
sudo -u "$RUNNER_USER" ./config.sh \
    --unattended \
    --url "https://github.com/${REPO_OWNER}/${REPO_NAME}" \
    --token "$REG_TOKEN" \
    --name "aliyun-cn-runner" \
    --labels "self-hosted,linux,x64,cn,aliyun" \
    --work "_work" \
    --replace

echo "==> install systemd service"
./svc.sh install "$RUNNER_USER"
./svc.sh start

sleep 3
systemctl status "actions.runner.${REPO_OWNER}-${REPO_NAME}.aliyun-cn-runner.service" --no-pager | head -15

echo
echo "==> also enable SSH on port 443 (so remote admin works through corporate proxies)"
if ! grep -qE '^Port 443' /etc/ssh/sshd_config; then
    echo 'Port 22' >> /etc/ssh/sshd_config
    echo 'Port 443' >> /etc/ssh/sshd_config
    systemctl restart ssh
    echo "   sshd now listens on 22 + 443"
else
    echo "   already configured"
fi

echo
echo "==> done. Check runner status at:"
echo "   https://github.com/${REPO_OWNER}/${REPO_NAME}/settings/actions/runners"
