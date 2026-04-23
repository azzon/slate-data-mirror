#!/usr/bin/env bash
# Idempotent hardening for the self-hosted runner on a small aliyun box.
#
# Run as root once. Addresses three pain points from the bootstrap v2
# post-mortem:
#
# 1. **OOM killer eats sshd.** The runner's worker (python + httpx +
#    pandas holding per-ticker bars pre-flush) can spike past 1 GB on
#    the 2 GB box. The kernel picks sshd as a victim because it's
#    smaller and older. Cap the runner service at 1.2 GB via
#    MemoryMax so the OOM killer targets the runner first, not sshd.
#
# 2. **Runner doesn't auto-restart on crash.** `svc.sh install`'s
#    default unit uses Restart=always already but doesn't honour
#    StartLimitBurst, so a crash-loop after OOM can leave it offline.
#    Set a generous restart policy.
#
# 3. **sshd lost its :443 listener.** Re-assert the dual-port config
#    so an ECS reboot doesn't silently drop 443.
set -euo pipefail

RUNNER_SVC="actions.runner.azzon-slate-data-mirror.aliyun-cn-runner.service"

echo "==> runner systemd drop-in: memory cap + restart policy"
install -d -m 755 /etc/systemd/system/"$RUNNER_SVC".d
# Box has 1.6 GB RAM. Reserve ~100M for sshd + systemd + kernel. Runner
# peak (pip install pandas+pyarrow + bootstrap chunk with 200MB pandas
# frames) lands near 1.3G so give 1.5G hard cap + 1.3G soft throttle.
# Previous 1200M was too tight — pip install tripped it silently.
cat > /etc/systemd/system/"$RUNNER_SVC".d/10-harden.conf <<'EOF'
[Service]
MemoryMax=1500M
MemoryHigh=1300M
Restart=always
RestartSec=10
StartLimitBurst=10
StartLimitIntervalSec=300
EOF

echo "==> sshd dual-port (reassert in case ECS reboot wiped it)"
mkdir -p /run/sshd /etc/tmpfiles.d /etc/ssh/sshd_config.d
chmod 755 /run/sshd
cat > /etc/tmpfiles.d/sshd-privsep.conf <<'EOF'
d /run/sshd 0755 root root -
EOF
cat > /etc/ssh/sshd_config.d/99-dualport.conf <<'EOF'
Port 22
Port 443
EOF
if systemctl list-unit-files ssh.socket >/dev/null 2>&1; then
    systemctl stop ssh.socket 2>/dev/null || true
    systemctl disable ssh.socket 2>/dev/null || true
fi
systemctl enable ssh.service >/dev/null 2>&1

sshd -t

echo "==> reload + restart"
systemctl daemon-reload
systemctl restart ssh.service
systemctl restart "$RUNNER_SVC"
sleep 3

echo "==> status"
ss -ltnp | grep -E ":22 |:443 " || true
systemctl is-active "$RUNNER_SVC"
systemctl show "$RUNNER_SVC" -p MemoryMax -p Restart -p RestartSec
echo
echo "done."
