#!/usr/bin/env bash
# Make sshd actually listen on 443 on Ubuntu 22.04+ / 24.04.
#
# Naively appending `Port 443` to /etc/ssh/sshd_config doesn't work on
# these releases because:
#   1. sshd_config has `Include /etc/ssh/sshd_config.d/*.conf` near the
#      top, so any `Port` directive after that include is overridden.
#   2. More importantly, Ubuntu 22.04+ uses ssh.socket (socket-activated),
#      so the Port directive in sshd_config is ignored entirely — the
#      listen ports are defined by the socket unit.
#
# Fix: switch to ssh.service and pin Port lines in a drop-in.
set -euo pipefail

echo "==> disable ssh.socket (if present) and enable ssh.service"
if systemctl list-unit-files ssh.socket >/dev/null 2>&1; then
    systemctl stop ssh.socket 2>/dev/null || true
    systemctl disable ssh.socket 2>/dev/null || true
fi
systemctl enable ssh.service >/dev/null 2>&1

echo "==> ensure PrivSep dir (ssh.socket used to auto-create this)"
mkdir -p /run/sshd
chmod 755 /run/sshd
# Make it survive reboots without ssh.socket
cat > /etc/tmpfiles.d/sshd-privsep.conf <<'EOF'
d /run/sshd 0755 root root -
EOF

echo "==> write drop-in pinning Port 22 + Port 443"
mkdir -p /etc/ssh/sshd_config.d
cat > /etc/ssh/sshd_config.d/99-dualport.conf <<'EOF'
Port 22
Port 443
EOF

echo "==> validate config"
if ! sshd -t; then
    echo "!! sshd config invalid — leaving running sshd untouched" >&2
    exit 1
fi

echo "==> restart ssh.service"
systemctl restart ssh.service
sleep 1

echo "==> current listeners:"
ss -ltnp 2>/dev/null | awk '/:22 |:443 / {print}' || netstat -ltnp 2>/dev/null | awk '/:22|:443/ {print}'

echo
echo "done. Both ports should now answer SSH banner."
