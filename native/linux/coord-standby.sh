#!/usr/bin/env bash
# A standby coordinator on a Linux workstation (systemd --user, no root). It copies the
# primary's state every few seconds and refuses work until promoted:
#   slime.py promote --coord http://<this host>:8700 --admin-token-file <its admin.token>
# Its depot is this host's runner cache (it holds every shard once prefetch has run).
#   ./coord-standby.sh install | uninstall | status | logs
# Settings (env): PRIMARY (COORD from site.env), PORT (8700).
# Needs coord.py and hosts.keys in ~/.local/share/electric-slime (enroll-host.sh keeps
# the keys in step with the primary's).
set -euo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$HOME/.local/share/electric-slime
UNIT=electric-slime-coord.service
UNIT_PATH=$HOME/.config/systemd/user/$UNIT
[ -f "$HERE/../../site.env" ] && . "$HERE/../../site.env"
PRIMARY=${PRIMARY:-${COORD:?set PRIMARY or fill in site.env (see site.env.example)}}
PORT=${PORT:-8700}

case "${1:-}" in
install)
    install -m 644 "$HERE/../../coord.py" "$ROOT/coord.py"
    [ -s "$ROOT/hosts.keys" ] || { echo "no $ROOT/hosts.keys: enroll hosts first"; exit 1; }
    [ -s "$ROOT/admin.token" ] ||
        (umask 077; python3 -c 'import secrets; print(secrets.token_hex(24))' > "$ROOT/admin.token")
    mkdir -p "$(dirname "$UNIT_PATH")"
    cat > "$UNIT_PATH" <<EOF
[Unit]
Description=electric-slime standby coordinator (replicates $PRIMARY)
After=network-online.target

[Service]
ExecStart=/usr/bin/python3 -u $ROOT/coord.py serve --port $PORT --budget 16000 --role standby --peer $PRIMARY --state $ROOT/coord-standby.json --depot $ROOT/cache/blobs --keys $ROOT/hosts.keys --auth required --admin-token-file $ROOT/admin.token
Restart=always
RestartSec=5
Nice=10
MemoryMax=512M

[Install]
WantedBy=default.target
EOF
    systemctl --user daemon-reload
    systemctl --user enable --now "$UNIT" >/dev/null 2>&1 || systemctl --user enable --now "$UNIT"
    systemctl --user restart "$UNIT"
    echo "standby coordinator on :$PORT replicating $PRIMARY"
    ;;
uninstall)
    systemctl --user disable --now "$UNIT" 2>/dev/null || true
    rm -f "$UNIT_PATH"
    systemctl --user daemon-reload
    echo "standby removed (state and keys kept in $ROOT)"
    ;;
status) systemctl --user --no-pager status "$UNIT" | head -5 ;;
logs)   journalctl --user -u "$UNIT" -n "${TAIL:-40}" -o cat --no-pager ;;
*)      sed -n '2,10p' "$0"; exit 2 ;;
esac
