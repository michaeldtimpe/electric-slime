#!/usr/bin/env bash
# Native electric-slime runner on a Linux workstation, as the logged-in user: no root, no
# packages. The runner and llama.cpp run as plain processes under a systemd --user service.
# (The USB nodes run the system unit from the stick image instead.)
#   ./slime-node.sh install      # fetch+verify llama.cpp, install runner, start it
#   ./slime-node.sh uninstall    # stop it and remove the unit (keeps the model cache)
#   ./slime-node.sh status | logs
# Settings (env, at install): SLOTS (2), SLIME_CAPACITY_MB (80% of RAM), SLIME_THREADS
# (half the threads), SLIME_MAX_TIER (no limit), COORD (from site.env),
# NODE (short hostname). Needs libgomp1 (present on desktop Ubuntu).
# A --user service runs while the user has a session; `loginctl enable-linger` keeps it
# running without one (a choice for the machine's owner).
set -euo pipefail
LLAMA_TAG=b11399
LLAMA_SHA=da013120cdb86a7f6390ef3b61da04800f4bdf692bcd7cdbd0680ad63dcf8b08
HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$HOME/.local/share/electric-slime
UNIT=electric-slime-runner.service
UNIT_PATH=$HOME/.config/systemd/user/$UNIT
[ -f "$(dirname "$0")/../../site.env" ] && . "$(dirname "$0")/../../site.env"
COORD=${COORD:?set COORD or fill in site.env (see site.env.example)}
NODE=${NODE:-$(hostname -s | tr '[:upper:]' '[:lower:]')}
SLOTS=${SLOTS:-2}

install_llama() {
    local dir=$ROOT/llama-$LLAMA_TAG tgz=$ROOT/llama-$LLAMA_TAG.tar.gz
    [ -x "$dir/llama-server" ] && return
    curl -sSfL -o "$tgz" \
        "https://github.com/ggml-org/llama.cpp/releases/download/$LLAMA_TAG/llama-$LLAMA_TAG-bin-ubuntu-x64.tar.gz"
    echo "$LLAMA_SHA  $tgz" | sha256sum -c --quiet
    tar -xzf "$tgz" -C "$ROOT" && rm -f "$tgz"
    if ldd "$dir/llama-server" | grep 'not found'; then
        echo "llama-server is missing libraries (libgomp1?)"; exit 1
    fi
    "$dir/llama-server" --version 2>&1 | grep -m1 version
}

case "${1:-}" in
install)
    mkdir -p "$ROOT/cache" "$(dirname "$UNIT_PATH")"
    install_llama
    install -m 644 "$HERE/../../slime.py" "$ROOT/slime.py"
    cap_mb=${SLIME_CAPACITY_MB:-}
    cat > "$UNIT_PATH" <<EOF
[Unit]
Description=electric-slime runner (native, user service)
After=network-online.target

[Service]
Environment=SLIME_CACHE=$ROOT/cache LLAMA_SERVER=$ROOT/llama-$LLAMA_TAG/llama-server
Environment=SLIME_GPU_LAYERS=${SLIME_GPU_LAYERS:-0}${cap_mb:+ SLIME_CAPACITY_MB=$cap_mb}${SLIME_THREADS:+ SLIME_THREADS=$SLIME_THREADS}${SLIME_MAX_TIER:+ SLIME_MAX_TIER=$SLIME_MAX_TIER}
ExecStart=/usr/bin/python3 -u $ROOT/slime.py pool --coord $COORD --slots $SLOTS --node $NODE
Restart=always
RestartSec=5
Nice=10
${cap_mb:+MemoryHigh=$((cap_mb + 1024))M
MemoryMax=$((cap_mb + 2048))M}

[Install]
WantedBy=default.target
EOF
    systemctl --user daemon-reload
    systemctl --user enable --now "$UNIT" >/dev/null 2>&1 || systemctl --user enable --now "$UNIT"
    systemctl --user restart "$UNIT"
    echo "runner installed: node $NODE, $SLOTS slots -> $COORD (journalctl --user -u $UNIT)"
    ;;
uninstall)
    systemctl --user disable --now "$UNIT" 2>/dev/null || true
    rm -f "$UNIT_PATH"
    systemctl --user daemon-reload
    echo "runner removed; model cache kept in $ROOT/cache"
    ;;
status) systemctl --user --no-pager status "$UNIT" | head -5 ;;
logs)   journalctl --user -u "$UNIT" -n "${TAIL:-40}" -o cat --no-pager ;;
*)      sed -n '2,13p' "$0"; exit 2 ;;
esac
