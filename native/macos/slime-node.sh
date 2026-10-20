#!/usr/bin/env bash
# Native electric-slime runner on macOS: the runner and llama.cpp run as plain processes under
# a user LaunchAgent. No VM in between: macOS containers always run inside a Linux VM,
# and that costs memory and I/O for binaries that run natively anyway.
#   ./slime-node.sh install      # fetch+verify llama.cpp, install runner, start it at login
#   ./slime-node.sh uninstall    # stop it and remove the LaunchAgent (keeps the model cache)
#   ./slime-node.sh status | logs
# Settings (env, at install): SLOTS (2), SLIME_CAPACITY_MB (80% of RAM), SLIME_THREADS
# (half the cores), SLIME_GPU_LAYERS (0 = CPU only; 99 puts the model on Metal),
# COORD (from site.env), NODE (short hostname).
set -euo pipefail
LLAMA_TAG=b11399
LLAMA_SHA=bbd48f4f7fc20c942635f0d8d9c69b9e605a34d6100add6ed04aa68a3df3f254
HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$HOME/.local/share/electric-slime
LABEL=com.electric-slime.runner
PLIST=$HOME/Library/LaunchAgents/$LABEL.plist
[ -f "$(dirname "$0")/../../site.env" ] && . "$(dirname "$0")/../../site.env"
COORD=${COORD:?set COORD or fill in site.env (see site.env.example)}
NODE=${NODE:-$(hostname -s | tr '[:upper:]' '[:lower:]')}
SLOTS=${SLOTS:-2}

install_llama() {
    local dir=$ROOT/llama-$LLAMA_TAG tgz=$ROOT/llama-$LLAMA_TAG.tar.gz
    [ -x "$dir/llama-server" ] && return
    curl -sSfL -o "$tgz" \
        "https://github.com/ggml-org/llama.cpp/releases/download/$LLAMA_TAG/llama-$LLAMA_TAG-bin-macos-arm64.tar.gz"
    echo "$LLAMA_SHA  $tgz" | shasum -a 256 -c - >/dev/null
    tar -xzf "$tgz" -C "$ROOT" && rm -f "$tgz"
    "$dir/llama-server" --version 2>&1 | grep -m1 version
}

env_entry() { printf '    <key>%s</key><string>%s</string>\n' "$1" "$2"; }

case "${1:-}" in
install)
    mkdir -p "$ROOT/cache" "$HOME/Library/LaunchAgents"
    install_llama
    install -m 644 "$HERE/../../slime.py" "$ROOT/slime.py"
    launchctl bootout "gui/$(id -u)/$LABEL" 2>/dev/null || true
    {
        cat <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0"><dict>
  <key>Label</key><string>$LABEL</string>
  <key>ProgramArguments</key><array>
    <string>/usr/bin/python3</string><string>-u</string><string>$ROOT/slime.py</string>
    <string>pool</string><string>--coord</string><string>$COORD</string>
    <string>--slots</string><string>$SLOTS</string><string>--node</string><string>$NODE</string>
  </array>
  <key>EnvironmentVariables</key><dict>
EOF
        env_entry SLIME_CACHE "$ROOT/cache"
        env_entry LLAMA_SERVER "$ROOT/llama-$LLAMA_TAG/llama-server"
        env_entry SLIME_GPU_LAYERS "${SLIME_GPU_LAYERS:-0}"
        [ -n "${SLIME_CAPACITY_MB:-}" ] && env_entry SLIME_CAPACITY_MB "$SLIME_CAPACITY_MB"
        [ -n "${SLIME_THREADS:-}" ] && env_entry SLIME_THREADS "$SLIME_THREADS"
        cat <<EOF
  </dict>
  <key>WorkingDirectory</key><string>$ROOT</string>
  <key>RunAtLoad</key><true/>
  <key>KeepAlive</key><true/>
  <key>ThrottleInterval</key><integer>10</integer>
  <key>Nice</key><integer>10</integer>
  <key>StandardOutPath</key><string>$ROOT/runner.log</string>
  <key>StandardErrorPath</key><string>$ROOT/runner.log</string>
</dict></plist>
EOF
    } > "$PLIST"
    plutil -lint "$PLIST" >/dev/null
    launchctl bootstrap "gui/$(id -u)" "$PLIST"
    echo "runner installed: node $NODE, $SLOTS slots -> $COORD (log $ROOT/runner.log)"
    ;;
uninstall)
    launchctl bootout "gui/$(id -u)/$LABEL" 2>/dev/null || true
    rm -f "$PLIST"
    echo "runner removed; model cache kept in $ROOT/cache"
    ;;
status) launchctl print "gui/$(id -u)/$LABEL" 2>/dev/null | grep -E '^\s+(state|pid|last exit code) =' || echo "not loaded" ;;
logs)   tail -n "${TAIL:-40}" "$ROOT/runner.log" ;;
*)      sed -n '2,10p' "$0"; exit 2 ;;
esac
