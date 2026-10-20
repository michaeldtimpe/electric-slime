#!/usr/bin/env bash
# Give a freshly flashed stick its identity and secrets (macOS, run WITHOUT sudo: the
# secrets come from your login Keychain, service `electric-slime`, which root can't read).
#   ./personalize.sh /dev/diskN slime-t450-a [SLOTS]
# Writes /slime/{node.env,wifi.json,ts.key,host.key} on the stick's EFI partition; the
# node's first boot moves them into root-only places and deletes them from the stick.
# host.key is minted here and enrolled with both coordinators (../enroll-host.sh, over ssh)
# so the node's runner calls are signed and accepted from its first boot.
set -euo pipefail
DISK=${1:?usage: personalize.sh /dev/diskN NODE_NAME [SLOTS]}
NAME=${2:?usage: personalize.sh /dev/diskN NODE_NAME [SLOTS]}
SLOTS=${3:-3}
[ -f "$(dirname "$0")/../site.env" ] && . "$(dirname "$0")/../site.env"
COORD=${COORDS:?fill in site.env (see site.env.example)}   # primary, standby
[[ "$NAME" =~ ^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$ ]] || { echo "bad node name: $NAME"; exit 1; }
[[ "$SLOTS" =~ ^[1-9][0-9]?$ ]] || { echo "bad slot count: $SLOTS"; exit 1; }

info=$(diskutil info "$DISK")
grep -qE 'Device Location: +External' <<<"$info" && grep -qE 'Protocol: +USB' <<<"$info" \
    || { echo "REFUSED: $DISK is not an external USB disk"; exit 1; }

ESP=$(diskutil list "$DISK" | awk '/ UEFI /{print $NF; exit}')
[ -n "$ESP" ] || { echo "no UEFI partition on $DISK — flash it first"; exit 1; }
diskutil mount "$ESP" >/dev/null
MP=$(diskutil info "$ESP" | awk -F': +' '/Mount Point/{print $2; exit}')
[ -f "$MP/EFI/BOOT/BOOTX64.EFI" ] || { echo "REFUSED: $ESP doesn't look like a node image"; exit 1; }

mkdir -p "$MP/slime"
printf 'NODE_NAME=%s\nCOORD=%s\nSLOTS=%s\nWIFI_COUNTRY=US\n' "$NAME" "$COORD" "$SLOTS" > "$MP/slime/node.env"
# only the networks a node can use (use_for_nodes); secrets go file-to-file, never echoed
security find-generic-password -s electric-slime -a wifi -w | python3 -c '
import json, sys
nets = [{"ssid": n["ssid"], "psk": n["psk"]} for n in json.load(sys.stdin) if n.get("use_for_nodes")]
assert nets, "no node networks in keychain"
json.dump(nets, sys.stdout)' > "$MP/slime/wifi.json"
security find-generic-password -s electric-slime -a ts-authkey -w | tr -d '\n' > "$MP/slime/ts.key"
[ -s "$MP/slime/wifi.json" ] && [ -s "$MP/slime/ts.key" ] || { echo "secret write failed"; exit 1; }
(umask 077; python3 -c 'import secrets; print(secrets.token_hex(32))' > "$MP/slime/host.key")
"$(dirname "$0")/../enroll-host.sh" "$NAME" < "$MP/slime/host.key" \
    || { echo "enrolling $NAME failed: the node would be refused"; exit 1; }

sync
echo "$NAME: $(python3 -c 'import json,sys;print(len(json.load(open(sys.argv[1]))))' "$MP/slime/wifi.json") wifi networks, tailscale key, enrolled host key, coord $COORD, $SLOTS slots"
diskutil eject "$DISK" >/dev/null && echo "ejected $DISK — boot a T450 from it (F12)"
