#!/usr/bin/env bash
# Write the node image to a USB stick (macOS). Needs root for the raw disk write:
#   sudo ./flash.sh build/slime-node-24.04-YYYYMMDD.img.zst /dev/diskN
# Refuses anything that is not an external, removable USB disk of 8-256 GB.
set -euo pipefail
IMG=${1:?usage: sudo flash.sh IMAGE.img.zst /dev/diskN}
DISK=${2:?usage: sudo flash.sh IMAGE.img.zst /dev/diskN}
ZSTD=$(command -v zstd || echo /opt/homebrew/bin/zstd)
[ "$(id -u)" -eq 0 ] || { echo "run with sudo (raw disk write)"; exit 1; }
[[ "$DISK" =~ ^/dev/disk[0-9]+$ ]] || { echo "give a whole disk like /dev/disk6"; exit 1; }

if [ -f "$IMG.sha256" ]; then
    echo "verifying image checksum…"
    (cd "$(dirname "$IMG")" && shasum -a 256 -c "$(basename "$IMG").sha256")
fi

info=$(diskutil info "$DISK")
field() { awk -F': +' -v k="$1" '$1 ~ "^ *"k"$" {print $2; exit}' <<<"$info"; }
bytes=$(field "Disk Size" | sed -E 's/.*\(([0-9]+) Bytes\).*/\1/')
boot=$(diskutil info / | awk -F': +' '/Part of Whole/{print $2; exit}')
[ "$(field 'Device Location')" = "External" ] || { echo "REFUSED: $DISK is not external"; exit 1; }
[ "$(field 'Protocol')" = "USB" ]             || { echo "REFUSED: $DISK is not USB"; exit 1; }
[ "$(field 'Removable Media')" = "Removable" ] || { echo "REFUSED: $DISK is not removable media"; exit 1; }
[ "$(basename "$DISK")" != "$boot" ]          || { echo "REFUSED: $DISK is the boot disk"; exit 1; }
(( bytes >= 8000000000 && bytes <= 256000000000 )) || { echo "REFUSED: size $bytes outside 8-256 GB"; exit 1; }

echo "About to ERASE $DISK: $(field 'Device / Media Name'), $(field 'Disk Size' | cut -d'(' -f1)"
echo "Ctrl-C within 10 s to abort."; sleep 10

diskutil unmountDisk "$DISK"
"$ZSTD" -dc "$IMG" | dd of="/dev/r$(basename "$DISK")" bs=4m status=progress
sync
echo "flashed $DISK — now run (without sudo): ./personalize.sh $DISK <node-name>"
