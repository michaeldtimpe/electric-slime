#!/usr/bin/env bash
# Build the electric-slime USB node image from Ubuntu 24.04's official cloud image.
# Run as root on an amd64 Ubuntu host. Contains NO secrets: wifi and the Tailscale key
# go onto each stick's EFI partition at flash time (personalize.sh, on the Mac).
#
#   sudo PUBKEY="ssh-ed25519 AAAA… user@host" ./build-image.sh
set -euo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
OUT=${OUT:-$HERE/build}
SIZE=${SIZE:-6G}
PUBKEY=${PUBKEY:?set PUBKEY to the authorized_keys line for the slime user}
BASE=https://cloud-images.ubuntu.com/releases/noble/release
IMG=ubuntu-24.04-server-cloudimg-amd64.img
NAME=slime-node-24.04-$(date +%Y%m%d)

LOOP= MNT=
cleanup() {
    [ -n "$MNT" ] && { umount -R "$MNT" 2>/dev/null || true; rmdir "$MNT" 2>/dev/null || true; }
    [ -n "$LOOP" ] && losetup -d "$LOOP" 2>/dev/null || true
}
trap cleanup EXIT

mkdir -p "$OUT" && cd "$OUT"

echo "== 1. fetch and verify the cloud image (signed SHA256SUMS)"
curl -fsSLO "$BASE/SHA256SUMS"
curl -fsSLO "$BASE/SHA256SUMS.gpg"
gpgv --keyring /usr/share/keyrings/ubuntu-cloudimage-keyring.gpg SHA256SUMS.gpg SHA256SUMS
if ! grep " \*\?$IMG\$" SHA256SUMS | sha256sum -c --status 2>/dev/null; then
    curl -fL --progress-bar -o "$IMG" "$BASE/$IMG"
    grep " \*\?$IMG\$" SHA256SUMS | sha256sum -c
fi

echo "== 2. raw working copy grown to $SIZE"
qemu-img convert -O raw "$IMG" "$NAME.img"
qemu-img resize -q -f raw "$NAME.img" "$SIZE"
sgdisk -e "$NAME.img" >/dev/null            # move the backup GPT to the new end
LOOP=$(losetup -fP --show "$NAME.img")
udevadm settle
growpart "$LOOP" 1
e2fsck -pf "${LOOP}p1" || [ $? -le 1 ]      # 1 = "errors corrected", not a failure
resize2fs "${LOOP}p1"

# if, not `[ ] && echo`: a non-matching last partition must not return 1 under set -e
part_by_label() {
    for p in "$LOOP"p*; do
        if [ "$(blkid -o value -s LABEL "$p" || true)" = "$1" ]; then echo "$p"; fi
    done
}
ROOT=$(part_by_label cloudimg-rootfs); BOOT=$(part_by_label BOOT); ESP=$(part_by_label UEFI)
echo "   root=$ROOT boot=${BOOT:-none} esp=$ESP"

echo "== 3. mount and chroot"
MNT=$(mktemp -d)
mount "$ROOT" "$MNT"
[ -n "$BOOT" ] && mount "$BOOT" "$MNT/boot"
mount "$ESP" "$MNT/boot/efi"
mount -t proc proc "$MNT/proc"
mount -t sysfs sys "$MNT/sys"
mount --bind /dev "$MNT/dev"
mount --bind /dev/pts "$MNT/dev/pts"
mv "$MNT/etc/resolv.conf" "$MNT/etc/resolv.conf.slime"
cp -L /etc/resolv.conf "$MNT/etc/resolv.conf"        # chroot shares the host's netns
printf '#!/bin/sh\nexit 101\n' > "$MNT/usr/sbin/policy-rc.d"   # no daemons start in the chroot
chmod 755 "$MNT/usr/sbin/policy-rc.d"

echo "== 4. overlay + scaffold"
cp -r --no-preserve=ownership "$HERE/rootfs/." "$MNT/"
install -d -m 755 "$MNT/opt/electric-slime"
install -m 644 "$HERE/../slime.py" "$HERE/../manifest.json" "$HERE/../genome.json" "$MNT/opt/electric-slime/"
# llama.cpp's own CPU release (one .so per x86 generation, picked at load), pinned + verified
LLAMA_TAG=b11399
LLAMA_SHA=da013120cdb86a7f6390ef3b61da04800f4bdf692bcd7cdbd0680ad63dcf8b08
LLAMA_TGZ="$OUT/llama-$LLAMA_TAG-bin-ubuntu-x64.tar.gz"
[ -f "$LLAMA_TGZ" ] || curl -sSfL -o "$LLAMA_TGZ" \
    "https://github.com/ggml-org/llama.cpp/releases/download/$LLAMA_TAG/llama-$LLAMA_TAG-bin-ubuntu-x64.tar.gz"
echo "$LLAMA_SHA  $LLAMA_TGZ" | sha256sum -c --quiet
tar -xzf "$LLAMA_TGZ" -C "$MNT/opt" --no-same-owner
ln -sfn "llama-$LLAMA_TAG" "$MNT/opt/llama"

echo "== 5. customise inside the chroot"
install -m 755 "$HERE/customise.sh" "$MNT/tmp/customise.sh"
chroot "$MNT" env PUBKEY="$PUBKEY" /tmp/customise.sh </dev/null
rm -f "$MNT/tmp/customise.sh"

echo "== 6. inventory (informational: never fails the build)"
{ ls "$MNT/boot/efi/EFI/BOOT/" | sed 's/^/   EFI\/BOOT: /'; } || true
{ ls "$MNT/boot" | grep -E '^vmlinuz-' | sed 's/^/   kernel: /'; } || true
{ chroot "$MNT" dpkg -l 'linux-modules-extra-*' tailscale wpasupplicant 2>/dev/null | awk '/^ii/{print "   pkg: "$2" "$3}'; } || true
{ df -h --output=used,avail "$MNT" | sed 's/^/   /'; } || true

echo "== 7. unmount, trim, compress"
rm -f "$MNT/usr/sbin/policy-rc.d"
mv -f "$MNT/etc/resolv.conf.slime" "$MNT/etc/resolv.conf"
fstrim "$MNT" || true
[ -n "$BOOT" ] && { fstrim "$MNT/boot" || true; }
umount -R "$MNT"; rmdir "$MNT"; MNT=
losetup -d "$LOOP"; LOOP=
zstd -q -T0 -10 --rm "$NAME.img" -o "$NAME.img.zst"
sha256sum "$NAME.img.zst" > "$NAME.img.zst.sha256"
ls -lh "$NAME.img.zst"
echo "built $OUT/$NAME.img.zst"
