#!/usr/bin/env bash
# Boot-test a built node image in a UEFI VM that mimics a T450: OVMF with Secure Boot on
# and Microsoft's keys enrolled, empty NVRAM (so the removable-media path is what boots),
# the image attached as a USB stick on xHCI, an Intel e1000e NIC, and the disk padded to
# 16 GB so first boot has to grow the root partition. Personalised with a test name and
# NO secrets. The node's runners dial the coordinator at the host (10.0.2.2:8700).
# FIRMWARE=bios boots it with SeaBIOS instead, the way the T450s actually boot it.
#   sudo ./vm-test.sh build/slime-node-24.04-YYYYMMDD.img.zst     # start
#   sudo FIRMWARE=bios ./vm-test.sh build/…img.zst                 # start, legacy BIOS
#   sudo ./vm-test.sh stop                                         # stop + clean up
set -euo pipefail
W=/var/tmp/slime-vmtest

if [ "${1:-}" = stop ]; then
    [ -f "$W/qemu.pid" ] && kill "$(cat "$W/qemu.pid")" 2>/dev/null || true
    sleep 2; rm -rf "$W"; echo "VM stopped, $W removed"; exit 0
fi

IMG=${1:?usage: vm-test.sh IMAGE.img.zst | stop}
mkdir -p "$W"
zstd -q -dc "$IMG" > "$W/disk.img"
truncate -s 16G "$W/disk.img"

LOOP=$(losetup -fP --show "$W/disk.img")
udevadm settle
ESP=
for p in "$LOOP"p*; do
    if [ "$(blkid -o value -s LABEL "$p" || true)" = UEFI ]; then ESP=$p; fi
done
M=$(mktemp -d)
mount "$ESP" "$M"
echo "   ESP EFI/BOOT: $(ls "$M/EFI/BOOT" | tr '\n' ' ')"
mkdir -p "$M/slime"
printf 'NODE_NAME=slime-vmtest\nCOORD=http://10.0.2.2:8700\nSLOTS=2\nWIFI_COUNTRY=US\n' > "$M/slime/node.env"
# a signing key, kept root-only here; the swarm refuses the VM until it is enrolled:
#   ssh relay 'sudo cat /var/tmp/slime-vmtest/host.key' | ./enroll-host.sh slime-vmtest
(umask 077; python3 -c 'import secrets; print(secrets.token_hex(32))' > "$W/host.key")
cp "$W/host.key" "$M/slime/host.key"
umount "$M"; rmdir "$M"; losetup -d "$LOOP"

if [ "${FIRMWARE:-uefi}" = bios ]; then
    fw=(-machine q35)
else
    cp /usr/share/OVMF/OVMF_VARS_4M.ms.fd "$W/vars.fd"
    fw=(-machine q35,smm=on -global driver=cfi.pflash01,property=secure,value=on
        -drive if=pflash,format=raw,unit=0,readonly=on,file=/usr/share/OVMF/OVMF_CODE_4M.secboot.fd
        -drive if=pflash,format=raw,unit=1,file="$W/vars.fd")
fi
qemu-system-x86_64 -name slime-vmtest -enable-kvm -cpu host -smp 2 -m 2048 "${fw[@]}" \
    -device qemu-xhci -drive if=none,id=stick,format=raw,file="$W/disk.img" \
    -device usb-storage,drive=stick,removable=on \
    -netdev user,id=n0,hostfwd=tcp:127.0.0.1:2222-:22 -device e1000e,netdev=n0 \
    -display none -monitor unix:"$W/mon.sock",server,nowait \
    -daemonize -pidfile "$W/qemu.pid"
echo "VM up (pid $(cat "$W/qemu.pid")); ssh from m1: ssh -J relay -p 2222 slime@127.0.0.1"
