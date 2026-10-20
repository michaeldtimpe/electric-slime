# node-image — USB boot sticks for electric-slime nodes

Turns any spare x86 laptop (built for the two T450s) into a swarm node: plug in the
stick, power on from AC, and it joins the wifi + tailnet and starts claiming tiers from
the coordinator. Nothing on the laptop's internal disk is touched.

## What's on the stick

Ubuntu 24.04 from the **official cloud image** (signature-verified at build), customised:

- **real-hardware kernel** (`linux-image-generic`: adds `iwlwifi` and `linux-firmware`),
  `wpasupplicant`, Tailscale (official repo), the scaffold in `/opt/electric-slime`
- **the v1 runtime**: llama.cpp **b11399** (official `ubuntu-x64` release, sha256-pinned in
  `build-image.sh`) at `/opt/llama`, plus `libgomp1`; the build fails if `llama-server` is
  missing any library. `slime-runner.service` caches models in `/var/cache/slime`, caps
  itself at `MemoryHigh=85%`/`MemoryMax=90%`, and serves its cache to peers on :8701.
  Defaults (`/etc/slime/node.env`, from `slime-firstboot`): 3 slots, `SLIME_MAX_TIER=1`.
- **legacy BIOS and UEFI**: the T450s boot it in BIOS mode; `firstboot` points
  `grub-pc/install_devices` at the stick's `/dev/disk/by-id/usb-…` so grub-pc upgrades rewrite
  the stick's own MBR
- **no cloud-init, no snapd** — `slime-firstboot` personalises the node instead, and snap
  seeding would cost minutes of first boot on a USB stick
- **never sleeps**: lid ignored, sleep targets masked; Wi-Fi power save off
- **user `slime`**: key-only SSH (your m1 fleet key), passwordless sudo, no password
- **no identity baked in**: `machine-id`, SSH host keys and Tailscale state are created on
  each stick's first boot, so cloned sticks never collide
- **no secrets in the image**: wifi, the Tailscale key and the node's **signing key** are
  written per stick by `personalize.sh` onto the EFI partition, then moved into place
  (`host.key` → `/etc/slime/host.key`, root:slime 640) and deleted on first boot.
  `personalize.sh` mints the signing key and enrolls it with both coordinators over ssh
  (`../enroll-host.sh`), so the node is accepted from its first boot; the coordinators
  refuse unsigned or unknown hosts
- **never writes the host laptop's firmware** (boot entries / BootOrder), now or on upgrades:
  - `EFI/BOOT` is shim → `grubx64.efi` directly, with **no `fbx64.efi`** (shim's fallback
    creates an NVRAM entry on every machine it boots)
  - `/usr/sbin/grub-install` is a `dpkg-divert` wrapper that always adds `--no-nvram`: cloud
    images upgrade grub via `grub-multi-install`'s bare `grub-install`, which ignores the
    `grub2/update_nvram` debconf setting (found in the VM test — an upgrade put "Ubuntu" first
    in BootOrder before this)
  - an apt post-invoke hook (`slime-sync-efi-boot`) keeps `EFI/BOOT` in step with upgraded
    shim/grub and deletes `fbx64.efi` if anything restores it

## Networking

- Ethernet (any `en*`, DHCP, metric 100) wins whenever a cable is in.
- Wifi: the home's 5 GHz and 2.4 GHz networks at **equal priority** — wpa_supplicant picks by
  estimated throughput, so 5 GHz near the router and 2.4 GHz on the far side of the house.
  Not the 6 GHz one (the T450's Intel 7265 has no 6 GHz) or the guest one (LAN-isolated).
- Tailscale with the **ephemeral, reusable** swarm key, kept root-only so a node re-joins after
  any reboot. `--accept-dns=false`. Key lasts ~90 days (minted 2026-10-19); after that,
  re-personalise or mint a new one — the LAN data plane keeps working regardless.
- Coordinators: `COORDS` from `site.env` (primary on the relay's DHCP-reserved LAN address, then the standby).

## Build (on the relay, as root)

    rsync -a node-image/ relay:electric-slime/node-image/ --exclude build
    ssh relay 'cd ~/electric-slime/node-image && \
        sudo PUBKEY="$(cat ~/.ssh/id_ed25519.pub)" nice -n 19 ionice -c3 ./build-image.sh'   # PUBKEY from m1

The relay is also the swarm's coordinator: build at idle priority (as above). A
full-priority build on 2026-10-20 stalled it past the heartbeat window and every host
lapsed at once (the immune system called it a `mass_lapse` and charged no one).

Output: `build/slime-node-24.04-YYYYMMDD.img.zst` (+ `.sha256`). Host needs `qemu-utils`,
`cloud-guest-utils`, `ubuntu-cloudimage-keyring`, `zstd`, `gdisk`.

## Boot-test in a VM (on the relay)

    sudo ./vm-test.sh build/slime-node-24.04-YYYYMMDD.img.zst   # needs qemu-system-x86, ovmf
    ssh -J relay -p 2222 slime@127.0.0.1               # from m1
    sudo ./vm-test.sh stop

OVMF with Secure Boot on and Microsoft keys enrolled, empty NVRAM, the image as a USB stick
on xHCI, an e1000e NIC, disk padded to 16 GB. Runners dial the coordinator at `10.0.2.2:8700`.
`FIRMWARE=bios` boots the same stick with SeaBIOS instead — the T450s boot it that way, so
run both before flashing a new build. The VM joins the **live** swarm (it's a real, if small,
host: 2 GB → 1.5 GB offered, max tier 1). Last results (2026-10-19): UEFI Secure Boot and
SeaBIOS both boot clean, firstboot and re-personalisation work, grub-pc reinstall + reboot
work under BIOS, NVRAM untouched under UEFI; the v1 image joined the live swarm in 19 s,
fetched the 0.6B from a peer and answered through its own llama-server in 1 s. Rebuilt
2026-10-20 02:28 (`slime-node-24.04-20261020`, runner with graceful SIGTERM leave, memory
and thermal pressure, lead-aware orchestration, pipelined reviews, sleep detection) at idle
priority while the swarm worked — no host lapsed: SeaBIOS boot, signed in and seen by the
swarm moments after enrollment, runner byte-identical to the repo's; key revoked after.

Already-booted sticks don't need reflashing for runner updates: `../deploy.sh runners`
pushes `slime.py` to every node.

## Flash + personalise (on the Mac)

    sudo ./flash.sh build/slime-node-24.04-YYYYMMDD.img.zst /dev/diskN   # root: raw write
    ./personalize.sh /dev/diskN slime-t450-a                              # NOT root: Keychain, ssh

`flash.sh` refuses anything that isn't an external, removable USB disk of 8–256 GB, or is
the boot disk, and gives 10 s to abort. `personalize.sh` reads Keychain service
`electric-slime` and ejects the stick when done.

## Boot a T450

`F12` at power-on → pick the USB stick. Secure Boot can stay on (shim + signed kernel).
First boot grows the root partition to fill the stick, creates the node's identity, joins
wifi and the tailnet, and starts `slime-runner`. With a monitor attached, the console login
banner shows the node name and its IPv4. Then from m1: `ssh slime@<node-name>`.
