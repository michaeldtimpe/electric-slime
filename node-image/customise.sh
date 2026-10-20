#!/bin/bash
# Runs INSIDE the image chroot (build-image.sh copies it to /tmp). Expects PUBKEY.
set -euo pipefail
export DEBIAN_FRONTEND=noninteractive LC_ALL=C.UTF-8

chown -R root:root /etc/systemd/system /etc/netplan /etc/sudoers.d /etc/apt/apt.conf.d /usr/local
chmod 755 /usr/local/sbin/slime-firstboot /usr/local/sbin/slime-tailscale-up \
          /usr/local/sbin/slime-sync-efi-boot /usr/local/lib/slime/write-netplan-wifi.py \
          /usr/local/lib/slime/grub-install-no-nvram

# a stick boots other people's laptops: nothing may write their NVRAM. The shim/grub
# upgrade path here is cloud-style grub-multi-install -> bare grub-install, which no
# debconf setting reaches, so wrap grub-install itself (the divert survives upgrades).
dpkg-divert --local --rename --divert /usr/sbin/grub-install.real --add /usr/sbin/grub-install
install -m 755 /usr/local/lib/slime/grub-install-no-nvram /usr/sbin/grub-install
# belt and braces for the non-cloud grub-efi-amd64 postinst path, which does read these
debconf-set-selections <<'DEBCONF'
grub-efi-amd64 grub2/update_nvram boolean false
grub-efi-amd64 grub2/no_efi_extra_removable boolean true
DEBCONF
chmod 440 /etc/sudoers.d/slime
chmod 600 /etc/netplan/50-slime-eth.yaml
visudo -cf /etc/sudoers.d/slime

apt-get update -qq
# real-hardware kernel: generic adds linux-modules-extra (iwlwifi) and linux-firmware
apt-get install -y -qq --no-install-recommends linux-image-generic wpasupplicant iw wireless-regdb cloud-guest-utils libgomp1
apt-get purge -y -qq linux-virtual linux-image-virtual linux-headers-virtual >/dev/null 2>&1 || true
# snap seeding would cost minutes of first boot on a USB stick; nothing here needs snaps
apt-get purge -y -qq snapd >/dev/null 2>&1 || true
rm -rf /var/lib/snapd /snap /var/snap

curl -fsSL https://pkgs.tailscale.com/stable/ubuntu/noble.noarmor.gpg -o /usr/share/keyrings/tailscale-archive-keyring.gpg
curl -fsSL https://pkgs.tailscale.com/stable/ubuntu/noble.tailscale-keyring.list -o /etc/apt/sources.list.d/tailscale.list
apt-get update -qq
apt-get install -y -qq tailscale

# bare metal has no cloud datasource; slime-firstboot does the personalisation instead
touch /etc/cloud/cloud-init.disabled
rm -f /etc/netplan/50-cloud-init.yaml
# initrd-less first boot can't find a USB root (usb-storage is a module): always use the initramfs
rm -f /etc/default/grub.d/40-force-partuuid.cfg
cat > /etc/default/grub.d/50-cloudimg-settings.cfg <<'GRUB'
GRUB_CMDLINE_LINUX_DEFAULT="console=tty1"
GRUB_TIMEOUT=1
GRUB_TIMEOUT_STYLE=hidden
GRUB

useradd -m -s /bin/bash -c "electric-slime node" slime
passwd -l slime >/dev/null
install -d -m 700 -o slime -g slime /home/slime/.ssh
printf '%s\n' "$PUBKEY" > /home/slime/.ssh/authorized_keys
chown slime:slime /home/slime/.ssh/authorized_keys
chmod 600 /home/slime/.ssh/authorized_keys

systemctl enable slime-hostkeys.service slime-firstboot.service slime-tailscale-up.service \
                 slime-runner.service tailscaled.service ssh.socket >/dev/null
systemctl mask sleep.target suspend.target hibernate.target hybrid-sleep.target >/dev/null

update-initramfs -u -k all
update-grub
# removable-media boot path, so any UEFI firmware finds it without an NVRAM entry
test -f /boot/efi/EFI/ubuntu/shimx64.efi -a -f /boot/efi/EFI/ubuntu/grub.cfg
/usr/local/sbin/slime-sync-efi-boot
test -f /boot/efi/EFI/BOOT/grubx64.efi -a ! -e /boot/efi/EFI/BOOT/fbx64.efi
# every library llama-server needs is on the image (it would only fail at a tier's first load)
if ldd /opt/llama/llama-server | grep 'not found'; then exit 1; fi
echo "   llama: $(/opt/llama/llama-server --version 2>&1 | grep -m1 version)"
{ echo "   grub/shim pkgs: $(dpkg-query -W -f='${db:Status-Abbrev} ${Package}\n' 'grub*' 'shim*' | awk '/^ii/{printf "%s ", $2}')"
  debconf-show grub-efi-amd64 grub-pc 2>/dev/null | grep -E 'update_nvram|extra_removable' | sed 's/^/   debconf:/'
  echo "   divert: $(dpkg-divert --list /usr/sbin/grub-install)"; } || true

# per-machine identity is created on each node's first boot, never shipped
truncate -s 0 /etc/machine-id
rm -f /var/lib/dbus/machine-id /etc/ssh/ssh_host_*
rm -rf /var/lib/tailscale/*
apt-get clean
rm -rf /var/lib/apt/lists/* /var/log/*.log /var/log/apt/*
