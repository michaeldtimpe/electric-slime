# native runners

A runner is a plain process tree: `slime.py pool` plus one `llama-server` per embodied
agent. The host's own supervisor is the container manager (launchd on macOS, systemd on
Linux); "isolation" is a directory and a memory budget the coordinator enforces. No VM:
on a Mac a container means a Linux VM, which costs RAM and I/O for binaries that run
natively anyway.

Every host runs the **same llama.cpp release, b11399** (2026-10-04), pinned by sha256:

| Build | sha256 |
|---|---|
| `llama-b11399-bin-macos-arm64.tar.gz` | `bbd48f4f…3df3f254` |
| `llama-b11399-bin-ubuntu-x64.tar.gz` | `da013120…ad63dcf8b08` |

The Ubuntu build carries one CPU backend per x86 generation and picks at load (the T450s'
Broadwell gets the `haswell` one); it needs `libgomp1`, which the stick image installs.

What every runner does for the swarm's immune system, whatever supervises it:
- **SIGTERM = leave:** a service stop or deploy (`launchctl kickstart -k`, `systemctl
  restart`) hands every agent back to the coordinator before exiting, so nothing waits out a
  heartbeat and the host isn't charged a lapse. SIGKILL can't do that; it costs one lapse.
- **Pressure:** the OS's own signals, sampled every 5 s — macOS
  `kern.memorystatus_vm_pressure_level` and `pmset -g therm`; Linux `/proc/pressure/memory`
  (PSI) and the CPU package throttle counter. Warn (memory or heat): take no new agents.
  Critical memory: leave with the lowest-tier agent, one per 30 s.
- **Sleep:** a laptop that suspended notices on waking (the monotonic clock stopped, the wall
  clock didn't) and reports the window, so the lapse it caused is excused.

## macOS (m1, neo)

    native/macos/slime-node.sh install     # SLOTS, SLIME_CAPACITY_MB, SLIME_THREADS, COORD
    native/macos/slime-node.sh status | logs | uninstall

Installs to `~/.local/share/electric-slime/` (llama build, `slime.py`, `cache/`, `runner.log`)
and a user LaunchAgent `com.electric-slime.runner` (KeepAlive, nice 10, starts at login).

Two macOS specifics, both measured on m1:

- **Local Network privacy:** a launchd job that never got the "allow local network" prompt
  can't open LAN connections (`EHOSTUNREACH`), while the same script works from a terminal.
  So Mac runners dial the coordinator's **tailnet** address (`COORD_TAILNET` in site.env),
  and peers advertise their tailnet address after their LAN one. Inbound connections to
  the runner's peer server are fine (the system Python is Apple-signed).
- **Not under `~/Downloads`:** TCC would block a launchd job from reading it; hence
  `~/.local/share`.

Current settings: m1 4 slots / 8000 MB / 4 threads; neo 2 slots / 3500 MB / 3 threads.
Docker Desktop is not needed for any of this.

## Linux desktop (framework)

    native/linux/slime-node.sh install     # SLOTS, SLIME_CAPACITY_MB, SLIME_THREADS, SLIME_MAX_TIER
    native/linux/slime-node.sh status | logs | uninstall

No root and no packages (needs `libgomp1`, which desktop Ubuntu has): `~/.local/share/electric-slime/`
plus a systemd **user** unit `electric-slime-runner.service` (nice 10; `MemoryHigh`/`MemoryMax`
= capacity + 1/2 GB, enforced because user@ has the memory controller delegated). A user
unit runs while the user has a session; `loginctl enable-linger` would keep it running
without one — left to the machine's owner. framework: 4 slots / 12000 MB / 4 threads, no tier
cap, so it is the second host that can run the 4B (m1 being the other).

### Standby coordinator (framework)

    native/linux/coord-standby.sh install | uninstall | status | logs

A second user unit, `electric-slime-coord.service`, on :8700: `coord.py serve --role standby
--peer $COORD`, depot = this host's runner cache, `--auth required` with
its own copy of `hosts.keys` (kept in step by `enroll-host.sh`) and its own `admin.token`.
Promote on framework with `slime.py promote --coord http://127.0.0.1:8700
--admin-token-file ~/.local/share/electric-slime/admin.token`.

### Identity

Each runner signs its calls with `host.key` (this directory's parent of `cache/` on the
Macs and framework, user-only; `/etc/slime/host.key`, root:slime 640, on the T450s). A
host without an enrolled key is refused by both coordinators.

## Linux (T450 USB nodes)

`/opt/llama -> /opt/llama-b11399`, `slime-runner.service` (systemd: `CacheDirectory=slime`
→ `/var/cache/slime`, `MemoryHigh=85%`, `MemoryMax=90%`, nice 5), slots from
`/etc/slime/node.env` (default 3). Both are baked into the stick image (node-image/).
