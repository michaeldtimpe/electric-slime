# runner container

> **Superseded on the Macs (2026-10-19):** m1 and neo now run the runner natively under
> launchd ([native/](../native/README.md)); a container there means a Linux VM, overhead
> with nothing gained. This kit stays for hosts that only do containers. It still
> packages the v0.2 stand-in runner: a v1 image needs llama-server in it (e.g. FROM the
> official `ghcr.io/ggml-org/llama.cpp:server`, Ubuntu 24.04, amd64+arm64).

One image, `electric-slime/runner`, for **amd64 + arm64**: python 3.12 alpine (pinned by
digest) plus `slime.py`, `manifest.json`, `genome.json`. No RUN steps and no weights (shards
arrive over the net), so it is ~42 MB and builds for both arches without emulation.
Runs as `nobody`, read-only root, all capabilities dropped. Default command: a 2-slot pool
against the relay coordinator.

## Build (m1, Docker Desktop with the containerd image store)

    ./container/build.sh

Loads `electric-slime/runner:<tag>` + `:latest` locally and writes
`container/build/slime-runner-<tag>.oci.tar` (+ `.sha256`). The tag is a hash of the three
files and the Dockerfile, so unchanged code rebuilds to the same tag.

## Ship to another host (no registry)

    rsync -a container/slime-runner.sh <host>:electric-slime/container/
    rsync -a container/build/slime-runner-<tag>.oci.tar{,.sha256} <host>:electric-slime/container/build/
    # on the host, from container/build/:
    shasum -a 256 -c slime-runner-<tag>.oci.tar.sha256
    docker load -i slime-runner-<tag>.oci.tar
    docker tag electric-slime/runner:<tag> electric-slime/runner:latest

## Run

    SLOTS=4 CPUS=2 ./container/slime-runner.sh up     # m1
    SLOTS=2 CPUS=1 ./container/slime-runner.sh up     # neo
    ./container/slime-runner.sh status | logs | down

Container `slime-runner`, node name `<hostname>-docker`, `--restart unless-stopped` (comes
back with Docker Desktop), capped by `CPUS` and `MEMORY` (default 256m; the stand-in runner
uses ~15 MiB). It dials `$COORD` (site.env) over the LAN, so away from home it just
idles and retries.

| Host | Node | Slots | Caps |
|---|---|---|---|
| m1 (M1 Max) | `m1-docker` | 4 | 2 CPUs, 256 MiB |
| neo (A18 Pro, 8 GB) | `neo-docker` | 2 | 1 CPU, 256 MiB |

An amd64 host can take the same archive if it's ever needed: 1 slot, hard caps.

## Networking note

Docker Desktop's userspace network proxy occasionally swallows a *new* outbound connection
(measured from m1: 13 of 757 calls hung 10 s with a connection per call; 0 from the host).
With a 5 s heartbeat window that got container-held agents reaped. Fixed in the protocol, not
the runtime: the client keeps one HTTP/1.1 connection per slot with a 3 s timeout, and the
coordinator's window is 10 s. After the fix: 0 of 1172 calls stalled from the same container,
and container-held tiers held steady (worst heartbeat gap 0.68 s over 3 min).
