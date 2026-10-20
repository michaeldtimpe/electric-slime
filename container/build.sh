#!/usr/bin/env bash
# Build the runner image for amd64 + arm64 and load it into this host's Docker (needs the
# containerd image store, the Docker Desktop default). Also writes an OCI archive that
# another host can `docker load` without a registry:
#   ./container/build.sh                    # -> electric-slime/runner:<tag> (+ :latest)
#   docker load -i container/build/slime-runner-<tag>.oci.tar     # on the other host
set -euo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(dirname "$HERE")
PLATFORMS=${PLATFORMS:-linux/amd64,linux/arm64}

# tag = content of the three files, so a rebuild of unchanged code is the same tag
TAG=$(cat "$ROOT/slime.py" "$ROOT/manifest.json" "$ROOT/genome.json" "$HERE/Dockerfile" \
      | shasum -a 256 | cut -c1-12)
IMG=electric-slime/runner

mkdir -p "$HERE/build"
docker buildx build --platform "$PLATFORMS" -f "$HERE/Dockerfile" \
    -t "$IMG:$TAG" -t "$IMG:latest" --load "$ROOT"
docker buildx build --platform "$PLATFORMS" -f "$HERE/Dockerfile" \
    -t "$IMG:$TAG" --output "type=oci,dest=$HERE/build/slime-runner-$TAG.oci.tar" "$ROOT"
( cd "$HERE/build" && shasum -a 256 "slime-runner-$TAG.oci.tar" > "slime-runner-$TAG.oci.tar.sha256" )
echo "built $IMG:$TAG for $PLATFORMS"
ls -l "$HERE/build/slime-runner-$TAG.oci.tar"
