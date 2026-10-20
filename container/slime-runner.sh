#!/usr/bin/env bash
# Run this host's electric-slime runner container, capped so it can't crowd out the host.
#   ./slime-runner.sh up        # start (or replace) it; restarts with Docker
#   ./slime-runner.sh down      # stop + remove it
#   ./slime-runner.sh status | logs
# Settings via env: SLOTS (2), CPUS (1), MEMORY (256m), COORD, NODE (<short hostname>-docker).
set -euo pipefail
NAME=slime-runner
IMG=${IMG:-electric-slime/runner:latest}
[ -f "$(dirname "$0")/../site.env" ] && . "$(dirname "$0")/../site.env"
COORD=${COORD:?set COORD or fill in site.env (see site.env.example)}
SLOTS=${SLOTS:-2}
NODE=${NODE:-$(hostname -s | tr '[:upper:]' '[:lower:]')-docker}

case "${1:-}" in
up)
    docker rm -f "$NAME" >/dev/null 2>&1 || true
    docker run -d --name "$NAME" --hostname "$NODE" --restart unless-stopped \
        --cpus "${CPUS:-1}" --memory "${MEMORY:-256m}" --pids-limit 64 \
        --read-only --cap-drop ALL --security-opt no-new-privileges \
        --log-opt max-size=5m --log-opt max-file=2 \
        "$IMG" pool --coord "$COORD" --slots "$SLOTS" --node "$NODE"
    echo "$NAME up: node $NODE, $SLOTS slots -> $COORD"
    ;;
down)   docker rm -f "$NAME" ;;
status) docker ps -a --filter "name=^$NAME\$" --format '{{.Names}}  {{.Status}}  {{.Image}}' ;;
logs)   docker logs --tail "${TAIL:-40}" "$NAME" ;;
*)      sed -n '2,7p' "$0"; exit 2 ;;
esac
