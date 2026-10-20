#!/usr/bin/env bash
# Register a host's signing key with both coordinators. The key comes on stdin and only
# ever travels through ssh pipes: it is never printed or written anywhere else.
#   ssh HOST 'cat <keyfile>' | ./enroll-host.sh HOST
#   ./enroll-host.sh --revoke HOST
set -euo pipefail
RELAY_KEYS='~/electric-slime/hosts.keys'
STANDBY_KEYS='~/.local/share/electric-slime/hosts.keys'
op=enroll
if [ "${1:-}" = --revoke ]; then op=revoke; shift; fi
HOST=${1:?usage: enroll-host.sh [--revoke] HOST  (key on stdin to enroll)}
[[ "$HOST" =~ ^[a-z0-9][a-z0-9-]{0,62}$ ]] || { echo "bad host name: $HOST"; exit 1; }
key=""
[ $op = enroll ] && key=$(cat)

run() {   # $1 = ssh target, $2 = coord.py path, $3 = keys path
    printf '%s\n' "$key" | ssh "$1" "python3 $2 $op $HOST --keys $3"
}
run relay '~/electric-slime/coord.py' "$RELAY_KEYS"
run framework '~/.local/share/electric-slime/coord.py' "$STANDBY_KEYS" ||
    echo "standby (framework) not updated: re-run when it is reachable"
