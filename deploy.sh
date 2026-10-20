#!/usr/bin/env bash
# Push the current coord.py / slime.py to the fleet and restart what runs them.
#   ./deploy.sh coord          # relay: coordinator (state resumes; agents ride it out)
#   ./deploy.sh runners        # every runner host (agents die and are re-embodied from cache)
#   ./deploy.sh all
# Hosts: relay (coordinator), slime-t450-a/b (systemd), m1 + neo (launchd), framework (systemd --user).
set -euo pipefail
cd "$(dirname "$0")"
[ -f site.env ] && . ./site.env
read -ra T450S <<<"${T450S:?fill in site.env (see site.env.example)}"
python3 -m py_compile coord.py slime.py

coord() {   # primary on the relay, standby on framework (each restart resumes its role)
    rsync -a coord.py slime.py dashboard.html canaries.json manifest-v1.json genome.json relay:electric-slime/
    ssh -n relay 'sudo systemctl restart slime-coord && sleep 1 && echo "relay: $(systemctl is-active slime-coord)"'
    rsync -a coord.py dashboard.html canaries.json framework:.local/share/electric-slime/
    ssh -n framework 'systemctl --user restart electric-slime-coord && echo "framework standby: $(systemctl --user is-active electric-slime-coord)"'
}

runners() {
    for ip in "${T450S[@]}"; do
        scp -q slime.py "slime@$ip:/tmp/slime.py"
        ssh -n "slime@$ip" 'sudo install -m 644 /tmp/slime.py /opt/electric-slime/slime.py && rm /tmp/slime.py &&
            sudo systemctl restart slime-runner && echo "$(hostname): $(systemctl is-active slime-runner)"'
    done
    install -m 644 slime.py "$HOME/.local/share/electric-slime/slime.py"
    launchctl kickstart -k "gui/$(id -u)/com.electric-slime.runner" && echo "m1: restarted"
    rsync -a slime.py neo-lan:.local/share/electric-slime/slime.py
    ssh -n neo-lan 'launchctl kickstart -k "gui/$(id -u)/com.electric-slime.runner" && echo "neo: restarted"'
    rsync -a slime.py framework:.local/share/electric-slime/slime.py
    ssh -n framework 'systemctl --user restart electric-slime-runner && echo "framework: restarted"'
}

case "${1:-}" in
coord)   coord ;;
runners) runners ;;
all)     coord; runners ;;
*)       sed -n '2,6p' "$0"; exit 2 ;;
esac
