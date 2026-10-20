#!/usr/bin/env python3
"""Emit netplan YAML for the node's wifi from personalize.sh's wifi.json.

All access points go in one wpa_supplicant config at equal priority, so the supplicant
picks the best by estimated throughput: 5 GHz `below` near the router, 2.4 GHz `above`
on the far side of the house. Ethernet (50-slime-eth.yaml, metric 100) beats both.
JSON strings are valid YAML double-quoted scalars, so json.dumps quotes PSKs safely."""
import glob, json, os, sys

nets = json.load(open(sys.argv[1]))
country = sys.argv[2]
ifaces = sorted(os.path.basename(os.path.dirname(p)) for p in glob.glob("/sys/class/net/*/wireless"))
if not ifaces:
    sys.exit("no wireless interface found")

print("network:\n  version: 2\n  renderer: networkd\n  wifis:")
for iface in ifaces:
    print(f"    {iface}:")
    print("      dhcp4: true\n      optional: true")
    print("      dhcp4-overrides:\n        route-metric: 600")
    print(f"      regulatory-domain: {json.dumps(country)}")
    print("      access-points:")
    for n in nets:
        print(f"        {json.dumps(n['ssid'])}:\n          password: {json.dumps(n['psk'])}")
