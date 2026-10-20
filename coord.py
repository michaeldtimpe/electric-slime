#!/usr/bin/env python3
"""
electric-slime coordinator — the networked arena.

Holds the whole shared medium behind a small HTTP API so agents can be processes,
containers, or USB-booted bare metal, on any arch, anywhere on the LAN/tailnet. There is
no shared filesystem: this service is the single source of truth.

State is held in memory under one lock and snapshotted to disk so a restart survives.
The control signal is `wants[tier]` = desired live replicas of that tier; growth (via
/assign) and trimming (via /retire) both chase it. Promotion sets it.

v1 adds a depot: content-addressed blobs on disk (the base model and every shard of the
larger ones), served at GET /blob/<sha256>. `stage` fills it from the manifest: download,
verify against the publisher's SHA-256, byte-split each larger model into its shards.
A tier with no `model` in the manifest is a stand-in (v0 behaviour, blobs over JSON).

Hosts are known by key: each runner signs its calls (HMAC-SHA256 over method, path, time
and body) with a per-host key from the keys file, and may act only on its own agents.
Once assembled, a controller moves each tier's want between its base and a max as work
waits or agents sit idle (GET/POST /control).

A standby coordinator replicates the primary's state and takes over when promoted; an
epoch number fences off a primary that comes back after being replaced.

    coord.py stage --manifest manifest-v1.json --depot DIR      # once, offline
    coord.py serve --port 8700 [--depot DIR] [--state coord-state.json] [--keys FILE]
                   [--auth off|optional|required] [--role primary|standby --peer URL]
    coord.py enroll HOST --keys FILE   (key on stdin)  ·  coord.py revoke HOST --keys FILE
    POST it the manifest+genome once with `slime.py seed --coord URL --manifest FILE`.
"""
from __future__ import annotations
import argparse, base64, hashlib, hmac, json, os, re, secrets, statistics, sys, threading, time, zlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse, parse_qs
from urllib.request import urlopen

HB_TIMEOUT = 10.0   # failover delay; also the margin for wifi and container NAT hiccups
LOCK = threading.Lock()
STATE: dict = {}
STATE_PATH = "coord-state.json"
DEPOT: Path | None = None
ADMIN_TOKEN: str | None = None   # when set, /seed (wipes the swarm) and /promote require it
MAX_BODY = 1 << 20       # bytes in any POST
MAX_QUESTION = 4000      # characters in a posted goal
ADMIN_ROUTES = {"/seed", "/promote", "/control", "/immune"}
SHA_RE = re.compile(r"^[0-9a-f]{64}$")

# persistence: what an acknowledged caller relies on is on disk before the reply; the
# chatter (assign/claim/retire polls, several a second per slot) is batched once a second
SYNC_ROUTES = {"/seed", "/promote", "/control", "/immune", "/task/fail", "/goal/fail",
               "/goal/post", "/goal/done", "/task/post", "/task/done",
               "/model_ready", "/elect", "/deposit"}
LIVENESS_ROUTES = {"/heartbeat", "/node"}   # stale after any restart: never worth a write
DIRTY = threading.Event()
KEEP_GOALS = 50          # finished goals kept in state; older ones go to the archive
EVENTS_KEPT = 500        # completed-work events kept for /metrics

# identity: runner calls are signed per host; a host may act only on its own agents
RUNNER_ROUTES = {"/assign", "/node", "/heartbeat", "/leave", "/deposit", "/elect",
                 "/model_ready", "/retire", "/goal/claim", "/goal/done", "/task/post",
                 "/task/claim", "/task/done", "/task/fail", "/goal/fail", "/report"}
AGENT_ROUTES = RUNNER_ROUTES - {"/assign", "/node", "/report"}
AUTH = "off"             # off | optional (verify when signed) | required
KEYS_PATH: Path | None = None
KEYS: dict[str, bytes] = {}
_keys_seen = [0.0, 0.0]  # file mtime, last check
SKEW = 30.0              # seconds a signed timestamp may be off
SEEN_SIGS: dict[str, float] = {}   # signature -> when it may be forgotten
SIG_LOCK = threading.Lock()
_pruned = [0.0]
UNSIGNED: dict[str, int] = {}   # runner calls that arrived unsigned, by caller (migration aid)

# high availability: one primary, an optional replicating standby
ROLE = "primary"
PEER: str | None = None
ROLE_PATH = ""


# --- stand-in model primitives (tiers without a real `model`) -----------------
def make_shard_bytes(provides_tier: int, index: int) -> bytes:
    seed = hashlib.sha256(f"slime-v0:tier{provides_tier}:shard{index}".encode()).digest()
    return seed * 64


def shard_sha(provides_tier: int, index: int) -> str:
    return hashlib.sha256(make_shard_bytes(provides_tier, index)).hexdigest()


# --- state helpers (call under LOCK) ------------------------------------------
def now() -> float:
    return time.time()


def tier_spec(tier: int) -> dict | None:
    for t in STATE.get("manifest", {}).get("tiers", []):
        if t["tier"] == tier:
            return t
    return None


def reap() -> None:
    """Drop agents whose heartbeat lapsed, and release whatever they were holding:
    an election they won, a task or goal they were working on."""
    dead = [aid for aid, a in STATE["agents"].items() if now() - a["last_hb"] >= HB_TIMEOUT]
    if dead:
        note_lapse({STATE["agents"][aid].get("host") for aid in dead})
    for aid in dead:
        del STATE["agents"][aid]
    alive = STATE["agents"]
    for tier, aid in list(STATE["electors"].items()):
        if aid not in alive and tier not in STATE["ready"]:
            del STATE["electors"][tier]          # assembler died mid-stitch: re-elect
    for t in STATE["tasks"].values():
        if t["status"] == "doing" and t["worker"] not in alive:
            retry_task(t, t.get("worker_host"), "its worker was lost")
    for g in STATE["goals"].values():
        if g["status"] == "active" and g["orchestrator"] is not None \
                and g["orchestrator"] not in alive:
            retry_goal(g, g.get("orchestrator_host"), "its orchestrator was lost")


def used() -> int:
    return sum(a["cost"] for a in STATE["agents"].values())


def node_used(host: str) -> int:
    return sum(a["cost"] for a in STATE["agents"].values() if a.get("host") == host)


def live_counts() -> dict[int, int]:
    out: dict[int, int] = {}
    for a in STATE["agents"].values():
        out[a["tier"]] = out.get(a["tier"], 0) + 1
    return out


WRITE_LOCK = threading.Lock()   # orders snapshot writes; never held together with LOCK's work
_snap_seq = [0, 0, 0]           # last taken, last written, last fsync'd


def snapshot() -> tuple[int, bytes]:
    """The state, serialised (call under LOCK: milliseconds, no disk)."""
    _snap_seq[0] += 1
    DIRTY.clear()
    return _snap_seq[0], json.dumps(STATE).encode()


def write_snapshot(snap: tuple[int, bytes], sync: bool = False) -> None:
    """Write a snapshot atomically, fsync'd when `sync` (acknowledged work), outside LOCK:
    a slow disk delays only the caller that needs durability, never the heartbeats. A
    snapshot older than one already written is dropped."""
    seq, data = snap
    with WRITE_LOCK:
        if seq <= _snap_seq[1]:   # a newer snapshot (with this one's changes) is on disk
            if sync and seq > _snap_seq[2]:
                try:
                    fd = os.open(STATE_PATH, os.O_RDONLY)
                    try:
                        os.fsync(fd)
                    finally:
                        os.close(fd)
                    _snap_seq[2] = _snap_seq[1]
                except OSError as e:
                    print(f"persist failed: {e}", flush=True)
            return
        try:
            tmp = STATE_PATH + ".tmp"
            with open(tmp, "wb") as f:
                f.write(data)
                if sync:
                    f.flush()
                    os.fsync(f.fileno())
            os.replace(tmp, STATE_PATH)
            _snap_seq[1] = seq
            if sync:
                _snap_seq[2] = seq
        except OSError as e:
            print(f"persist failed: {e}", flush=True)


def persist(sync: bool = False) -> None:
    """Snapshot and write in one go (call under LOCK; for rare paths only)."""
    write_snapshot(snapshot(), sync)


def writer_loop() -> None:
    """Batch the chatter: at most one snapshot a second, only when something changed."""
    while True:
        DIRTY.wait()
        time.sleep(1.0)
        with LOCK:
            snap = snapshot() if DIRTY.is_set() else None
        if snap:
            write_snapshot(snap)


def archive_path() -> Path:
    return Path(STATE_PATH).with_name("goals-archive.jsonl")


def archive_done_goals() -> None:
    """Keep state small: finished goals beyond the newest KEEP_GOALS (and their tasks) move
    to an append-only archive, which is the long-term record."""
    done = sorted((g for g in STATE["goals"].values() if g["status"] == "done"),
                  key=lambda g: int(g["id"]))
    old = done[:-KEEP_GOALS] if len(done) > KEEP_GOALS else []
    if not old:
        return
    with open(archive_path(), "a") as f:
        for g in old:
            f.write(json.dumps(goal_view(g)) + "\n")
            for tid in g["tasks"]:
                STATE["tasks"].pop(tid, None)
            del STATE["goals"][g["id"]]


EVENTS_LOG_MAX = 50 << 20   # bytes; then it becomes events.jsonl.1 (one generation kept)


def events_path() -> Path:
    return Path(STATE_PATH).with_name("events.jsonl")


def record(ev: dict) -> None:
    """An event: kept in state (the last EVENTS_KEPT, for /metrics and the controller) and
    appended to events.jsonl, the long-term record (GET /events, `slime.py report`)."""
    ev["t"] = now()
    evs = STATE.setdefault("events", [])
    evs.append(ev)
    del evs[:-EVENTS_KEPT]
    try:
        p = events_path()
        if p.exists() and p.stat().st_size > EVENTS_LOG_MAX:
            os.replace(p, p.with_name(p.name + ".1"))
        with open(p, "a") as f:
            f.write(json.dumps(ev) + "\n")
    except OSError as e:
        print(f"events log: {e}", flush=True)


def read_events(since: float, limit: int = 50000) -> list[dict]:
    out: list[dict] = []
    p = events_path()
    for f in (p.with_name(p.name + ".1"), p):
        try:
            with open(f) as fh:
                for line in fh:
                    try:
                        e = json.loads(line)
                    except ValueError:
                        continue
                    if e.get("t", 0) >= since:
                        out.append(e)
        except OSError:
            pass
    return out[-limit:]


def review_verdict(result: str) -> bool | None:
    try:
        v = json.loads(result)
        return bool(v["agrees"]) if isinstance(v, dict) and "agrees" in v else None
    except (ValueError, TypeError):
        return None


def fresh_state(budget: int) -> dict:
    return {"budget": budget, "agents": {}, "wants": {}, "ready": {}, "electors": {},
            "shards": {}, "tasks": {}, "goals": {}, "nodes": {}, "next_id": 1,
            "events": [], "epoch": STATE.get("epoch", 1), "ctl_timers": {}, "ctl_changed": {}, "ctl_blocked": {},
            "escalation": {"on": False, "since": 0.0, "hot": 0.0, "why": ""},
            "health": STATE.get("health", {}), "lapse_pending": [], "excess_since": {}}


def next_id() -> str:
    i = STATE["next_id"]
    STATE["next_id"] = i + 1
    return str(i)


def add_goal(text: str) -> str:
    gid = next_id()
    STATE["goals"][gid] = {"id": gid, "text": text, "status": "todo", "orchestrator": None,
                           "tasks": [], "answer": None, "posted": now(), "done": None}
    ab = antibody(text) if "antibodies" in STATE else None
    if ab:   # adaptive memory: this exact question was given up as poison before
        STATE["goals"][gid].update(status="done", done=now(),
                                   answer=f"(no answer: known poison input; {ab['why']})")
        record({"type": "antibody", "goal": gid})
    return gid


# --- request handlers (each returns a JSON-able dict; called under LOCK) ------
def h_seed(body: dict) -> dict:
    STATE.update(fresh_state(body.get("budget", STATE.get("budget", 16))))
    STATE["manifest"] = body["manifest"]
    STATE["genome"] = body["genome"]
    STATE["control"] = control_config(body["manifest"])
    STATE["immune"] = {**IMMUNE_DEFAULTS, **{k: v for k, v in body["manifest"].get(
        "immune", {}).items() if k in IMMUNE_DEFAULTS}}
    t0 = tier_spec(0)
    STATE["wants"] = {str(t0["tier"]): t0["seed"]}
    goal = body["genome"].get("goal")
    if isinstance(goal, dict) and goal.get("text"):
        add_goal(goal["text"])   # the standing goal the swarm works on once assembled
    return {"ok": True, "budget": STATE["budget"], "wants": STATE["wants"]}


def model_need(spec: dict) -> str | None:
    """The blob a host must hold to run this tier without downloading (None: stand-in)."""
    m = spec.get("model")
    return (m.get("blob") or m.get("sha256")) if m else None


GRACE = 30.0   # seconds a short tier waits for a host that already holds its model


def holder_with_room(need: str, cost: int, host: str) -> bool:
    """Another live host already holds `need` and has the memory for it now."""
    for h, n in STATE["nodes"].items():
        if (h == host or need not in n.get("cached", ()) or now() - n.get("seen", 0) > 20
                or quarantined(h)):
            continue
        if n.get("capacity_mb") is None or n["capacity_mb"] - node_used(h) >= cost:
            return True
    return False


def h_assign(body: dict) -> dict:
    """FCFS: give an idle runner the highest-tier unmet want that fits the global budget
    and, when the runner reports one, its node's memory capacity. Cache-aware: a host that
    would have to download the tier's model yields, for a grace period, to a live host
    that already holds it and has room."""
    reap()
    host, cap = body.get("_auth") or body.get("host"), body.get("capacity_mb")
    cached = set(body.get("cached") or ())
    max_tier = body.get("max_tier", 99)
    if host:
        STATE["nodes"].setdefault(host, {}).update(
            capacity_mb=cap, cpus=body.get("cpus"), seen=now(), polled=now(),
            cached=sorted(cached), peer=body.get("peer"), max_tier=max_tier,
            pressure=body.get("pressure", "ok"))
        if quarantined(host) or body.get("pressure", "ok") != "ok":
            return {"assigned": False, "quarantined": quarantined(host)}
    counts = live_counts()
    free = STATE["budget"] - used()
    node_free = cap - node_used(host) if host and cap else None
    short = STATE.setdefault("short_since", {})
    r = relocation()
    for tier in sorted((int(k) for k in STATE["wants"]), reverse=True):
        want = STATE["wants"][str(tier)]
        spec = tier_spec(tier)
        if r and (host == r["host"]) != (tier == r["tier"]):
            continue   # a host reserved for the lead tier takes only that, and only it does
        if counts.get(tier, 0) >= want:
            short.pop(str(tier), None)
            continue
        since = short.setdefault(str(tier), now())
        if spec["cost"] > free or tier > max_tier:
            continue
        if node_free is not None and spec["cost"] > node_free:
            continue
        # models are GBs and some links are 2 MB/s wifi: for GRACE seconds a short tier is
        # kept for a host that already holds its model (its slot may be freeing right now)
        need = model_need(spec)
        if (host and need and need not in cached and now() - since < GRACE
                and holder_with_room(need, spec["cost"], host)):
            continue
        # a host with recent incidents gets work only when no healthy one took it first
        if host and health(host)["score"] >= immune_cfg()["release_at"] and now() - since < 10:
            continue
        # nor does a slow host while a much faster one, idle and with room, could take it
        if host and now() - since < SPEED_GRACE and faster_taker(host, tier, spec["cost"]):
            continue
        # an extra orchestrator goes to a host that isn't running one already
        if host and counts.get(tier, 0) >= base_want(tier) and doubles_up(host, tier):
            continue
        n = spec.get("provides")
        shard_index = None
        if n:
            holders: dict[int, int] = {i: 0 for i in range(n["n_shards"])}
            for a in STATE["agents"].values():
                if a.get("provides_tier") == n["tier"] and a.get("shard_index") is not None:
                    holders[a["shard_index"]] = holders.get(a["shard_index"], 0) + 1
            shards = {s["index"]: s["sha"]
                      for s in (tier_spec(n["tier"]).get("model") or {}).get("shards", [])}
            # least-carried shard first; among equals, one this host already has
            shard_index = min(holders, key=lambda i: (holders[i], shards.get(i) not in cached))
        slot = body.get("node", "?")
        aid = f"t{tier}-{slot}-{int(now()*1000)%100000}"
        STATE["agents"][aid] = {
            "tier": tier, "cost": spec["cost"], "node": slot, "host": host or slot,
            "last_hb": now(), "status": "alive", "phase": "embodying",
            "provides_tier": n["tier"] if n else None, "shard_index": shard_index,
            "holds": False,
        }
        return {"assigned": True, "agent_id": aid, "tier": tier,
                "shard_index": shard_index, "spec": spec,
                "manifest": STATE["manifest"], "genome": STATE["genome"]}
    return {"assigned": False}


SPEED_GRACE = 8.0     # seconds a short tier waits for a much faster idle host
SPEED_YIELD = 1.5     # "much faster"


def faster_taker(host: str, tier: int, cost: int) -> bool:
    sp = _speeds()
    mine = host_speed(host, tier, sp)
    if not mine:
        return False
    return any(h != host and eligible(h, tier) and room(h) >= cost
               and now() - n.get("polled", 0) < 5
               and host_speed(h, tier, sp) >= SPEED_YIELD * mine
               for h, n in STATE["nodes"].items())


def excuse_sleep(host: str, windows) -> None:
    """The host was suspended (lid shut): lapses inside those windows were the sleep's,
    not a fault. Pending ones are dropped; charged ones are taken back (as decayed)."""
    try:
        wins = [(float(a), float(b)) for a, b in windows][:5]
    except (TypeError, ValueError):
        return
    wins = [(a, b) for a, b in wins if 0 < b - a < 7 * 86400 and b <= now() + 60]
    if not wins:
        return
    inside = lambda t: any(a - HB_TIMEOUT <= t <= b + LAPSE_SETTLE for a, b in wins)  # noqa: E731
    STATE["lapse_pending"] = [p for p in STATE.get("lapse_pending", [])
                              if not (p["host"] == host and inside(p["t"]))]
    hl = immune_cfg()["half_life"]
    for e in STATE.get("events", []):
        if (e["type"] == "incident" and e["host"] == host and e["kind"] == "lapse"
                and not e.get("excused") and inside(e["t"])):
            e["excused"] = True
            h = health(host)
            h["score"] = max(0.0, h["score"] - WEIGHTS["lapse"] * 0.5 ** ((now() - e["t"]) / hl))
    record({"type": "slept", "host": host,
            "seconds": round(sum(b - a for a, b in wins))})


def h_node(body: dict) -> dict:
    """A host's periodic announcement: what it holds and where peers can fetch it."""
    if body.get("slept"):
        excuse_sleep(body.get("_auth") or body["host"], body["slept"])
    STATE["nodes"].setdefault(body.get("_auth") or body["host"], {}).update(
        capacity_mb=body.get("capacity_mb"), cpus=body.get("cpus"), seen=now(),
        cached=sorted(body.get("cached") or ()), peer=body.get("peer"),
        max_tier=body.get("max_tier", 99), pressure=body.get("pressure", "ok"),
        pressure_why=body.get("pressure_why", ""))
    return {"ok": True}


def peers_holding(sha: str, exclude: str) -> list[str]:
    return [n["peer"] for h, n in STATE["nodes"].items()
            if h != exclude and n.get("peer") and sha in n.get("cached", ())
            and now() - n.get("seen", 0) < 20 and not quarantined(h)]


def h_heartbeat(body: dict) -> dict:
    a = STATE["agents"].get(body["agent_id"])
    if not a:
        return {"status": "gone"}
    a["last_hb"] = now()
    if body.get("phase"):
        a["phase"] = body["phase"]
    return {"status": a["status"]}


def h_leave(body: dict) -> dict:
    """An agent goes on purpose: done, shut down, shedding load, or unable to embody."""
    aid = body["agent_id"]
    a = STATE["agents"].pop(aid, None)
    # work in its hands goes back as it was: leaving on purpose isn't the work's failure
    for t in STATE["tasks"].values():
        if t["status"] == "doing" and t["worker"] == aid:
            t.update(status="todo", worker=None, requeued=now())
    for g in STATE["goals"].values():
        if g["status"] == "active" and g["orchestrator"] == aid:
            g.update(orchestrator=None, requeued=now())
    if a and body.get("reason") == "embody_failed":
        incident(a.get("host"), "embody_failed", f"tier{a['tier']}: {body.get('error', '')}")
    elif a and body.get("reason"):
        record({"type": "leave", "host": a.get("host"), "tier": a["tier"],
                "reason": str(body["reason"])[:40]})
    return {"ok": True}


def h_deposit(body: dict) -> dict:
    """An agent now carries its shard. Stand-in shards arrive inline (`blob`); real ones
    already sit in the depot by sha, so the deposit is the agent's claim to hold it."""
    key = f"tier{body['provides_tier']}"
    entry = {"sha": body["sha"]}
    if "blob" in body:
        entry["blob"] = body["blob"]
    STATE["shards"].setdefault(key, {})[str(body["index"])] = entry
    a = STATE["agents"].get(body.get("agent_id", ""))
    if a:
        a["holds"] = True
    return {"ok": True}


def h_elect(body: dict) -> dict:
    """One assembler per tier. First caller wins; the rest are told to stand down.
    Quorum: k distinct shards carried by live agents, and k live replicas below."""
    reap()
    tier = str(body["tier"])
    if tier in STATE["ready"] or tier in STATE["electors"]:
        return {"won": False}
    if quarantined(STATE["agents"].get(body.get("agent_id", ""), {}).get("host")):
        return {"won": False}
    if tier_spec(int(tier)) and tier_spec(int(tier)).get("on_demand") and not escalated():
        return {"won": False}   # a dormant tier is carried, not assembled
    below = int(tier) - 1
    k = tier_spec(below)["provides"]["k"]
    held = {a["shard_index"] for a in STATE["agents"].values()
            if a.get("provides_tier") == int(tier) and a.get("holds")}
    if len(held) < k or live_counts().get(below, 0) < k:
        return {"won": False}
    STATE["electors"][tier] = body.get("agent_id")
    return {"won": True, "k": k}


def h_model_ready(body: dict) -> dict:
    """Promotion committed: record READY and update wants (grow next, trim below)."""
    tier = body["tier"]
    elector = STATE["electors"].get(str(tier))
    if elector and body.get("agent_id") != elector:
        return {"ok": False, "error": "only the elected assembler can promote"}
    STATE["ready"][str(tier)] = body["sha"]
    # the assembler's host holds the stitched model now; don't wait for its next announce
    a = STATE["agents"].get(body.get("agent_id", ""))
    need = model_need(tier_spec(tier))
    if a and need and a.get("host") in STATE["nodes"]:
        n = STATE["nodes"][a["host"]]
        n["cached"] = sorted(set(n.get("cached", ())) | {need})
    STATE["wants"][str(tier)] = tier_spec(tier)["target"]
    for t in range(tier):
        STATE["wants"][str(t)] = tier_spec(t)["serve_floor"]
    return {"ok": True, "wants": STATE["wants"]}


def eligible(host: str, tier: int) -> bool:
    n = STATE["nodes"].get(host, {})
    return (now() - n.get("seen", 0) < 20 and n.get("max_tier", 99) >= tier
            and n.get("pressure", "ok") == "ok" and not quarantined(host))


def room(host: str) -> int:
    cap = STATE["nodes"].get(host, {}).get("capacity_mb")
    return 1 << 30 if cap is None else cap - node_used(host)


def evict_for(a: dict) -> int | None:
    """Lower tiers terminate to free resources: if a higher tier has been short for a while
    and no eligible host has room for it, but this agent's host would after retiring
    lower-tier agents, this one goes (each one asks on its own tick; it stops as soon as
    there is room). Returns the tier it makes way for."""
    host, counts, short = a.get("host"), live_counts(), STATE.get("short_since", {})
    for t in sorted((int(k) for k in STATE["wants"]), reverse=True):
        if t <= a["tier"] or counts.get(t, 0) >= STATE["wants"][str(t)]:
            continue
        if now() - short.get(str(t), now()) < 5 or not eligible(host, t):
            continue
        if counts.get(t, 0) >= base_want(t):
            continue   # only restoring a tier to its base evicts; scaling out never does
        cost = tier_spec(t)["cost"]
        if any(eligible(h, t) and room(h) >= cost for h in STATE["nodes"]):
            continue   # it fits somewhere already: no need to evict anyone
        lower = [x for x in STATE["agents"].values() if x.get("host") == host and x["tier"] < t]
        free = room(host)
        # fewest evictions: if one agent's going is enough, only such an agent goes (the
        # lowest tier of them); otherwise lower agents go one by one until it fits
        singles = [x for x in lower if free + x["cost"] >= cost]
        if singles:
            if a["tier"] == min(x["tier"] for x in singles) and a in singles:
                return t
        elif free + sum(x["cost"] for x in lower) >= cost:
            return t
    return None


def h_retire(body: dict) -> dict:
    """Right-to-die lease: honored if live > want for this tier (floor held), or if the
    agent must make way for a higher tier (see evict_for)."""
    reap()
    a = STATE["agents"].get(body["agent_id"])
    if not a:
        return {"retire": True}  # already gone
    if quarantined(a.get("host")) and not last_refuge(a):
        del STATE["agents"][body["agent_id"]]
        return {"retire": True, "quarantined": True}
    r = relocation()
    if r and body["agent_id"] in r["evict"]:
        r["evict"].remove(body["agent_id"])
        del STATE["agents"][body["agent_id"]]
        return {"retire": True, "for_tier": r["tier"]}
    want = STATE["wants"].get(str(a["tier"]), tier_spec(a["tier"])["serve_floor"])
    if live_counts().get(a["tier"], 0) > want:
        if not first_to_go(a):
            return {"retire": False}
        del STATE["agents"][body["agent_id"]]
        STATE.get("excess_since", {}).pop(str(a["tier"]), None)
        return {"retire": True}
    STATE.get("excess_since", {}).pop(str(a["tier"]), None)
    t = evict_for(a)
    if t is not None:
        del STATE["agents"][body["agent_id"]]
        return {"retire": True, "for_tier": t}
    return {"retire": False, "lead": lead_tier(), "top": standing_top()}


# --- control loop: wants follow demand ------------------------------------------
# Assembly sets each tier's base want (the top tier's target, the floors below it). Once
# the swarm is assembled the controller moves wants between that base and a per-tier max:
#   out: work has waited `up_after` s with nobody free to take it -> +1 on the cheapest
#        tier licensed for it (goals waiting -> the orchestrating tier), when the replica
#        fits on a host with an idle slot right now (scaling out never evicts)
#   in:  a scaled-out tier has had an idle agent and nothing it could take for
#        `down_after` s -> -1 (the idle agent retires; busy ones finish first)
#   one step per tier per `cooldown` s, none while a replica is still starting, and a
#   raise nobody takes within `place_timeout` s is withdrawn
CONTROL_DEFAULTS = {"enabled": True, "interval": 2.0, "up_after": 5.0, "down_after": 60.0,
                    "cooldown": 20.0, "place_timeout": 60.0,
                    # quality escalation (on-demand tiers): auto | on | off
                    "escalate": "auto", "escalate_at": 0.35, "escalate_min_reviews": 8,
                    "escalate_window": 1800.0, "escalate_hold": 1800.0,
                    # canaries: known-answer questions asked when idle (0 = never)
                    "canary_every": 900.0, "canary_fail_at": 0.5, "canary_min": 4}
STARTING = ("embodying", "loading", "assembling")


def control_config(manifest: dict) -> dict:
    """Defaults, overridden by the manifest's `control` block; a tier's max is its `max`,
    else twice its base want."""
    c = {**CONTROL_DEFAULTS, **{k: v for k, v in manifest.get("control", {}).items()
                                if k in CONTROL_DEFAULTS}}
    c["max"] = {}
    for t in manifest.get("tiers", []):
        base = base_want(t["tier"], manifest) or t.get("target", 1)
        c["max"][str(t["tier"])] = max(t.get("max", 2 * base), base)
    return c


def escalated() -> bool:
    return bool((STATE.get("escalation") or {}).get("on"))


def standing_top(manifest: dict | None = None) -> int:
    """The tier meant to lead now: the highest that isn't on-demand, or the on-demand ones
    too while the swarm is escalated."""
    tiers = (manifest or STATE.get("manifest", {})).get("tiers", [])
    esc = escalated() and manifest is None
    return max((t["tier"] for t in tiers if not t.get("on_demand") or esc), default=0)


def base_want(tier: int, manifest: dict | None = None) -> int:
    """What assembly leaves a tier at: the leading tier's target, a floor for those below,
    nothing for a dormant on-demand tier."""
    tiers = (manifest or STATE.get("manifest", {})).get("tiers", [])
    spec = next((t for t in tiers if t["tier"] == tier), {})
    top = standing_top(manifest)
    if tier > top:
        return 0
    nxt = next((t for t in tiers if t["tier"] == tier + 1), {})
    if manifest is None and escalated() and nxt.get("on_demand") \
            and str(tier + 1) not in STATE.get("ready", {}):
        return max(spec.get("serve_floor", 1), spec["provides"]["k"])   # its quorum
    return spec.get("target", 1) if tier == top else spec.get("serve_floor", 1)


def assembled() -> bool:
    tiers = STATE.get("manifest", {}).get("tiers", [])
    top = standing_top()
    return bool(tiers) and all(str(t["tier"]) in STATE["ready"] for t in tiers
                               if 0 < t["tier"] <= top and not t.get("on_demand"))


def lead_tier() -> int:
    """The tier that orchestrates: the highest with a replica up and running (so the one
    below keeps leading while a newly stitched model is still loading)."""
    up = [a["tier"] for a in STATE["agents"].values() if a.get("phase") not in STARTING]
    return max(up, default=0)


def action_for(tier: int) -> str:
    """The most complex directive this tier is licensed for; orchestration only for the
    tier that leads (standing_top), the tiers below it serve."""
    licensed = [d for d in STATE.get("genome", {}).get("directives", []) if d["complexity"] <= tier]
    if tier != standing_top():
        licensed = [d for d in licensed if d["action"] != "orchestrate"]
    return licensed[-1]["action"] if licensed else "serve"


def doubles_up(host: str, tier: int) -> bool:
    """An extra orchestrator on a host that already runs one only splits its CPU: two goals
    at half speed finish later on average than the same two one after the other."""
    return action_for(tier) == "orchestrate" and any(
        a.get("host") == host and a["tier"] == tier for a in STATE["agents"].values())


def placeable(tier: int) -> bool:
    """One more replica would be taken now: it fits the budget and an eligible host with an
    idle slot (one that polled /assign just now) has the memory for it."""
    cost = tier_spec(tier)["cost"]
    if STATE["budget"] - used() < cost:
        return False
    return any(eligible(h, tier) and room(h) >= cost and now() - n.get("polled", 0) < 5
               and not doubles_up(h, tier) for h, n in STATE["nodes"].items())


def set_want(tier: int, to: int, why: str) -> None:
    frm = STATE["wants"].get(str(tier), 0)
    if frm == to:
        return
    STATE["wants"][str(tier)] = to
    STATE.setdefault("ctl_changed", {})[str(tier)] = now()
    record({"type": "scale", "tier": tier, "from": frm, "to": to, "why": why})
    print(f"control: tier{tier} {frm} -> {to} ({why})", flush=True)


def control_step() -> bool:
    """One pass of the controller (call under LOCK). True when a want changed."""
    c = STATE.get("control") or {}
    if not c.get("enabled") or not assembled():
        return False
    reap()
    t = now()
    timers = STATE.setdefault("ctl_timers", {})

    def held(key: str, cond: bool, after: float) -> bool:   # cond true for `after` s
        if not cond:
            timers.pop(key, None)
            return False
        return t - timers.setdefault(key, t) >= after

    counts = live_counts()
    by_tier: dict[int, list] = {}
    for a in STATE["agents"].values():
        by_tier.setdefault(a["tier"], []).append(a)
    todo = [x for x in STATE["tasks"].values() if x["status"] == "todo"
            and x.get("only_tier") is None]   # canary probes are not demand
    waiting_goals = [g for g in STATE["goals"].values() if g["status"] == "todo"
                     or (g["status"] == "active" and g["orchestrator"] is None)]
    tiers = sorted(int(k) for k in STATE["wants"])
    changed = False

    def demand(tier: int) -> tuple[float, int]:
        """(age of the oldest work this tier could take, how much of it there is)"""
        if action_for(tier) == "orchestrate":
            ages = [t - g.get("posted", t) for g in waiting_goals]
        else:
            ages = [t - x.get("posted", t) for x in todo if x["min_tier"] <= tier]
        return max(ages, default=0.0), len(ages)

    info: dict[int, dict] = {}
    for tier in tiers:
        key, want = str(tier), STATE["wants"][str(tier)]
        mine = by_tier.get(tier, [])
        oldest, n = demand(tier)
        info[tier] = {
            "want": want, "base": base_want(tier), "oldest": oldest, "n": n,
            "steady": not any(a.get("phase") in STARTING for a in mine)
                      and t - STATE.get("ctl_changed", {}).get(key, 0) >= c["cooldown"],
            "idle": sum(a.get("phase") == "ready" for a in mine)}
        i = info[tier]
        i["can_grow"] = (i["steady"] and want < c["max"].get(key, i["base"])
                         and i["base"] > 0
                         and t >= STATE.setdefault("ctl_blocked", {}).get(key, 0)
                         and counts.get(tier, 0) >= want and i["idle"] == 0 and placeable(tier))

    # a raise nobody took: withdraw it rather than leave the tier short for ever
    for tier, i in info.items():
        if held(f"short{tier}", i["want"] > i["base"] and counts.get(tier, 0) < i["want"],
                c["place_timeout"]):
            set_want(tier, i["want"] - 1,
                     f"no host took the extra replica in {c['place_timeout']:.0f}s")
            # whatever kept it from being placed may persist: don't flap straight back
            STATE.setdefault("ctl_blocked", {})[str(tier)] = t + 2 * c["place_timeout"]
            i["steady"] = i["can_grow"] = False
            changed = True

    # out: work that has waited picks the cheapest licensed tier that can still grow. One
    # raise per pass (the budget it needs isn't spent until a runner takes it), and
    # subtasks of goals in flight come before starting another orchestrator
    grow: list[int] = []
    for m in sorted({x["min_tier"] for x in todo}):
        if max(t - x.get("posted", t) for x in todo if x["min_tier"] == m) < c["up_after"]:
            continue
        grow += [next((tier for tier in tiers if tier >= m and action_for(tier) == "serve"
                       and info[tier]["can_grow"]), None)]
    grow += [tier for tier in tiers if action_for(tier) == "orchestrate"
             and info[tier]["oldest"] >= c["up_after"] and info[tier]["can_grow"]]
    grow = [tier for tier in grow if tier is not None][:1]

    for tier, i in info.items():
        key = str(tier)
        idle_long = held(f"idle{key}", i["want"] > i["base"] and i["idle"] > 0 and i["n"] == 0,
                         c["down_after"])
        if tier in grow:
            set_want(tier, i["want"] + 1, f"{i['n']} waiting, oldest {i['oldest']:.0f}s, none idle")
            changed = True
        elif idle_long and i["steady"]:
            set_want(tier, i["want"] - 1, f"idle for {c['down_after']:.0f}s")
            timers.pop(f"idle{key}", None)
            changed = True
    return changed


REBALANCE_RATIO = 2.0      # a replica this many times slower than an idle host could be
REBALANCE_EVERY = 900.0    # per tier


def rebalance_step() -> bool:
    """Move a replica toward speed: when a tier is calm (at its want, nothing queued or
    starting) and one of its replicas runs on a host REBALANCE_RATIO times slower than an
    idle eligible host with room, raise the want by one. Speed-aware placement puts the
    new replica on the fast host; the ordinary idle scale-in later retires the slowest."""
    c = STATE.get("control") or {}
    if not c.get("enabled") or not assembled():
        return False
    if any(x["status"] == "todo" for x in STATE["tasks"].values()):
        return False
    sp, counts, t = _speeds(), live_counts(), now()
    last = STATE.setdefault("rebalanced", {})
    for tier in sorted(int(k) for k in STATE["wants"]):
        want, key = STATE["wants"][str(tier)], str(tier)
        mine = [a for a in STATE["agents"].values() if a["tier"] == tier]
        if (not mine or counts.get(tier, 0) != want or want >= c["max"].get(key, want)
                or any(a.get("phase") in STARTING for a in mine)
                or t - last.get(key, 0) < REBALANCE_EVERY
                or t - STATE.get("ctl_changed", {}).get(key, 0) < c["cooldown"]):
            continue
        cost = tier_spec(tier)["cost"]
        if STATE["budget"] - used() < cost:
            continue
        slow = min(mine, key=lambda a: host_speed(a.get("host"), tier, sp))
        slow_tps = host_speed(slow.get("host"), tier, sp)
        fast = [h for h, n in STATE["nodes"].items()
                if h != slow.get("host") and eligible(h, tier) and room(h) >= cost
                and t - n.get("polled", 0) < 5]
        if not slow_tps or not fast:
            continue
        best = max(fast, key=lambda h: host_speed(h, tier, sp))
        best_tps = host_speed(best, tier, sp)
        if best_tps >= REBALANCE_RATIO * slow_tps:
            last[key] = t
            set_want(tier, want + 1, f"rebalance: {slow.get('host')} runs it at "
                     f"{slow_tps:.0f} tok/s, {best} would at ~{best_tps:.0f}")
            return True
    return False


RELOCATE_TTL = 180.0   # how long a host stays reserved for the lead tier


def relocation() -> dict | None:
    r = STATE.get("relocation")
    if r and now() > r["until"]:
        STATE.pop("relocation", None)
        record({"type": "relocated", "tier": r["tier"], "host": r["host"], "ok": False})
        return None
    return r


def relocate_step() -> bool:
    """The leading tier on the fastest host it can have: if a host REBALANCE_RATIO times
    faster could fit a lead replica by shedding idle lower-tier agents that have somewhere
    else to go, reserve that host for the lead tier, retire those agents (their tiers refill
    elsewhere), and add one lead replica; it lands on the reserved host and the slow one
    retires once idle (scale-in ranks by speed). Once per REBALANCE_EVERY."""
    c = STATE.get("control") or {}
    if not c.get("enabled") or not assembled() or relocation():
        return False
    if any(x["status"] == "todo" for x in STATE["tasks"].values()):
        return False
    lead, t, sp = standing_top(), now(), _speeds()
    key = str(lead)
    want = STATE["wants"].get(key, 0)
    mine = [a for a in STATE["agents"].values() if a["tier"] == lead]
    last = STATE.setdefault("rebalanced", {})
    if (not mine or len(mine) != want or any(a.get("phase") in STARTING for a in mine)
            or t - last.get("lead", 0) < REBALANCE_EVERY):
        return False
    cost = tier_spec(lead)["cost"]
    if STATE["budget"] - used() < cost:
        return False
    slow = min(mine, key=lambda a: host_speed(a.get("host"), lead, sp))
    slow_tps = host_speed(slow.get("host"), lead, sp)
    if not slow_tps:
        return False
    holders = {a.get("host") for a in mine}
    cands = sorted((h for h in STATE["nodes"] if h not in holders and eligible(h, lead)
                    and host_speed(h, lead, sp) >= REBALANCE_RATIO * slow_tps
                    and (STATE["nodes"][h].get("capacity_mb") or 0) >= cost),
                   key=lambda h: -host_speed(h, lead, sp))
    for h in cands:
        need = cost - room(h)
        lower = sorted((a for a in STATE["agents"].values() if a.get("host") == h
                        and a["tier"] < lead), key=lambda a: (a["tier"], a["cost"]))
        if any(a.get("phase") != "ready" for a in lower):
            continue                      # don't cut anyone off mid-task
        evict, freed = [], 0
        for a in lower:
            if freed >= need:
                break
            evict.append(a)
            freed += a["cost"]
        if freed < need:
            continue
        # every evicted agent must fit somewhere else (first fit, other hosts' free room)
        spare = {o: room(o) for o in STATE["nodes"] if o != h}
        placed = True
        for a in evict:
            o = next((o for o in spare if eligible(o, a["tier"]) and spare[o] >= a["cost"]), None)
            if o is None:
                placed = False
                break
            spare[o] -= a["cost"]
        if not placed:
            continue
        aid_of = {id(v): k for k, v in STATE["agents"].items()}
        STATE["relocation"] = {"tier": lead, "host": h, "from": slow.get("host"),
                               "evict": [aid_of[id(a)] for a in evict], "until": t + RELOCATE_TTL}
        last["lead"] = t
        set_want(lead, want + 1, f"relocate the lead: {slow.get('host')} runs tier{lead} at "
                 f"{slow_tps:.0f} tok/s, {h} would at ~{host_speed(h, lead, sp):.0f} "
                 f"(shedding {len(evict)} lower agent(s) there)")
        return True
    return False


def relocation_done() -> None:
    """Finish a relocation once a lead replica is up on the reserved host: the want goes
    back down and scale-in retires the slowest replica when it is free."""
    r = relocation()
    if not r:
        return
    up = [a for a in STATE["agents"].values() if a["tier"] == r["tier"]
          and a.get("host") == r["host"] and a.get("phase") not in STARTING]
    if up:
        STATE.pop("relocation", None)
        set_want(r["tier"], max(STATE["wants"][str(r["tier"])] - 1, base_want(r["tier"])),
                 f"relocated the lead to {r['host']}")
        record({"type": "relocated", "tier": r["tier"], "host": r["host"], "ok": True})


def review_disagreement(window: float) -> tuple[float | None, int]:
    t = now()
    rs = [e["agrees"] for e in STATE.get("events", []) if e["type"] == "task"
          and e.get("kind") == "review" and e.get("agrees") is not None and t - e["t"] < window]
    rs = rs[-20:]
    return (round(1 - sum(rs) / len(rs), 2) if rs else None), len(rs)


def escalation_step() -> bool:
    """Bring in the next on-demand tier while review disagreement stays high (the small
    models keep contradicting each other: a bigger model should plan and synthesise), or
    while an admin says so; relax `escalate_hold` s after the last reason to stay."""
    c = STATE.get("control") or {}
    od = sorted(t["tier"] for t in STATE.get("manifest", {}).get("tiers", []) if t.get("on_demand"))
    if not od or not c:
        return False
    tier, t = od[0], now()
    esc = STATE.setdefault("escalation", {"on": False, "since": 0.0, "hot": 0.0, "why": ""})
    rate, n = review_disagreement(c["escalate_window"])
    # canaries span hours (one per canary_every when idle), so they get a longer window
    crate, cn = canary_failure(max(c["escalate_window"], 8 * c.get("canary_every", 900)),
                               lead_tier())   # judged on the setup that is leading now
    mode = c.get("escalate", "auto")
    by_reviews = n >= c["escalate_min_reviews"] and rate >= c["escalate_at"]
    by_canaries = cn >= c.get("canary_min", 4) and crate >= c.get("canary_fail_at", 0.5)
    hot = mode == "auto" and (by_reviews or by_canaries)
    if hot or mode == "on":
        esc["hot"] = t
    elif mode == "off":
        esc["hot"] = 0.0   # switching back to auto later starts from the signal, not the past
    # starting needs a reason now; the hold only keeps an escalation that is already on
    start = mode == "on" or hot
    stay = mode == "on" or (mode == "auto" and t - esc.get("hot", 0) < c["escalate_hold"])
    if start and not esc["on"]:
        why = ("forced by admin" if mode == "on"
               else f"review disagreement {rate:.0%} over the last {n} reviews" if by_reviews
               else f"{crate:.0%} of the last {cn} canaries answered wrong")
        esc.update(on=True, since=t, why=why)
        below = tier_spec(tier - 1)
        if str(tier) in STATE["ready"]:            # stitched before: just embody it again
            set_want(tier, tier_spec(tier)["target"], f"escalating: {why}")
        else:                                      # enough carriers below for a quorum
            k = below["provides"]["k"]
            set_want(tier - 1, max(STATE["wants"].get(str(tier - 1), 0), k),
                     f"escalating to tier{tier} (quorum of {k} carriers): {why}")
        record({"type": "escalate", "tier": tier, "why": why})
        return True
    if not stay and esc["on"]:
        why = "admin switched it off" if mode == "off" else \
            f"no reason to stay for {c['escalate_hold']:.0f}s"
        esc.update(on=False, since=t, why=why)
        set_want(tier, 0, f"relaxing: {why}")
        set_want(tier - 1, base_want(tier - 1), "relaxing: back to leading")
        record({"type": "relax", "tier": tier, "why": why})
        return True
    return False


CANARIES: list[dict] = []


def load_canaries() -> None:
    """canaries.json next to this file: [{id, text, expect (regex)}]."""
    try:
        data = json.loads(Path(__file__).with_name("canaries.json").read_text())
        CANARIES[:] = [c for c in data.get("canaries", [])
                       if c.get("id") and c.get("text") and re.compile(c["expect"], re.I)]
    except (OSError, ValueError, re.error) as e:
        print(f"canaries not loaded: {e}", flush=True)


def canary_step() -> bool:
    """Ask a known-answer question now and then, only when nothing real is waiting or
    running, rotating through the set; the answer is checked in h_goal_done."""
    c = STATE.get("control") or {}
    every = c.get("canary_every", 0)
    if not every or not CANARIES or not assembled():
        return False
    if any(g["status"] != "done" for g in STATE["goals"].values()):
        return False
    if any(x["status"] in ("todo", "doing") for x in STATE["tasks"].values()):
        return False
    st = STATE.setdefault("canary", {"last": 0.0, "next": 0})
    if now() - st["last"] < every:
        return False
    k = CANARIES[st["next"] % len(CANARIES)]
    st.update(last=now(), next=st["next"] + 1)
    gid = add_goal(k["text"])
    STATE["goals"][gid]["canary"] = k["id"]
    # the same question straight to each serving tier: which model falls for the trap
    for tier in sorted(int(x) for x in STATE["wants"]):
        if STATE["wants"][str(tier)] and action_for(tier) == "serve":
            tid = next_id()
            STATE["tasks"][tid] = {"id": tid, "goal": None, "prompt": k["text"], "q": None,
                                   "kind": "answer", "of": None, "min_tier": tier,
                                   "only_tier": tier, "canary": k["id"], "status": "todo",
                                   "worker": None, "result": None, "posted": now(), "avoid": []}
    return True


def check_canary(g: dict) -> None:
    k = next((x for x in CANARIES if x["id"] == g.get("canary")), None)
    if not k:
        return
    ok = bool(re.search(k["expect"], g.get("answer") or "", re.I))
    record({"type": "canary", "id": k["id"], "ok": ok, "lead": lead_tier(),
            "latency": round(g["done"] - g["posted"], 1)})
    print(f"canary {k['id']}: {'pass' if ok else 'FAIL'} (lead tier{lead_tier()})", flush=True)


def canary_failure(window: float, lead: int | None = None) -> tuple[float | None, int]:
    """Share of recent whole-swarm canaries answered wrong (by `lead` as the leading tier,
    if given); per-tier probes don't count here."""
    t = now()
    rs = [e["ok"] for e in STATE.get("events", []) if e["type"] == "canary"
          and not e.get("probe") and t - e["t"] < window
          and (lead is None or e.get("lead") == lead)][-8:]
    return (round(1 - sum(rs) / len(rs), 2) if rs else None), len(rs)


def control_loop() -> None:
    while True:
        time.sleep((STATE.get("control") or CONTROL_DEFAULTS)["interval"])
        with LOCK:
            if ROLE == "primary" and STATE.get("manifest"):
                try:   # one bad pass must not end the loop (a daemon thread dies silently)
                    reap()
                    settle_lapses()
                    escalation_step()
                    canary_step()
                    relocation_done()
                    control_step() or relocate_step() or rebalance_step()
                except Exception as e:   # noqa: BLE001
                    print(f"control loop: {type(e).__name__}: {e}", flush=True)
                DIRTY.set()


def h_control(body: dict) -> dict:
    """Admin: switch the controller on or off, or change its settings, a tier's max, or the
    swarm's budget (which otherwise only a reseed sets)."""
    c = STATE.setdefault("control", control_config(STATE.get("manifest", {})))
    for k, v in body.items():
        if k == "enabled":
            c["enabled"] = bool(v)
        elif k == "escalate" and v in ("auto", "on", "off"):
            c["escalate"] = v
        elif k in CONTROL_DEFAULTS and isinstance(v, (int, float)) and v >= 0:
            c[k] = float(v)
        elif k == "budget" and isinstance(v, int) and v > 0:
            STATE["budget"] = v   # the swarm-wide ceiling; hosts still cap what they offer
        elif k == "max" and isinstance(v, dict):
            for tier, m in v.items():
                if str(tier) in c["max"] and isinstance(m, int):
                    c["max"][str(tier)] = max(m, base_want(int(tier)))
        elif not k.startswith("_"):
            return {"ok": False, "error": f"unknown setting {k!r}"}
    if not c["enabled"]:   # off: back to the base the assembly left
        for tier in sorted(int(k) for k in STATE["wants"]):
            if assembled() and STATE["wants"][str(tier)] != base_want(tier) \
                    and not tier_spec(tier).get("on_demand"):
                set_want(tier, base_want(tier), "controller switched off")
    return {"ok": True, "control": c}


def control_view() -> dict:
    return {"control": STATE.get("control"), "assembled": assembled(),
            "escalation": STATE.get("escalation"), "lead": lead_tier(),
            "review_disagreement": review_disagreement(
                {**CONTROL_DEFAULTS, **(STATE.get("control") or {})}["escalate_window"]),
            "canaries": {"failure": canary_failure(86400.0),
                         "recent": [e for e in STATE.get("events", [])
                                    if e["type"] == "canary"][-8:]},
            "wants": STATE.get("wants", {}),
            "base": {str(t["tier"]): base_want(t["tier"])
                     for t in STATE.get("manifest", {}).get("tiers", [])},
            "speed": {h: round(v, 2) for h in sorted(STATE.get("nodes", {}))
                      if (v := speed_score(h)) is not None},
            "decisions": [e for e in STATE.get("events", [])
                          if e["type"] in ("scale", "escalate", "relax")][-20:]}


# --- immune system (innate) ----------------------------------------------------------
# Incidents raise a host's health score, which halves every `half_life` s. At
# `quarantine_at` the host is quarantined: no new agents, no task claims, no elections, no
# serving blobs to peers, and its agents retire (unless one is the last refuge of its
# tier). Below `release_at` it is released by itself. Outputs are screened before they
# count; a task that keeps failing is given up after `max_attempts` (poison). While
# incidents spike (`inflame_at` in `inflame_window` s) the swarm is inflamed: quarantine
# comes sooner and orchestrators have more answers cross-checked.
IMMUNE_DEFAULTS = {"half_life": 600.0, "quarantine_at": 3.0, "release_at": 1.0,
                   "max_attempts": 3, "inflame_at": 3, "inflame_window": 300.0,
                   "avoid_for": 30.0}
WEIGHTS = {"lapse": 1.0, "embody_failed": 1.0, "bad_output": 1.0, "task_error": 0.5,
           "bad_blob": 1.5}
LAPSE_SETTLE = 10.0     # lapses are charged only once it's clear they weren't everyone's
EXCUSED_UNTIL = [0.0]   # no lapse is charged right after this coordinator starts or promotes


def immune_cfg() -> dict:
    return {**IMMUNE_DEFAULTS, **(STATE.get("immune") or {})}


def health(host: str) -> dict:
    """A host's ledger entry, decayed to now (and released, once it has decayed enough)."""
    c, t = immune_cfg(), now()
    h = STATE.setdefault("health", {}).setdefault(
        host, {"score": 0.0, "t": t, "quarantined": False, "counts": {}})
    h["score"] *= 0.5 ** (max(t - h["t"], 0.0) / c["half_life"])
    h["t"] = t
    if h["quarantined"] and h["score"] < c["release_at"]:
        h["quarantined"] = False
        record({"type": "release", "host": host})
        print(f"immune: {host} released (score {h['score']:.2f})", flush=True)
    return h


def quarantined(host: str | None) -> bool:
    return bool(host) and host in STATE.get("health", {}) and health(host)["quarantined"]


def inflamed() -> bool:
    """Incidents in the window, not counting those since pardoned or refunded."""
    c, t = immune_cfg(), now()
    evs = [e for e in STATE.get("events", []) if t - e["t"] < c["inflame_window"]]
    pardoned = {e["host"]: e["t"] for e in evs if e["type"] == "pardon"}
    refunded = {e["task"] for e in evs if e["type"] == "refund"}
    n = sum(1 for e in evs if e["type"] == "incident" and not e.get("repeat")
            and not e.get("excused")
            and e["t"] > pardoned.get(e["host"], 0) and e.get("task") not in refunded)
    return n >= c["inflame_at"]


def incident(host: str | None, kind: str, detail: str = "", task: dict | None = None) -> None:
    if not host:
        return
    ev = {"type": "incident", "host": host, "kind": kind, "detail": detail[:160]}
    if task is not None:   # one charge per host per task, remembered in case it's the culprit
        ev["task"] = task["id"]
        if any(c[0] == host for c in task.get("charged", ())):
            record({**ev, "repeat": True})
            return
        task.setdefault("charged", []).append([host, WEIGHTS[kind]])
    h = health(host)
    h["score"] += WEIGHTS[kind]
    h["counts"][kind] = h["counts"].get(kind, 0) + 1
    record(ev)
    at = immune_cfg()["quarantine_at"] * (2 / 3 if inflamed() else 1)
    print(f"immune: {host} {kind} ({detail[:80]}) -> score {h['score']:.2f}", flush=True)
    if not h["quarantined"] and h["score"] >= at:
        h["quarantined"] = True
        record({"type": "quarantine", "host": host, "score": round(h["score"], 2)})
        print(f"immune: {host} QUARANTINED at score {h['score']:.2f}", flush=True)


def note_lapse(hosts: set) -> None:
    """Hosts whose agents' heartbeats lapsed. Charged later (settle_lapses): if half the
    swarm lapsed together, the fault is more likely this coordinator's network than theirs."""
    pend = STATE.setdefault("lapse_pending", [])
    recent = {e["host"] for e in STATE.get("events", [])[-100:] if e["type"] == "incident"
              and e["kind"] == "lapse" and now() - e["t"] < 3 * LAPSE_SETTLE}
    for h in hosts:
        if not h or now() < EXCUSED_UNTIL[0] or h in recent:
            continue
        # one lapse per host per incident: a host's agents die together but are reaped over
        # a second or two (their last heartbeats differ), which must not count as several
        if any(p["host"] == h and now() - p["t"] < 3 * LAPSE_SETTLE for p in pend):
            continue
        pend.append({"host": h, "t": now()})


def settle_lapses() -> None:
    pend = STATE.get("lapse_pending") or []
    t = now()
    ripe = [p for p in pend if t - p["t"] >= LAPSE_SETTLE]
    if not ripe:
        return
    active = {h for h, n in STATE["nodes"].items() if t - n.get("seen", 0) < 60}
    done: set[int] = set()
    for p in ripe:
        if id(p) in done:
            continue
        group = [q for q in pend if abs(q["t"] - p["t"]) <= LAPSE_SETTLE]
        together = {q["host"] for q in group}
        if len(together) >= max(2, -(-len(active) // 2)):
            record({"type": "mass_lapse", "hosts": sorted(together)})
            done |= {id(q) for q in group}   # one verdict for the whole group
        else:
            incident(p["host"], "lapse", "agents stopped heartbeating without leaving")
            done.add(id(p))
    STATE["lapse_pending"] = [p for p in pend if id(p) not in done]


def retry_task(t: dict, host: str | None, why: str) -> None:
    """Back to the queue, avoiding the host it failed on; given up after max_attempts."""
    t["attempts"] = t.get("attempts", 0) + 1
    if host and host not in t.setdefault("avoid", []):
        t["avoid"].append(host)
    if host:   # it failed here: not here again for avoid_for, alternatives or not
        t.setdefault("banned", {})[host] = now()
    t["worker"] = None
    if t["attempts"] >= immune_cfg()["max_attempts"]:
        t.update(status="failed", result=None, error=why, done=now())
        record({"type": "poison", "task": t["id"], "goal": t["goal"], "why": why[:160]})
        if t.get("goal") is None:   # a probe: nothing waits on it
            STATE["tasks"].pop(t["id"], None)
        if len({c[0] for c in t.get("charged", ())}) >= 2:   # it failed everywhere it went
            remember_poison(t["prompt"], why)
        print(f"immune: task {t['id']} given up after {t['attempts']} attempts ({why})", flush=True)
        refund(t)
    else:
        t.update(status="todo", requeued=now())


def refund(t: dict) -> None:
    """A task that failed on several hosts is the problem, not the hosts: take back what
    it cost them. (One host failing it every time is still that host's doing.)"""
    charged = t.get("charged") or []
    hosts = {h for h, _ in charged}
    if len(hosts) < 2:
        return
    for h, w in charged:
        e = health(h)
        e["score"] = max(0.0, e["score"] - w)
    record({"type": "refund", "task": t["id"], "hosts": sorted(hosts)})   # also for goals
    print(f"immune: task {t['id']} failed on {len(hosts)} hosts: charges refunded", flush=True)


def retry_goal(g: dict, host: str | None, why: str) -> None:
    """Claimable again (its posted tasks stand), preferably by another host; after
    max_attempts it is closed without an answer."""
    g["attempts"] = g.get("attempts", 0) + 1
    if host and host not in g.setdefault("avoid", []):
        g["avoid"].append(host)
    g["orchestrator"], g["requeued"] = None, now()
    if g["attempts"] >= immune_cfg()["max_attempts"]:
        g.update(status="done", done=now(),
                 answer=f"(no answer: given up after {g['attempts']} attempts; last: {why})")
        record({"type": "poison", "goal": g["id"], "why": why[:160]})
        if why.startswith("error:"):   # the model choked on it (not: its hosts went away)
            remember_poison(g["text"], why)
        record({"type": "goal", "latency": round(g["done"] - g["posted"], 1),
                "tasks": len(g["tasks"]), "failed": True})
        print(f"immune: goal {g['id']} given up after {g['attempts']} attempts ({why})", flush=True)
        refund(g)


ANTIBODIES_KEPT = 500     # remembered poison inputs (oldest forgotten first)
ANTIBODY_TTL = 7 * 86400  # and none for longer than a week: models and prompts change


def fingerprint(text: str) -> str:
    return hashlib.sha256(" ".join(str(text).lower().split()).encode()).hexdigest()


def remember_poison(text: str, why: str) -> None:
    """Adaptive memory: an input given up as poison is refused on sight next time."""
    ab = STATE.setdefault("antibodies", {})
    fp = fingerprint(text)
    e = ab.setdefault(fp, {"first": now(), "seen": 0, "why": why[:120], "text": str(text)[:80]})
    e["last"] = now()
    for old in sorted(ab, key=lambda k: ab[k].get("last", 0))[:-ANTIBODIES_KEPT]:
        del ab[old]


def antibody(text: str) -> dict | None:
    e = (STATE.get("antibodies") or {}).get(fingerprint(text))
    if e and now() - e.get("last", 0) > ANTIBODY_TTL:
        del STATE["antibodies"][fingerprint(text)]
        return None
    if e:
        e["seen"] += 1
    return e


def screen(result, kind: str) -> str | None:
    """Why an output is unusable, or None. Cheap, model-agnostic checks only."""
    if not isinstance(result, str) or not result.strip():
        return "empty"
    text = result.strip()
    if kind == "review":
        try:
            v = json.loads(text)
        except ValueError:
            return "review is not JSON"
        if not (isinstance(v, dict) and isinstance(v.get("agrees"), bool)
                and isinstance(v.get("own_answer"), str) and v["own_answer"].strip()):
            return "review lacks own_answer/agrees"
        text = v["own_answer"].strip()
    if len(text) >= 200 and len(zlib.compress(text.encode())) < 0.12 * len(text.encode()):
        return "degenerate repetition"
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    if len(lines) >= 6 and len(set(lines)) <= len(lines) // 3:
        return "repeated lines"
    return None


def _speeds() -> dict:
    """Median tok/s per (host, tier) and per tier, from recent work."""
    by: dict = {}
    for e in STATE.get("events", [])[-300:]:
        if e["type"] == "task" and e.get("tps") and e.get("host") and e.get("tier") is not None:
            by.setdefault((e["host"], e["tier"]), []).append(e["tps"])
            by.setdefault(("*", e["tier"]), []).append(e["tps"])
    return {k: statistics.median(v) for k, v in by.items()}


def speed_score(host: str, sp: dict | None = None) -> float | None:
    """How fast this host runs models relative to the swarm (1.0 = median), over every tier
    it has served; None if it hasn't served anything lately."""
    sp = _speeds() if sp is None else sp
    ratios = [v / sp[("*", t)] for (h, t), v in sp.items() if h == host and sp.get(("*", t))]
    return statistics.median(ratios) if ratios else None


def host_speed(host: str, tier: int, sp: dict | None = None) -> float:
    """Expected tok/s of `tier` on `host`: measured, else the tier's median scaled by the
    host's score elsewhere, else the tier's median."""
    sp = _speeds() if sp is None else sp
    if (host, tier) in sp:
        return sp[(host, tier)]
    base = sp.get(("*", tier), 0.0)
    return base * (speed_score(host, sp) or 1.0)


def first_to_go(a: dict) -> bool:
    """Scale-in picks who retires: quarantined, then pressured, then least healthy, then
    slowest hosts go first. The worst-ranked replica goes even if it is busy right now (it
    asks when it's done; the others wait for it), unless it sits idle without asking for
    30 s, when whoever asks may go."""
    tier = a["tier"]
    mine = [x for x in STATE["agents"].values() if x["tier"] == tier]
    up = [x for x in mine if x.get("phase") not in STARTING]
    if up == [a] and len(mine) > 1:
        return False   # the tier's only running replica waits for a starting one to be up
    since = STATE.setdefault("excess_since", {}).setdefault(str(tier), now())
    sp = _speeds()

    def rank(x: dict) -> tuple:
        h = x.get("host")
        return (quarantined(h), STATE["nodes"].get(h, {}).get("pressure", "ok") != "ok",
                round(health(h)["score"], 1) if h else 0.0, -host_speed(h, tier, sp))
    worst = max(up or mine, key=rank)
    if rank(a) >= rank(worst):
        return True
    return worst.get("phase") == "ready" and now() - since > 30


def benched(agent_id: str) -> bool:
    """Quarantined, unless it is the last refuge of its tier (better a suspect orchestrator
    than none: its outputs are still screened)."""
    a = STATE["agents"].get(agent_id)
    return bool(a) and quarantined(a.get("host")) and not last_refuge(a)


def last_refuge(a: dict) -> bool:
    """The only replica of its tier, with no healthy host able to take over."""
    tier, cost = a["tier"], a["cost"]
    if live_counts().get(tier, 0) > 1:
        return False
    return not any(h != a.get("host") and eligible(h, tier) and room(h) >= cost
                   for h in STATE["nodes"])


def h_report(body: dict) -> dict:
    """A host says a peer served it bytes that failed verification. One charge per
    reporter and peer per 10 minutes, so no single host can sink another alone."""
    reporter = body.get("_auth") or body.get("host")
    if body.get("kind") != "bad_blob":
        return {"ok": False, "error": "unknown report"}
    url = str(body.get("peer", ""))
    subject = next((h for h, n in STATE["nodes"].items()
                    if any(url.startswith(p) for p in n.get("peer") or ())), None)
    if not subject or subject == reporter:
        return {"ok": False, "error": "no such peer"}
    seen = STATE.setdefault("reports", {})
    key = f"{reporter}>{subject}"
    if now() - seen.get(key, 0) < 600:
        return {"ok": True, "counted": False}
    seen[key] = now()
    incident(subject, "bad_blob", f"reported by {reporter}: blob {str(body.get('sha'))[:12]}")
    return {"ok": True, "counted": True}


def h_immune(body: dict) -> dict:
    """Admin: pardon a host (score and quarantine cleared) or change a setting."""
    c = STATE.setdefault("immune", dict(IMMUNE_DEFAULTS))
    for k, v in body.items():
        if k == "forget":   # drop remembered poison inputs: "all" or an id prefix
            ab = STATE.get("antibodies") or {}
            for fp in [fp for fp in ab if v == "all" or fp.startswith(str(v))]:
                del ab[fp]
        elif k == "pardon":
            if v not in STATE.get("health", {}):
                return {"ok": False, "error": f"no ledger for {v!r}"}
            STATE["health"][v].update(score=0.0, quarantined=False, t=now())
            record({"type": "pardon", "host": v})
        elif k in IMMUNE_DEFAULTS and isinstance(v, (int, float)) and v >= 0:
            c[k] = v
        elif not k.startswith("_"):
            return {"ok": False, "error": f"unknown setting {k!r}"}
    return {"ok": True, **immune_view()}


def immune_view() -> dict:
    evs = STATE.get("events", [])
    return {"config": immune_cfg(), "inflamed": inflamed(),
            "hosts": {h: {"score": round(health(h)["score"], 2),
                          "quarantined": health(h)["quarantined"],
                          "counts": STATE["health"][h]["counts"]}
                      for h in sorted(STATE.get("health", {}))},
            "incidents": [e for e in evs if e["type"] in ("incident", "quarantine", "release",
                                                          "pardon", "mass_lapse", "refund")][-20:],
            "poison": [e for e in evs if e["type"] == "poison"][-10:],
            "antibodies": [{**v, "id": k[:12]} for k, v in
                           sorted((STATE.get("antibodies") or {}).items(),
                                  key=lambda kv: -kv[1].get("last", 0))][:20]}


def h_goal_post(body: dict) -> dict:
    """Anyone on the LAN/tailnet may ask (by design); a question is plain text, bounded."""
    text = body.get("text")
    if not isinstance(text, str) or not text.strip() or len(text) > MAX_QUESTION:
        return {"ok": False, "error": f"text must be 1..{MAX_QUESTION} characters"}
    return {"ok": True, "goal_id": add_goal(text.strip())}


def h_goal_claim(body: dict) -> dict:
    """An orchestrator resumes its own unfinished goal (after an error), else takes the
    oldest goal nobody alive is working on."""
    reap()
    me = body["agent_id"]
    host = STATE["agents"].get(me, {}).get("host")
    if benched(me):
        return {"claimed": False, "quarantined": True}
    for g in sorted(STATE["goals"].values(), key=lambda g: int(g["id"])):
        if g["status"] == "active" and g["orchestrator"] == me:
            return {"claimed": True, "goal": g, "inflamed": inflamed()}
    for g in sorted(STATE["goals"].values(), key=lambda g: int(g["id"])):
        if g["status"] == "todo" or (g["status"] == "active" and g["orchestrator"] is None):
            if host in g.get("avoid", ()) and \
                    now() - g.get("requeued", 0) < immune_cfg()["avoid_for"]:
                continue
            g["status"], g["orchestrator"] = "active", body["agent_id"]
            g["orchestrator_host"] = host
            g.setdefault("claimed", now())
            return {"claimed": True, "goal": g, "inflamed": inflamed()}
    return {"claimed": False}


def h_goal_done(body: dict) -> dict:
    g = STATE["goals"].get(str(body["goal_id"]))
    if g and body.get("_auth") and g["orchestrator"] != body.get("agent_id"):
        return {"ok": False, "error": "not this goal's orchestrator"}
    if g and g["status"] != "done":
        why = screen(body["answer"], "answer")
        answer = body["answer"]
        if why:
            incident(STATE["agents"].get(body.get("agent_id", ""), {}).get("host"),
                     "bad_output", f"goal {g['id']} synthesis: {why}")
            g["synth_rejects"] = g.get("synth_rejects", 0) + 1
            if g["synth_rejects"] < immune_cfg()["max_attempts"]:
                return {"ok": False, "rejected": why}
            answer = f"(no answer: the synthesis failed screening {g['synth_rejects']} times: {why})"
        g.update(status="done", answer=answer, done=now(),
                 by=STATE["agents"].get(body.get("agent_id", ""), {}).get("node"))
        if g.get("canary"):
            check_canary(g)
        else:
            record({"type": "goal", "latency": round(g["done"] - g["posted"], 1),
                    "tasks": len(g["tasks"])})
        a = STATE["agents"].get(body.get("agent_id", ""), {})
        if body.get("tps") and a:   # the leading tier's speed, for placement
            record({"type": "task", "kind": "synth", "tier": a.get("tier"), "host": a.get("host"),
                    "wait": None, "run": None, "tps": body["tps"], "agrees": None})
        archive_done_goals()
    return {"ok": True}


def h_task_post(body: dict) -> dict:
    """The orchestrator's plan: subtasks for subagents, each with the lowest tier
    licensed to take it."""
    g = STATE["goals"].get(str(body["goal_id"]))
    if g and body.get("_auth") and g["orchestrator"] != body.get("agent_id"):
        return {"ok": False, "error": "not this goal's orchestrator"}
    if not g or (g["tasks"] and not body.get("append")):
        return {"ok": True, "already": True}
    for t in body["tasks"]:
        tid = next_id()
        STATE["tasks"][tid] = {"id": tid, "goal": g["id"], "prompt": t["prompt"],
                               "q": str(t.get("q") or "")[:500] or None,
                               "kind": t.get("kind", "answer"), "of": t.get("of"),
                               "min_tier": int(t.get("min_tier", 0)), "status": "todo",
                               "worker": None, "result": None, "posted": now(),
                               "avoid": [str(h) for h in t.get("avoid") or ()][:8]}
        g["tasks"].append(tid)
        ab = antibody(t["prompt"])
        if ab:
            STATE["tasks"][tid].update(status="failed", done=now(),
                                       error=f"known poison input; {ab['why']}")
            record({"type": "antibody", "task": tid})
    return {"ok": True}


def h_task_claim(body: dict) -> dict:
    """Hardest task this tier is licensed for first, so small models get the easy ones."""
    tier, me = int(body.get("tier", 0)), body["agent_id"]
    mine = [t for t in STATE["tasks"].values() if t["status"] == "doing" and t["worker"] == me]
    if mine:   # this agent errored on it last time: retry rather than strand it
        return {"claimed": True, "task": mine[0]}
    a = STATE["agents"].get(body["agent_id"], {})
    host = a.get("host")
    if benched(body["agent_id"]):
        return {"claimed": False, "quarantined": True}
    # a host a task failed on (or, for a review, the host that wrote the answer) is
    # passed over while someone else might take it: an agent of a licensed tier elsewhere
    avoid_for = immune_cfg()["avoid_for"]

    def elsewhere(t: dict) -> bool:
        return any(x.get("host") not in t.get("avoid", ()) and x["tier"] >= t["min_tier"]
                   and action_for(x["tier"]) == "serve" for x in STATE["agents"].values())
    todo = [t for t in STATE["tasks"].values() if t["status"] == "todo" and t["min_tier"] <= tier
            and t.get("only_tier", tier) == tier
            and now() - (t.get("banned") or {}).get(host, 0) > avoid_for
            and (host not in t.get("avoid", ())
                 or now() - t.get("requeued", t["posted"]) > avoid_for or not elsewhere(t))]
    if not todo:
        return {"claimed": False}
    t = max(todo, key=lambda t: (t["min_tier"], -int(t["id"])))
    t.update(status="doing", worker=body["agent_id"], worker_node=a.get("node"),
             worker_tier=a.get("tier"), worker_host=a.get("host"), claimed=now())
    return {"claimed": True, "task": t}


def h_task_done(body: dict) -> dict:
    t = STATE["tasks"].get(str(body["id"]))
    if t and t["status"] == "doing" and t["worker"] == body.get("agent_id"):
        why = screen(body["result"], t.get("kind", "answer"))
        if why:
            incident(t.get("worker_host"), "bad_output", f"task {t['id']}: {why}", t)
            retry_task(t, t.get("worker_host"), f"output rejected: {why}")
            return {"ok": False, "rejected": why}
        t.update(status="done", result=body["result"], seconds=body.get("seconds"),
                 tps=body.get("tps"), done=now())
        claimed, posted = t.get("claimed") or t["done"], t.get("posted") or t["done"]
        record({"type": "task", "kind": t.get("kind", "answer"), "tier": t.get("worker_tier"),
                "host": t.get("worker_host"), "wait": round(claimed - posted, 2),
                "run": round(t["done"] - claimed, 2), "tps": t.get("tps"),
                "agrees": review_verdict(t["result"]) if t.get("kind") == "review" else None})
        if t.get("canary") and t.get("goal") is None:   # a per-tier probe: check and drop
            k = next((x for x in CANARIES if x["id"] == t["canary"]), None)
            if k:
                ok = bool(re.search(k["expect"], t["result"] or "", re.I))
                record({"type": "canary", "id": k["id"], "ok": ok, "probe": True,
                        "tier": t.get("worker_tier"), "host": t.get("worker_host")})
            STATE["tasks"].pop(t["id"], None)
    return {"ok": True}


def h_goal_fail(body: dict) -> dict:
    """The orchestrator's model could not plan or synthesise this goal."""
    g = STATE["goals"].get(str(body["goal_id"]))
    if g and g["status"] == "active" and g["orchestrator"] == body.get("agent_id"):
        err = str(body.get("error", ""))[:120]
        host = g.get("orchestrator_host")
        incident(host, "task_error", f"goal {g['id']}: {err}", g)
        retry_goal(g, host, f"error: {err}")
    return {"ok": True}


def h_task_fail(body: dict) -> dict:
    """The worker could not produce an answer (its model errored or timed out)."""
    t = STATE["tasks"].get(str(body["id"]))
    if t and t["status"] == "doing" and t["worker"] == body.get("agent_id"):
        err = str(body.get("error", ""))[:120]
        incident(t.get("worker_host"), "task_error", f"task {t['id']}: {err}", t)
        retry_task(t, t.get("worker_host"), f"error: {err}")
    return {"ok": True}


def h_promote(body: dict) -> dict:
    """Make this coordinator the primary, one epoch past anything either side has seen (it
    asks the peer, if it answers). A primary that finds a peer ahead of it steps down."""
    global ROLE
    peer_epoch = 0
    if PEER:
        try:
            peer_epoch = int(peer_get("/role").get("epoch", 0))
        except (OSError, ValueError):
            pass
    STATE["epoch"] = max(int(STATE.get("epoch", 1)), peer_epoch) + 1
    EXCUSED_UNTIL[0] = now() + 30   # heartbeats are only now finding their way here
    STATE["promoted_at"] = now()
    for a in STATE["agents"].values():   # their heartbeats went to the old primary: fresh
        a["last_hb"] = now()             # lease, so only the truly gone get reaped
    ROLE = "primary"
    save_role()
    print(f"promoted to primary at epoch {STATE['epoch']}", flush=True)
    return {"ok": True, "role": ROLE, "epoch": STATE["epoch"]}


ROUTES = {
    "/seed": h_seed, "/assign": h_assign, "/node": h_node, "/heartbeat": h_heartbeat,
    "/leave": h_leave,
    "/deposit": h_deposit, "/elect": h_elect, "/model_ready": h_model_ready,
    "/retire": h_retire, "/goal/post": h_goal_post, "/goal/claim": h_goal_claim,
    "/goal/done": h_goal_done, "/task/post": h_task_post, "/task/claim": h_task_claim,
    "/task/done": h_task_done, "/promote": h_promote, "/control": h_control,
    "/task/fail": h_task_fail, "/goal/fail": h_goal_fail, "/report": h_report,
    "/immune": h_immune,
}


def goal_view(g: dict) -> dict:
    return {**g, "tasks": [STATE["tasks"][tid] for tid in g["tasks"] if tid in STATE["tasks"]]}


# --- identity ------------------------------------------------------------------
def load_keys() -> None:
    """The keys file maps host -> hex key; reloaded when it changes (enroll/revoke)."""
    global KEYS
    if not KEYS_PATH or time.time() - _keys_seen[1] < 2:
        return
    _keys_seen[1] = time.time()
    try:
        mtime = KEYS_PATH.stat().st_mtime
        if mtime != _keys_seen[0]:
            KEYS = {h: bytes.fromhex(k) for h, k in json.loads(KEYS_PATH.read_text()).items()}
            _keys_seen[0] = mtime
    except (OSError, ValueError) as e:
        print(f"keys file unreadable ({e}); keeping {len(KEYS)} known hosts", flush=True)


def signed_payload(method: str, path: str, ts: str, body: bytes) -> bytes:
    return f"{method}\n{path}\n{ts}\n{hashlib.sha256(body).hexdigest()}".encode()


def authenticate(headers, method: str, path: str, body: bytes) -> tuple[str | None, str | None]:
    """(host, None) for a valid signature, (None, None) for an unsigned call, (None, why)
    for a bad one. A signature is good once: replays inside the time window are refused."""
    host, ts, sig = (headers.get("X-Slime-Host"), headers.get("X-Slime-Time"),
                     headers.get("X-Slime-Sig"))
    if not (host or ts or sig):
        return None, None
    load_keys()
    key = KEYS.get(host or "")
    if not key:
        return None, f"unknown host {host!r}"
    try:
        skew = abs(time.time() - float(ts))
    except (TypeError, ValueError):
        return None, "bad timestamp"
    if skew > SKEW:
        return None, f"clock skew {skew:.0f}s"
    want = hmac.new(key, signed_payload(method, path, ts, body), hashlib.sha256).hexdigest()
    if not hmac.compare_digest(want, sig or ""):
        return None, "bad signature"
    with SIG_LOCK:
        t = time.time()
        if t - _pruned[0] > 5:
            for k in [k for k, exp in SEEN_SIGS.items() if exp < t]:
                del SEEN_SIGS[k]
            _pruned[0] = t
        if sig in SEEN_SIGS:
            return None, "replayed request"
        SEEN_SIGS[sig] = t + 2 * SKEW
    return host, None


# --- metrics -------------------------------------------------------------------
def med(xs) -> float | None:
    xs = [x for x in xs if x is not None]
    return round(statistics.median(xs), 2) if xs else None


def metrics() -> dict:
    """What a controller (or a person) needs to judge load and quality."""
    t = now()
    queue: dict[str, dict] = {}
    for task in STATE["tasks"].values():
        if task["status"] == "todo":
            q = queue.setdefault(str(task["min_tier"]), {"todo": 0, "oldest_s": 0.0})
            q["todo"] += 1
            q["oldest_s"] = max(q["oldest_s"], round(t - task.get("posted", t), 1))
    evs = STATE.get("events", [])
    recent = [e for e in evs if e["type"] == "task" and t - e["t"] < 300]
    tiers: dict[str, dict] = {}
    for tier in sorted({str(e["tier"]) for e in recent}):
        es = [e for e in recent if str(e["tier"]) == tier]
        tiers[tier] = {"done_5m": len(es), "wait_p50": med(e["wait"] for e in es),
                       "run_p50": med(e["run"] for e in es), "tps_p50": med(e["tps"] for e in es)}
    hosts: dict[str, int] = {}
    for e in recent:
        hosts[e.get("host") or "?"] = hosts.get(e.get("host") or "?", 0) + 1
    reviews = [e for e in evs if e["type"] == "task" and e["kind"] == "review"
               and e.get("agrees") is not None][-50:]
    goals = [e for e in evs if e["type"] == "goal"][-20:]
    return {
        "role": ROLE, "epoch": STATE.get("epoch", 1), "auth": AUTH,
        "queue": queue, "doing": sum(x["status"] == "doing" for x in STATE["tasks"].values()),
        "tiers_5m": tiers, "hosts_5m": hosts,
        "review_disagreement": (round(1 - sum(e["agrees"] for e in reviews) / len(reviews), 2)
                                if reviews else None), "reviews_n": len(reviews),
        "goals_waiting": sum(g["status"] != "done" for g in STATE["goals"].values()),
        "goal_latency_p50": med(e["latency"] for e in goals), "goals_n": len(goals),
        "unsigned_calls": dict(UNSIGNED), "known_hosts": sorted(KEYS),
        "wants": STATE.get("wants", {}), "live": {str(k): v for k, v in live_counts().items()},
        "scaling": [e for e in evs if e["type"] == "scale"][-5:],
        "immune": {"inflamed": inflamed(),
                   "quarantined": sorted(h for h in STATE.get("health", {}) if quarantined(h)),
                   "scores": {h: round(health(h)["score"], 2) for h in STATE.get("health", {})},
                   "poison_5m": sum(e["type"] == "poison" and t - e["t"] < 300 for e in evs),
                   "rejected_5m": sum(e["type"] == "incident" and e["kind"] == "bad_output"
                                      and t - e["t"] < 300 for e in evs)},
    }


# --- high availability ---------------------------------------------------------
def save_role() -> None:
    try:
        with open(ROLE_PATH, "w") as f:
            json.dump({"role": ROLE, "epoch": STATE.get("epoch", 1)}, f)
    except OSError as e:
        print(f"role file not written: {e}", flush=True)


def peer_get(path: str) -> dict:
    with urlopen(PEER + path, timeout=5) as r:
        return json.load(r)


def outranks(peer: dict) -> bool:
    """Two primaries: the higher epoch wins; on a tie, the later promotion."""
    mine = (STATE.get("epoch", 1), STATE.get("promoted_at", 0))
    return (peer.get("epoch", 1), peer.get("promoted_at", 0)) > mine


def peer_loop(promote_after: float) -> None:
    """Standby: copy the primary's state every few seconds; optionally promote itself if
    the primary has been unreachable for `promote_after` seconds (0 = only by hand).
    Primary: step down if the peer is primary at a higher epoch (we were replaced)."""
    global ROLE
    last_ok = time.time()
    while True:
        time.sleep(3)
        try:
            if ROLE == "standby":
                snap = peer_get("/snapshot")
                if snap.pop("_role", None) == "primary" and \
                        snap.get("epoch", 1) >= STATE.get("epoch", 1):
                    snap.pop("promoted_at", None)   # that's the primary's, not ours
                    with LOCK:
                        STATE.clear()
                        STATE.update(snap)
                        DIRTY.set()
                last_ok = time.time()
            else:
                peer = peer_get("/role")
                if peer.get("role") == "primary" and outranks(peer):
                    with LOCK:
                        ROLE = "standby"
                        save_role()
                    print(f"stepped down: {PEER} is primary at epoch {peer['epoch']}", flush=True)
        except (OSError, ValueError):
            if ROLE == "standby" and promote_after and time.time() - last_ok > promote_after:
                with LOCK:
                    h_promote({})
                    persist(sync=True)


class Handler(BaseHTTPRequestHandler):
    # keep-alive: runners hold one connection per slot instead of one per call;
    # an idle one is dropped after `timeout` seconds and the client reconnects
    protocol_version = "HTTP/1.1"
    timeout = 60

    def log_message(self, *a):  # quiet
        pass

    def _send(self, obj, code=200):
        data = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _send_blob(self, sha: str):
        """Stream a depot blob (or its tail, for a resumed download); outside the lock, so a
        big transfer never stalls the arena."""
        p = DEPOT / sha if DEPOT and SHA_RE.match(sha) else None
        if not p or not p.is_file():
            return self._send({"error": "no such blob"}, 404)
        size, start = p.stat().st_size, 0
        rng = self.headers.get("Range", "")
        if rng.startswith("bytes=") and rng.endswith("-") and rng[6:-1].isdigit():
            start = int(rng[6:-1])
            if start >= size:
                self.send_response(416)
                self.send_header("Content-Range", f"bytes */{size}")
                self.send_header("Content-Length", "0")
                self.end_headers()
                return
        self.send_response(206 if start else 200)
        self.send_header("Content-Type", "application/octet-stream")
        if start:
            self.send_header("Content-Range", f"bytes {start}-{size - 1}/{size}")
        self.send_header("Content-Length", str(size - start))
        self.end_headers()
        with open(p, "rb") as f:
            f.seek(start)
            try:
                while chunk := f.read(1 << 20):
                    self.wfile.write(chunk)
            except (BrokenPipeError, ConnectionResetError):
                self.close_connection = True   # the fetcher gave up (it resumes elsewhere)

    def _send_page(self):
        """The dashboard (dashboard.html next to this file): reads the JSON endpoints."""
        try:
            data = Path(__file__).with_name("dashboard.html").read_bytes()
        except OSError:
            return self._send({"error": "no dashboard.html"}, 404)
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Security-Policy", "default-src 'none'; script-src 'unsafe-inline'; "
                         "style-src 'unsafe-inline'; connect-src 'self'; base-uri 'none'; "
                         "form-action 'none'; frame-ancestors 'none'")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        u = urlparse(self.path)
        q = parse_qs(u.query)
        if u.path in ("/", "/index.html"):
            return self._send_page()
        if u.path.startswith("/blob/"):
            return self._send_blob(u.path[len("/blob/"):])
        if u.path == "/role":
            return self._send({"role": ROLE, "epoch": STATE.get("epoch", 1), "peer": PEER,
                               "promoted_at": STATE.get("promoted_at", 0)})
        with LOCK:
            if u.path == "/snapshot":
                return self._send({**STATE, "_role": ROLE})
            if u.path == "/metrics":
                return self._send(metrics())
            if u.path == "/control":
                return self._send(control_view())
            if u.path == "/immune":
                return self._send(immune_view())
            if u.path == "/events":
                since = float(q.get("since", ["0"])[0] or 0)
                return self._send({"events": read_events(since), "now": now()})
            if u.path == "/state":
                if ROLE == "primary":
                    reap()
                goals = sorted(STATE["goals"].values(), key=lambda g: int(g["id"]))
                return self._send({
                    "role": ROLE, "epoch": STATE.get("epoch", 1),
                    "budget": STATE.get("budget"), "used": used(),
                    "wants": STATE.get("wants", {}), "ready": STATE.get("ready", {}),
                    "counts": {str(k): v for k, v in live_counts().items()},
                    "agents": STATE.get("agents", {}),
                    "nodes": {h: n for h, n in STATE.get("nodes", {}).items()
                              if now() - n.get("seen", 0) < 60},
                    "node_used": {h: node_used(h) for h in STATE.get("nodes", {})},
                    "shards": {k: sorted(v) for k, v in STATE.get("shards", {}).items()},
                    "labels": {str(t["tier"]): t["label"]
                               for t in STATE.get("manifest", {}).get("tiers", [])},
                    "goals": [goal_view(g) for g in goals[-5:]],
                    "base": control_view()["base"],
                    "control_on": bool((STATE.get("control") or {}).get("enabled")),
                    "scaling": control_view()["decisions"][-3:],
                    "escalation": STATE.get("escalation"), "lead": lead_tier(),
                    "immune": {"inflamed": inflamed(),
                               "hosts": {h: [round(health(h)["score"], 1), health(h)["quarantined"]]
                                         for h in STATE.get("health", {})
                                         if health(h)["score"] >= 0.05 or health(h)["quarantined"]}},
                })
            if u.path == "/manifest":
                return self._send(STATE.get("manifest", {}))
            if u.path == "/where":
                return self._send({"peers": peers_holding(q.get("sha", [""])[0],
                                                          q.get("host", [""])[0])})
            if u.path == "/goal":
                g = STATE["goals"].get(q.get("id", [""])[0])
                return self._send(goal_view(g) if g else {}, 200 if g else 404)
            if u.path == "/shard":
                s = STATE["shards"].get(f"tier{q['tier'][0]}", {}).get(q["index"][0])
                return self._send(s or {}, 200 if s and "blob" in s else 404)
        self._send({"error": "not found"}, 404)

    def do_POST(self):
        try:
            n = int(self.headers.get("Content-Length", 0))
        except ValueError:
            n = -1
        if not 0 <= n <= MAX_BODY:
            self.close_connection = True
            return self._send({"error": f"body must be 0..{MAX_BODY} bytes"}, 413)
        raw = self.rfile.read(n)
        try:
            body = json.loads(raw or b"{}")
        except ValueError:
            return self._send({"error": "body is not JSON"}, 400)
        if not isinstance(body, dict):
            return self._send({"error": "body must be a JSON object"}, 400)
        fn = ROUTES.get(self.path)
        if not fn:
            return self._send({"error": "not found"}, 404)
        if self.path in ADMIN_ROUTES and ADMIN_TOKEN and not hmac.compare_digest(
                self.headers.get("X-Slime-Admin", ""), ADMIN_TOKEN):
            return self._send({"error": "admin token required"}, 403)
        if ROLE != "primary" and self.path != "/promote":
            return self._send({"error": "standby", "primary": PEER}, 503)
        host, why = authenticate(self.headers, "POST", self.path, raw)
        if why:
            return self._send({"error": why}, 401)
        body.pop("_auth", None)
        if self.path in RUNNER_ROUTES:
            if host:
                body["_auth"] = host
            elif AUTH == "required":
                return self._send({"error": "signature required"}, 401)
            else:
                who = str(body.get("host") or body.get("node") or self.client_address[0])
                UNSIGNED[who] = UNSIGNED.get(who, 0) + 1
        code, snap = 200, None
        with LOCK:
            a = STATE["agents"].get(body.get("agent_id", "")) if host else None
            if a and self.path in AGENT_ROUTES and a.get("host") != host:
                out, code = {"error": f"agent belongs to {a.get('host')}, not {host}"}, 403
            else:
                out = fn(body)
                if self.path in SYNC_ROUTES:
                    snap = snapshot()
                elif self.path not in LIVENESS_ROUTES:
                    DIRTY.set()
        if snap:   # durable before the caller hears "ok", but the lock is already free
            write_snapshot(snap, sync=True)
        self._send(out, code)


# --- staging: fill the depot from the manifest --------------------------------
def sha256_file(p: Path) -> str:
    h = hashlib.sha256()
    with open(p, "rb") as f:
        while chunk := f.read(1 << 22):
            h.update(chunk)
    return h.hexdigest()


def fetch_model(m: dict, incoming: Path) -> Path:
    """The publisher's file, verified against the publisher's SHA-256."""
    p = incoming / m["file"]
    if not p.exists():
        url = f"https://huggingface.co/{m['repo']}/resolve/main/{m['file']}"
        print(f"  downloading {url}")
        part = p.with_name(p.name + ".part")
        with urlopen(url, timeout=60) as r, open(part, "wb") as f:
            while chunk := r.read(1 << 22):
                f.write(chunk)
        os.replace(part, p)
    got = sha256_file(p)
    if p.stat().st_size != m["size"] or got != m["sha256"]:
        raise SystemExit(f"{m['file']}: sha {got[:12]} != published {m['sha256'][:12]}")
    print(f"  {m['file']}: {m['size']/1e9:.2f} GB, sha256 matches the publisher")
    return p


def cmd_stage(args) -> None:
    depot = Path(args.depot)
    incoming = depot / "incoming"
    incoming.mkdir(parents=True, exist_ok=True)
    manifest = json.loads(Path(args.manifest).read_text())
    tiers = {t["tier"]: t for t in manifest["tiers"]}
    for t in manifest["tiers"]:
        m = t.get("model")
        if not m:
            continue
        src = fetch_model(m, incoming)
        parent = tiers.get(t["tier"] - 1)
        n = (parent or {}).get("provides") or {}
        if not n:   # a base tier ships whole
            dst = depot / m["sha256"]
            if not dst.exists():
                os.link(src, dst)
            m["blob"] = m["sha256"]
            print(f"  tier{t['tier']}: whole blob {m['sha256'][:12]}")
            continue
        # a promoted tier exists only as shards until a quorum stitches it
        size, k = m["size"], n["n_shards"]
        step = -(-size // k)
        shards = []
        with open(src, "rb") as f:
            for i in range(k):
                data = f.read(step)
                sha = hashlib.sha256(data).hexdigest()
                dst = depot / sha
                if not (dst.exists() and dst.stat().st_size == len(data)):
                    tmp = dst.with_name(sha + ".tmp")   # peers may be fetching: never torn
                    tmp.write_bytes(data)
                    os.replace(tmp, dst)
                shards.append({"index": i, "sha": sha, "size": len(data)})
                print(f"  tier{t['tier']}: shard {i} {sha[:12]} {len(data)/1e6:.0f} MB")
        m["shards"] = shards
    out = depot / "manifest.staged.json"
    out.write_text(json.dumps(manifest, indent=1))
    print(f"staged manifest -> {out}")


def cmd_serve(args) -> None:
    global STATE_PATH, DEPOT, ADMIN_TOKEN, AUTH, KEYS_PATH, ROLE, PEER, ROLE_PATH
    STATE_PATH = args.state
    ROLE_PATH = args.state + ".role"
    DEPOT = Path(args.depot) if args.depot else None
    if args.admin_token_file:
        ADMIN_TOKEN = Path(args.admin_token_file).read_text().strip() or None
    AUTH = args.auth
    KEYS_PATH = Path(args.keys) if args.keys else None
    load_keys()
    if AUTH != "off" and not KEYS:
        print("warning: auth on but no host keys loaded", flush=True)
    PEER = args.peer.rstrip("/") if args.peer else None
    STATE.update(fresh_state(args.budget))
    # resume after a restart; agents from before it are stale and get reaped by heartbeat
    try:
        with open(STATE_PATH) as f:
            saved = json.load(f)
        if "electors" in saved:          # a v0 state file is not resumable; start clean
            STATE.update(saved)
            print(f"resumed from {STATE_PATH}: wants {STATE['wants']}, ready {sorted(STATE['ready'])}")
    except (FileNotFoundError, ValueError):
        pass
    if STATE.get("manifest"):   # settings added since this state was seeded get defaults
        STATE["control"] = {**control_config(STATE["manifest"]), **(STATE.get("control") or {})}
    ROLE = args.role
    try:   # a role this coordinator took on (promoted, or stepped down) outlives restarts
        r = json.loads(Path(ROLE_PATH).read_text())
        ROLE, STATE["epoch"] = r["role"], max(STATE.get("epoch", 1), r.get("epoch", 1))
    except (OSError, ValueError, KeyError):
        pass
    if ROLE == "primary" and PEER:   # never serve as primary while a newer one exists
        try:
            peer = peer_get("/role")
            if peer.get("role") == "primary" and outranks(peer):
                ROLE = "standby"
                save_role()
                print(f"starting as standby: {PEER} is primary at epoch {peer['epoch']}")
        except (OSError, ValueError):
            pass
    if not events_path().exists() and STATE.get("events"):   # start the long-term record
        with open(events_path(), "w") as f:                # with what state still holds
            f.writelines(json.dumps(e) + "\n" for e in STATE["events"])
    load_canaries()
    EXCUSED_UNTIL[0] = now() + 30   # agents from before the restart may have lapsed meanwhile
    threading.Thread(target=writer_loop, daemon=True).start()
    threading.Thread(target=control_loop, daemon=True).start()
    if PEER:
        threading.Thread(target=peer_loop, args=(args.promote_after,), daemon=True).start()
    srv = ThreadingHTTPServer(("0.0.0.0", args.port), Handler)
    print(f"coordinator on :{args.port} as {ROLE} (epoch {STATE.get('epoch', 1)}, budget "
          f"{STATE['budget']}, depot {DEPOT}, admin {'token' if ADMIN_TOKEN else 'open'}, "
          f"auth {AUTH} with {len(KEYS)} hosts, peer {PEER})"
          + ("" if STATE["wants"] else " — awaiting seed"), flush=True)
    srv.serve_forever()


def write_keys(path: Path, keys: dict) -> None:
    tmp = path.with_name(path.name + ".tmp")
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as f:
        json.dump(keys, f, indent=1, sort_keys=True)
    os.replace(tmp, path)


def cmd_enroll(args) -> None:
    """Register a host's key, read from stdin (so it travels over ssh, never on screen)."""
    key = sys.stdin.read().strip()
    if not re.fullmatch(r"[0-9a-f]{64}", key):
        raise SystemExit("expected a 64-hex-char key on stdin")
    path = Path(args.keys)
    keys = json.loads(path.read_text()) if path.exists() else {}
    keys[args.host] = key
    write_keys(path, keys)
    print(f"enrolled {args.host} ({len(keys)} hosts in {path})")


def cmd_revoke(args) -> None:
    path = Path(args.keys)
    keys = json.loads(path.read_text()) if path.exists() else {}
    if keys.pop(args.host, None) is None:
        raise SystemExit(f"{args.host} was not enrolled")
    write_keys(path, keys)
    print(f"revoked {args.host} ({len(keys)} hosts left)")


def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("serve")
    s.add_argument("--port", type=int, default=8700)
    s.add_argument("--budget", type=int, default=16)
    s.add_argument("--state", default="coord-state.json")
    s.add_argument("--depot", default=None)
    s.add_argument("--admin-token-file", default=None)
    s.add_argument("--keys", default=None, help="host keys file (JSON host -> hex key)")
    s.add_argument("--auth", choices=["off", "optional", "required"], default="off")
    s.add_argument("--role", choices=["primary", "standby"], default="primary")
    s.add_argument("--peer", default=None, help="the other coordinator's URL")
    s.add_argument("--promote-after", type=float, default=0,
                   help="standby: promote itself after this many seconds without the primary")
    g = sub.add_parser("stage")
    g.add_argument("--manifest", required=True)
    g.add_argument("--depot", required=True)
    for name in ("enroll", "revoke"):
        e = sub.add_parser(name)
        e.add_argument("host")
        e.add_argument("--keys", required=True)
    args = ap.parse_args()
    {"serve": cmd_serve, "stage": cmd_stage, "enroll": cmd_enroll,
     "revoke": cmd_revoke}[args.cmd](args)


if __name__ == "__main__":
    main()
