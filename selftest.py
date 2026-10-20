#!/usr/bin/env python3
"""
electric-slime self-test: the coordinator and runners exercised on loopback, no models.

Unit checks run in-process against coord.py's handlers. Scenarios each start a private
coordinator (signed hosts, auth required, admin token) and a few named stand-in hosts
(`slime.py pool`, instant fake inference made slow and faulty on purpose through
SLIME_STANDIN_DELAY / SLIME_FAULT / SLIME_PRESSURE_FILE), then drive and watch them over
HTTP. Nothing touches the live swarm: ports are picked free, files go to a temp dir.

    python3 selftest.py                 # everything (~10 minutes)
    python3 selftest.py unit control    # only names containing these words
    python3 selftest.py -k              # keep the temp dir (logs) afterwards
"""
from __future__ import annotations
import json, os, secrets, shutil, signal, socket, subprocess, sys, tempfile, time, traceback
import urllib.error
import urllib.request
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import coord      # noqa: E402  (unit checks call its handlers directly)
import slime      # noqa: E402

STANDIN = {
    "version": "selftest", "resource_total": 60,
    "control": {"up_after": 3, "down_after": 10, "cooldown": 3, "place_timeout": 20,
                "canary_every": 0},
    "tiers": [
        {"tier": 0, "label": "500M", "cost": 1, "seed": 6, "serve_floor": 2, "max": 6,
         "provides": {"tier": 1, "n_shards": 4, "k": 4}},
        {"tier": 1, "label": "2B", "cost": 4, "target": 3, "serve_floor": 2, "max": 4,
         "provides": {"tier": 2, "n_shards": 3, "k": 3}},
        {"tier": 2, "label": "7B", "cost": 10, "target": 1, "serve_floor": 1, "max": 2,
         "provides": {"tier": 3, "n_shards": 2, "k": 2}},
        {"tier": 3, "label": "14B", "cost": 12, "target": 1, "serve_floor": 1, "max": 1,
         "on_demand": True, "provides": None},
    ],
}


LAPSE_WAIT = 25   # long enough for any second charge to have settled


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class Swarm:
    """A private coordinator plus stand-in hosts: {name: {"slots", "env"}}."""

    def __init__(self, work: Path, hosts: dict, manifest: dict | None = None,
                 immune: dict | None = None, delay: float = 0.5, standby: bool = False):
        self.work, self.hosts, self.delay = work, hosts, delay
        self.port = free_port()
        self.url = f"http://127.0.0.1:{self.port}"
        self.primary_url = self.url
        self.standby_url = f"http://127.0.0.1:{free_port()}" if standby else None
        self.procs: dict[str, subprocess.Popen] = {}
        keys = {}
        for name in hosts:
            (work / name).mkdir(parents=True, exist_ok=True)
            keys[name] = secrets.token_hex(32)
            (work / name / "host.key").write_text(keys[name])
        (work / "keys.json").write_text(json.dumps(keys))
        self.token = secrets.token_hex(16)
        (work / "admin.token").write_text(self.token)
        self.coord = self.start_coord(self.primary_url, "primary", self.standby_url, "state.json")
        self.wait(lambda: self.get("/role"), 10, "coordinator up")
        self.standby = (self.start_coord(self.standby_url, "standby", self.primary_url,
                                         "standby.json") if standby else None)
        self.started = time.time()
        for name in hosts:
            self.start(name)
        m = json.loads(json.dumps(manifest or STANDIN))
        if immune:
            m["immune"] = immune
        genome = json.loads((HERE / "genome.json").read_text())
        self.admin("/seed", {"manifest": m, "genome": genome, "budget": m["resource_total"]})

    def start_coord(self, url: str, role: str, peer: str | None, state: str) -> subprocess.Popen:
        return subprocess.Popen(
            [sys.executable, "-u", str(HERE / "coord.py"), "serve",
             "--port", url.rsplit(":", 1)[1], "--state", str(self.work / state),
             "--keys", str(self.work / "keys.json"), "--auth", "required",
             "--admin-token-file", str(self.work / "admin.token"), "--role", role]
            + (["--peer", peer] if peer else []),
            stdout=open(self.work / f"coord-{state}.log", "a"), stderr=subprocess.STDOUT)

    def start(self, name: str, **env_over) -> None:
        h = self.hosts[name]
        env = {**os.environ, "SLIME_CACHE": str(self.work / name / "cache"),
               "SLIME_KEY_FILE": str(self.work / name / "host.key"),
               "SLIME_PEER_PORT": str(free_port()), "SLIME_STANDIN_DELAY": str(self.delay),
               "SLIME_CAPACITY_MB": "100", "SLIME_PREFETCH": "0",
               "SLIME_PRESSURE_FILE": str(self.work / name / "pressure"),
               **h.get("env", {}), **env_over}
        env.pop("SLIME_FAULT", None) if env_over.get("SLIME_FAULT") == "" else None
        self.procs[name] = subprocess.Popen(
            [sys.executable, "-u", str(HERE / "slime.py"), "pool", "--coord",
             ",".join(u for u in (self.primary_url, self.standby_url) if u),
             "--slots", str(h.get("slots", 3)), "--node", name],
            env=env, stdout=open(self.work / f"{name}.log", "a"), stderr=subprocess.STDOUT)

    def signal(self, name: str, sig: int) -> None:
        self.procs[name].send_signal(sig)
        if sig in (signal.SIGTERM, signal.SIGKILL):
            self.procs[name].wait(15)

    def pressure(self, name: str, level: str) -> None:
        (self.work / name / "pressure").write_text(level)

    def get(self, path: str) -> dict:
        with urllib.request.urlopen(self.url + path, timeout=5) as r:
            return json.load(r)

    def post(self, path: str, body: dict, headers: dict | None = None) -> dict:
        req = urllib.request.Request(self.url + path, json.dumps(body).encode(),
                                     {"Content-Type": "application/json", **(headers or {})})
        with urllib.request.urlopen(req, timeout=5) as r:
            return json.load(r)

    def admin(self, path: str, body: dict) -> dict:
        return self.post(path, body, {"X-Slime-Admin": self.token})

    def ask(self, text: str) -> str:
        return self.post("/goal/post", {"text": text})["goal_id"]

    def goal(self, gid: str) -> dict:
        return self.get(f"/goal?id={gid}")

    def wait(self, cond, timeout: float, what: str):
        t_end = time.time() + timeout
        last = None
        while time.time() < t_end:
            try:
                v = cond()
                if v:
                    return v
            except (OSError, ValueError, KeyError) as e:
                last = e
            time.sleep(0.5)
        raise AssertionError(f"timed out after {timeout:.0f}s waiting for: {what}"
                             + (f" (last error {last!r})" if last else ""))

    def assembled(self, timeout: float = 90) -> None:
        self.wait(lambda: {"1", "2"} <= set(self.get("/state")["ready"])
                  and self.get("/state")["counts"].get("2"), timeout, "tiers 1 and 2 ready")

    def past_grace(self) -> None:
        """Lapses right after a coordinator starts are never charged (30 s)."""
        time.sleep(max(0.0, self.started + 31 - time.time()))

    def agents_on(self, name: str) -> list[dict]:
        return [a for a in self.get("/state")["agents"].values() if a.get("host") == name]

    def stop(self) -> None:
        for p in self.procs.values():
            if p.poll() is None:
                p.kill()
        coords = [c for c in (self.coord, self.standby) if c]
        for c in coords:
            c.kill()
        for p in [*self.procs.values(), *coords]:
            try:
                p.wait(5)
            except subprocess.TimeoutExpired:
                pass


# --- unit checks (in-process) ----------------------------------------------------
def fresh(manifest=STANDIN) -> None:
    coord.STATE.clear()
    coord.STATE.update(coord.fresh_state(manifest["resource_total"]))
    coord.STATE.update(manifest=json.loads(json.dumps(manifest)),
                       genome=json.loads((HERE / "genome.json").read_text()),
                       control=coord.control_config(manifest), immune=dict(coord.IMMUNE_DEFAULTS),
                       ready={"1": "x", "2": "y"}, wants={"0": 2, "1": 2, "2": 1})
    coord.EXCUSED_UNTIL[0] = 0.0


def unit_screen():
    s = coord.screen
    assert s("Paris is the capital of France.", "answer") is None
    assert s("   ", "answer") == "empty"
    assert s("lorem ipsum " * 40, "answer") == "degenerate repetition"
    assert s("\n".join(["same line"] * 9), "answer") in ("repeated lines", "degenerate repetition")
    assert s('{"own_answer": "100 C", "agrees": true}', "review") is None
    assert s('{"agrees": true}', "review") == "review lacks own_answer/agrees"
    assert s("OK", "review") == "review is not JSON"
    real = ("Rayleigh scattering makes shorter wavelengths scatter more strongly off air "
            "molecules, so blue light reaches the eye from every direction of the sky. At "
            "sunset the light crosses far more atmosphere, the blue is scattered out of the "
            "beam, and the remaining reds and oranges dominate what we see near the horizon.")
    assert s(real, "answer") is None, "ordinary prose must pass"


def unit_quarantine_and_decay():
    fresh()
    clock = [1000.0]
    coord.now = lambda: clock[0]
    try:
        for _ in range(3):
            coord.incident("h1", "bad_output", "x")
        assert coord.quarantined("h1")
        clock[0] += 600            # one half-life: 3.0 -> 1.5, still held (release at 1.0)
        assert coord.quarantined("h1")
        clock[0] += 600            # 0.75: released
        assert not coord.quarantined("h1")
        assert any(e["type"] == "release" for e in coord.STATE["events"])
    finally:
        coord.now = time.time


def unit_mass_lapse():
    fresh()
    clock = [1000.0]
    coord.now = lambda: clock[0]
    try:
        for h in ("a", "b", "c", "d"):
            coord.STATE["nodes"][h] = {"seen": clock[0]}
        coord.note_lapse({"a", "b", "c"})        # most of the swarm at once: our fault
        clock[0] += 11
        coord.settle_lapses()
        assert coord.STATE["health"].get("a", {}).get("score", 0) == 0
        assert any(e["type"] == "mass_lapse" for e in coord.STATE["events"])
        coord.note_lapse({"d"})                  # one host alone: its fault, charged once
        clock[0] += 0.5                          # though its agents are reaped over passes
        coord.note_lapse({"d"})
        clock[0] += 0.5
        coord.note_lapse({"d"})
        clock[0] += 10
        for h in ("a", "b", "c", "d"):
            coord.STATE["nodes"][h]["seen"] = clock[0]
        coord.settle_lapses()
        assert round(coord.health("d")["score"], 2) == 1.0
    finally:
        coord.now = time.time


def unit_sleep_excused():
    """A laptop that slept: the lapse it caused is taken back when it says so on waking."""
    fresh()
    clock = [10_000.0]
    coord.now = lambda: clock[0]
    try:
        coord.STATE["nodes"].update({h: {"seen": clock[0]} for h in "abcd"})
        coord.note_lapse({"lid"})                  # heartbeats stop as the lid shuts
        clock[0] += 11
        coord.settle_lapses()
        assert round(coord.health("lid")["score"], 2) == 1.0
        clock[0] += 600
        coord.excuse_sleep("lid", [[10_000.0 - 5, clock[0] - 1]])
        assert coord.health("lid")["score"] < 0.01, coord.health("lid")
        assert not coord.inflamed()
        coord.note_lapse({"lid"})                  # asleep again, says so before settling
        coord.excuse_sleep("lid", [[clock[0] - 5, clock[0] + 1]])
        clock[0] += 11
        coord.settle_lapses()
        assert coord.health("lid")["score"] < 0.01
    finally:
        coord.now = time.time


def unit_poison_refund():
    fresh()
    t = {"id": "9", "goal": "1", "status": "doing", "posted": time.time(), "prompt": "p9"}
    for h in ("a", "b", "c"):
        coord.incident(h, "task_error", "boom", t)
        coord.retry_task(t, h, "boom")
    assert t["status"] == "failed", t
    for h in ("a", "b", "c"):
        assert coord.health(h)["score"] < 0.01, (h, coord.health(h))
    # one host failing it every time keeps its charge, but it is charged once per task
    t2 = {"id": "10", "goal": "1", "status": "doing", "posted": time.time(), "prompt": "p10"}
    for _ in range(3):
        coord.incident("z", "task_error", "boom", t2)
        coord.retry_task(t2, "z", "boom")
    assert t2["status"] == "failed" and round(coord.health("z")["score"], 2) == 0.5


def unit_antibodies():
    fresh()
    t = {"id": "9", "goal": "1", "status": "doing", "posted": time.time(), "prompt": "Bad  Input"}
    for h in ("a", "b", "c"):
        coord.incident(h, "task_error", "boom", t)
        coord.retry_task(t, h, "boom")
    assert coord.antibody("bad input"), "remembered, whitespace/case-insensitively"
    coord.STATE["goals"]["1"] = {"id": "1", "tasks": [], "orchestrator": None, "status": "active"}
    coord.h_task_post({"goal_id": "1", "tasks": [{"prompt": "bad   INPUT"}, {"prompt": "fine"}]})
    sts = [coord.STATE["tasks"][i]["status"] for i in coord.STATE["goals"]["1"]["tasks"]]
    assert sts == ["failed", "todo"], sts
    one = {"id": "10", "goal": "1", "status": "doing", "posted": time.time(), "prompt": "only z"}
    for _ in range(3):
        coord.incident("z", "task_error", "boom", one)
        coord.retry_task(one, "z", "boom")
    assert not coord.antibody("only z"), "one host failing it proves nothing about the input"
    coord.h_immune({"forget": "all"})
    assert not coord.antibody("bad input")


def unit_old_state_upgrade():
    """A coordinator restarted on state seeded by older code: settings it lacks get defaults."""
    fresh()
    coord.STATE["control"] = {"enabled": True, "interval": 2.0, "up_after": 5.0,
                              "down_after": 60.0, "cooldown": 20.0, "place_timeout": 60.0,
                              "max": {"0": 4, "1": 4, "2": 2}}
    coord.STATE.pop("immune", None)
    coord.STATE.pop("escalation", None)
    coord.STATE["control"] = {**coord.control_config(coord.STATE["manifest"]),
                              **coord.STATE["control"]}     # what cmd_serve does on resume
    coord.control_view()
    coord.escalation_step()
    coord.control_step()
    coord.immune_view()
    coord.metrics()


def unit_snapshot_order():
    """Snapshots are written outside the lock: an older one never overwrites a newer one,
    and an acknowledged (sync) write whose snapshot was overtaken still gets fsync'd."""
    fresh()
    path = Path(tempfile.mkdtemp()) / "state.json"
    old_path, coord.STATE_PATH = coord.STATE_PATH, str(path)
    try:
        coord.STATE["marker"] = "first"
        a = coord.snapshot()
        coord.STATE["marker"] = "second"
        b = coord.snapshot()
        coord.write_snapshot(b)
        coord.write_snapshot(a, sync=True)        # older: skipped, but the file is fsync'd
        assert json.loads(path.read_text())["marker"] == "second"
        assert coord._snap_seq[2] == b[0]
    finally:
        coord.STATE_PATH = old_path
        shutil.rmtree(path.parent, ignore_errors=True)


def unit_slow_disk_never_blocks():
    """A 2 s fsync (a disk busy with something else) must not hold the arena's lock."""
    import threading
    fresh()
    path = Path(tempfile.mkdtemp()) / "state.json"
    old_path, coord.STATE_PATH = coord.STATE_PATH, str(path)
    real_fsync = os.fsync
    os.fsync = lambda fd: (time.sleep(2), real_fsync(fd))[1]
    try:
        with coord.LOCK:
            snap = coord.snapshot()
        w = threading.Thread(target=coord.write_snapshot, args=(snap, True))
        w.start()
        time.sleep(0.2)
        t0 = time.time()
        with coord.LOCK:                          # what a heartbeat needs
            pass
        assert time.time() - t0 < 0.1, "lock held during fsync"
        w.join()
        assert path.exists()
    finally:
        os.fsync = real_fsync
        coord.STATE_PATH = old_path
        shutil.rmtree(path.parent, ignore_errors=True)


def unit_report_dedupe():
    fresh()
    coord.STATE["nodes"]["evil"] = {"peer": ["http://10.0.0.9:8701"], "seen": time.time()}
    r1 = coord.h_report({"_auth": "victim", "kind": "bad_blob", "peer": "http://10.0.0.9:8701",
                         "sha": "ab" * 32})
    r2 = coord.h_report({"_auth": "victim", "kind": "bad_blob", "peer": "http://10.0.0.9:8701",
                         "sha": "ab" * 32})
    assert r1["counted"] and not r2["counted"]
    assert coord.h_report({"_auth": "evil", "kind": "bad_blob",
                           "peer": "http://10.0.0.9:8701"})["ok"] is False   # not about itself
    assert round(coord.health("evil")["score"], 1) == 1.5
    assert coord.peers_holding("cd" * 32, "x") == []


def unit_scale_in_order():
    fresh()
    t = time.time()
    for h in ("good", "sick"):
        coord.STATE["nodes"][h] = {"seen": t, "polled": t, "capacity_mb": 100, "max_tier": 99}
    coord.STATE["agents"] = {
        "g": {"tier": 0, "cost": 1, "host": "good", "last_hb": t, "phase": "ready"},
        "s": {"tier": 0, "cost": 1, "host": "sick", "last_hb": t, "phase": "ready"},
        "g2": {"tier": 0, "cost": 1, "host": "good", "last_hb": t, "phase": "ready"}}
    coord.STATE["wants"]["0"] = 2
    coord.incident("sick", "task_error", "x")
    assert coord.h_retire({"agent_id": "g"})["retire"] is False, "healthy host kept"
    assert coord.h_retire({"agent_id": "s"}) == {"retire": True}, "sick host's replica goes"
    # the only running replica stays while its replacement is still loading
    coord.STATE["agents"] = {
        "up": {"tier": 2, "cost": 10, "host": "good", "last_hb": t, "phase": "ready"},
        "new": {"tier": 2, "cost": 10, "host": "sick", "last_hb": t, "phase": "loading"}}
    coord.STATE["wants"]["2"] = 1
    assert coord.h_retire({"agent_id": "up"})["retire"] is False


def unit_speed_placement():
    """A slow host yields a short tier to a much faster idle one; a calm tier on a slow host
    is rebalanced toward the fast one (one extra replica; scale-in retires the slowest)."""
    fresh()
    t = time.time()
    for h in ("fast", "slow"):
        coord.STATE["nodes"][h] = {"seen": t, "polled": t, "capacity_mb": 100, "max_tier": 99}
    for _ in range(5):
        coord.record({"type": "task", "kind": "answer", "tier": 1, "host": "fast", "tps": 40,
                      "wait": 0, "run": 1, "agrees": None})
        coord.record({"type": "task", "kind": "answer", "tier": 1, "host": "slow", "tps": 8,
                      "wait": 0, "run": 5, "agrees": None})
    assert coord.faster_taker("slow", 1, 4) and not coord.faster_taker("fast", 1, 4)
    coord.STATE["wants"] = {"0": 0, "1": 1, "2": 0}
    coord.STATE["agents"] = {"s1": {"tier": 1, "cost": 4, "host": "slow", "last_hb": t,
                                    "phase": "ready"}}
    assert coord.rebalance_step() and coord.STATE["wants"]["1"] == 2
    assert not coord.rebalance_step(), "once per tier per REBALANCE_EVERY"
    coord.STATE["agents"]["f1"] = {"tier": 1, "cost": 4, "host": "fast", "last_hb": t,
                                   "phase": "ready"}
    coord.STATE["wants"]["1"] = 1                     # the idle scale-in, later
    assert coord.h_retire({"agent_id": "f1"})["retire"] is False, "fast replica stays"
    assert coord.h_retire({"agent_id": "s1"}) == {"retire": True}, "slow one goes"


def unit_lead_relocation():
    """The lead tier moves to a much faster host by displacing idle lower agents that fit
    elsewhere; the reserved host takes only the lead; the slow replica retires after."""
    fresh()
    t = time.time()
    nodes = {"fast": 14, "slow": 30, "spare": 30}
    for h, cap in nodes.items():
        coord.STATE["nodes"][h] = {"seen": t, "polled": t, "capacity_mb": cap, "max_tier": 99}
    for _ in range(5):
        for h, tps in (("fast", 60), ("slow", 12)):
            coord.record({"type": "task", "kind": "synth", "tier": 2, "host": h, "tps": tps,
                          "wait": None, "run": None, "agrees": None})
    A = lambda tier, host, phase="ready": {"tier": tier, "cost": [1, 4, 10][tier], "host": host,  # noqa: E731
                                           "node": host, "last_hb": t, "phase": phase}
    coord.STATE["agents"] = {"lead": A(2, "slow"), "x0": A(0, "fast"), "x1": A(1, "fast"),
                             "y1": A(1, "fast")}
    coord.STATE["wants"] = {"0": 1, "1": 2, "2": 1}
    assert coord.relocate_step(), "relocation starts"
    r = coord.STATE["relocation"]
    assert r["host"] == "fast" and coord.STATE["wants"]["2"] == 2 and len(r["evict"]) == 2, r
    for aid in list(r["evict"]):
        assert coord.h_retire({"agent_id": aid})["retire"] is True
    got = coord.h_assign({"_auth": "spare", "node": "spare-0", "capacity_mb": 30, "cached": []})
    assert got.get("tier") != 2, "only the reserved host may take the lead tier"
    got = coord.h_assign({"_auth": "fast", "node": "fast-0", "capacity_mb": 14, "cached": []})
    assert got.get("tier") == 2, got
    coord.STATE["agents"][got["agent_id"]]["phase"] = "ready"
    coord.relocation_done()
    assert coord.STATE["wants"]["2"] == 1 and "relocation" not in coord.STATE
    coord.STATE["agents"]["lead"]["phase"] = "awaiting goal 9"     # busy: it goes when done
    assert coord.h_retire({"agent_id": got["agent_id"]})["retire"] is False, "fast one stays"
    coord.STATE["agents"]["lead"]["phase"] = "ready"
    assert coord.h_retire({"agent_id": "lead"}) == {"retire": True}


def unit_canaries():
    """Known-answer questions: asked only when idle, checked, and a run of wrong answers
    escalates (judged on the tier leading at the time)."""
    fresh()
    coord.STATE["control"]["canary_every"] = 900.0   # the stand-in manifest turns them off
    coord.load_canaries()
    assert coord.CANARIES, "canaries.json loads"
    clock = [50_000.0]
    coord.now = lambda: clock[0]
    try:
        coord.STATE["agents"] = {"o": {"tier": 2, "cost": 10, "host": "h", "last_hb": clock[0],
                                       "phase": "ready"}}
        coord.STATE["goals"]["9"] = {"id": "9", "status": "todo", "tasks": [], "orchestrator": None}
        assert not coord.canary_step(), "never while real work waits"
        coord.STATE["goals"]["9"]["status"] = "done"
        assert coord.canary_step()
        g = next(g for g in coord.STATE["goals"].values() if g.get("canary"))
        assert not coord.canary_step(), "one per canary_every"
        probes = [t for t in coord.STATE["tasks"].values() if t.get("only_tier") is not None]
        assert sorted(t["only_tier"] for t in probes) == [0, 1], probes   # serving tiers
        coord.STATE["agents"]["w"] = {"tier": 1, "cost": 4, "host": "h", "last_hb": clock[0],
                                      "phase": "ready"}
        got = coord.h_task_claim({"agent_id": "w", "tier": 1})
        assert got["task"]["only_tier"] == 1, "a tier claims only its own probe"
        k1 = next(x for x in coord.CANARIES if x["id"] == got["task"]["canary"])
        coord.h_task_done({"id": got["task"]["id"], "agent_id": "w", "result": "I am not sure."})
        ev = [e for e in coord.STATE["events"] if e["type"] == "canary" and e.get("probe")]
        assert ev and ev[-1]["tier"] == 1 and ev[-1]["ok"] is False, ev
        assert got["task"]["id"] not in coord.STATE["tasks"], "probe dropped once checked"
        assert coord.canary_failure(3600) == (None, 0), "probes don't steer escalation"
        k = next(x for x in coord.CANARIES if x["id"] == g["canary"])
        g.update(status="done", done=clock[0], posted=clock[0] - 30, answer="no idea")
        coord.check_canary(g)
        for _ in range(3):
            coord.record({"type": "canary", "id": k["id"], "ok": False, "lead": 2})
        assert coord.canary_failure(3600, 2) == (1.0, 4)
        assert coord.escalation_step() and coord.escalated()
        assert "canaries" in coord.STATE["escalation"]["why"], coord.STATE["escalation"]
    finally:
        coord.now = time.time


def unit_avoid_only_with_alternatives():
    """A review avoids the answer's host only while another host could take it."""
    fresh()
    t = time.time()
    coord.STATE["agents"] = {"r": {"tier": 1, "cost": 4, "host": "m1", "last_hb": t, "phase": "ready"}}
    coord.STATE["tasks"]["5"] = {"id": "5", "goal": "1", "status": "todo", "min_tier": 1,
                                 "posted": t, "avoid": ["m1"], "kind": "review", "prompt": "q"}
    assert coord.h_task_claim({"agent_id": "r", "tier": 1})["claimed"], "no one else: take it"
    coord.STATE["tasks"]["5"].update(status="todo", worker=None)
    coord.STATE["agents"]["o"] = {"tier": 1, "cost": 4, "host": "neo", "last_hb": t, "phase": "task 9"}
    assert not coord.h_task_claim({"agent_id": "r", "tier": 1})["claimed"], "neo can: leave it"
    # a host that *failed* the task is banned from it for a while, alternatives or not
    del coord.STATE["agents"]["o"]
    coord.STATE["tasks"]["6"] = {"id": "6", "goal": "1", "status": "doing", "min_tier": 1,
                                 "posted": t, "avoid": [], "kind": "answer", "prompt": "q",
                                 "worker": "r", "worker_host": "m1"}
    coord.retry_task(coord.STATE["tasks"]["6"], "m1", "output rejected")
    got = coord.h_task_claim({"agent_id": "r", "tier": 1})
    assert not got.get("claimed") or got["task"]["id"] != "6", "not straight back to m1"


def unit_no_doubled_orchestrators():
    """Goals queue: a second orchestrator may go to another host, never beside the first."""
    fresh()
    t = time.time()
    coord.STATE["nodes"]["big"] = {"seen": t, "polled": t, "capacity_mb": 100, "max_tier": 99}
    coord.STATE["agents"] = {"lead": {"tier": 2, "cost": 10, "host": "big", "last_hb": t,
                                      "phase": "awaiting goal 1"}}
    assert not coord.placeable(2), "only the orchestrator's own host has room"
    coord.STATE["wants"]["2"] = 2
    got = coord.h_assign({"_auth": "big", "node": "big-1", "capacity_mb": 100, "cached": []})
    assert got.get("tier") != 2, got
    coord.STATE["nodes"]["other"] = {"seen": t, "polled": t, "capacity_mb": 100, "max_tier": 99}
    assert coord.placeable(2)
    got = coord.h_assign({"_auth": "other", "node": "other-1", "capacity_mb": 100, "cached": []})
    assert got.get("tier") == 2, got


def unit_withdraw_and_no_evict():
    fresh()
    clock = [1000.0]
    coord.now = lambda: clock[0]
    try:
        coord.STATE["nodes"]["h"] = {"capacity_mb": 100, "seen": clock[0], "polled": clock[0],
                                     "max_tier": 99}
        for i, tier in enumerate([0, 0, 1, 1, 2]):
            coord.STATE["agents"][f"a{i}"] = {"tier": tier, "cost": [1, 4, 10][tier],
                                              "host": "h", "last_hb": clock[0], "phase": "task 1"}
        coord.STATE["goals"]["50"] = {"id": "50", "status": "active", "orchestrator": "a4",
                                      "tasks": []}
        coord.STATE["tasks"]["51"] = {"id": "51", "status": "todo", "min_tier": 1,
                                      "posted": clock[0], "goal": "50"}
        for _ in range(3):
            clock[0] += 2
            for a in coord.STATE["agents"].values():
                a["last_hb"] = clock[0]
            coord.STATE["nodes"]["h"].update(seen=clock[0], polled=clock[0])
            coord.control_step()
        assert coord.STATE["wants"]["1"] == 3, coord.STATE["wants"]
        for _ in range(12):
            clock[0] += 2
            for a in coord.STATE["agents"].values():
                a["last_hb"] = clock[0]
            coord.STATE["nodes"]["h"].update(seen=clock[0], polled=clock[0])
            coord.control_step()
        assert coord.STATE["wants"]["1"] == 2, "an unplaced raise is withdrawn"
    finally:
        coord.now = time.time


# --- scenarios (processes on loopback) --------------------------------------------
def scenario_assemble_and_answer(work: Path):
    sw = Swarm(work, {"a": {"slots": 4}, "b": {"slots": 4}, "c": {"slots": 3}})
    try:
        sw.assembled()
        gid = sw.ask("What is a swarm?")
        g = sw.wait(lambda: sw.goal(gid)["status"] == "done" and sw.goal(gid), 60, "answer")
        assert "[stand-in tier2]" in g["answer"], g["answer"]
        reviews = [t for t in g["tasks"] if t["kind"] == "review"]
        answers = {t["id"]: t for t in g["tasks"]}
        for r in reviews:   # a review prefers another host than the answer it checks
            assert r["avoid"] == [answers[r["of"]]["worker_host"]], (r, answers[r["of"]])
        m = sw.get("/metrics")
        assert m["unsigned_calls"] == {}, m["unsigned_calls"]
        assert sw.post("/goal/post", {"text": "x" * 5000})["ok"] is False   # bounded
        try:   # refused unread: a 413, or the connection closed under the sender
            sw.post("/goal/post", {"text": "y" * (2 << 20)})
            raise AssertionError("2 MB body accepted")
        except urllib.error.HTTPError as e:
            assert e.code == 413
        except (urllib.error.URLError, ConnectionError):
            pass
        assert sw.get("/role")["role"] == "primary", "still serving after the oversized post"
        with urllib.request.urlopen(sw.url + "/", timeout=5) as r:   # the dashboard
            page = r.read().decode()
            assert r.headers["Content-Type"].startswith("text/html") and "<title>" in page
            assert "default-src 'none'" in r.headers["Content-Security-Policy"]
        rep = subprocess.run([sys.executable, str(HERE / "slime.py"), "report", "--coord", sw.url],
                             capture_output=True, text=True, timeout=30)
        assert rep.returncode == 0 and "goals: 2 answered" in rep.stdout, rep.stdout + rep.stderr
    finally:
        sw.stop()


def scenario_control_burst(work: Path):
    sw = Swarm(work, {"a": {"slots": 6}, "b": {"slots": 6}}, delay=3)
    try:
        sw.assembled()
        sw.wait(lambda: sw.get("/state")["wants"] == {"0": 2, "1": 2, "2": 1}, 60, "base wants")
        ids = [sw.ask(f"burst {i}") for i in range(6)]
        peak = {}

        def done():
            for k, v in sw.get("/state")["wants"].items():
                peak[k] = max(peak.get(k, 0), v)
            return all(sw.goal(i)["status"] == "done" for i in ids)
        sw.wait(done, 240, "6 goals answered")
        base = {"0": 2, "1": 2, "2": 1}
        assert any(peak[k] > v for k, v in base.items()), f"never scaled out: {peak}"
        assert all(peak[k] <= m for k, m in (("0", 6), ("1", 4), ("2", 2))), f"over max: {peak}"
        sw.wait(lambda: sw.get("/state")["wants"] == {"0": 2, "1": 2, "2": 1}, 90,
                "back to base after idle")
    finally:
        sw.stop()


def scenario_bad_output_quarantine(work: Path):
    """A host whose model emits garbage: its answers never count, it is quarantined, its
    agents leave; restarted healthy, it is released and works again."""
    imm = {"half_life": 30.0, "quarantine_at": 3.0, "release_at": 1.0}
    # the healthy hosts can't hold the whole base load alone, so the sick one must take work
    sw = Swarm(work, {"a": {"slots": 2}, "b": {"slots": 2},   # 4 slots < the base load of 5
                      "sick": {"slots": 4, "env": {"SLIME_FAULT": "garbage",
                                                   "SLIME_MAX_TIER": "1"}}}, immune=imm)
    try:
        sw.assembled()
        ids = [sw.ask(f"question {i}") for i in range(4)]
        sw.wait(lambda: sw.get("/immune")["hosts"].get("sick", {}).get("quarantined"), 120,
                "sick host quarantined")
        sw.wait(lambda: not sw.agents_on("sick"), 30, "sick host's agents gone")
        sw.wait(lambda: all(sw.goal(i)["status"] == "done" for i in ids), 180, "goals done")
        for i in ids:
            for t in sw.goal(i)["tasks"]:
                assert t["status"] == "failed" or "lorem" not in (t["result"] or ""), t
        im = sw.get("/immune")
        assert not im["hosts"].get("a", {}).get("quarantined")
        sw.signal("sick", signal.SIGTERM)
        sw.start("sick", SLIME_FAULT="")
        sw.wait(lambda: not sw.get("/immune")["hosts"]["sick"]["quarantined"], 90, "released")
        gid = sw.ask("after release")
        sw.wait(lambda: sw.goal(gid)["status"] == "done", 90, "answered after release")
    finally:
        sw.stop()


def scenario_inflammation(work: Path):
    """Incidents spike -> orchestrators cross-check tier-1 answers too."""
    sw = Swarm(work, {"a": {"slots": 5}, "b": {"slots": 5}},
               immune={"inflame_at": 1, "quarantine_at": 50})
    try:
        sw.assembled()
        g0 = sw.ask("before")
        sw.wait(lambda: sw.goal(g0)["status"] == "done", 60, "first goal")
        t1_reviewed = lambda g: any(t["kind"] == "review" and   # noqa: E731
                                    next(x for x in g["tasks"] if x["id"] == t["of"])
                                    .get("worker_tier") == 1 for t in g["tasks"])
        assert not t1_reviewed(sw.goal(g0)), "calm swarm: tier-1 answers unreviewed"
        sw.past_grace()
        victim = sw.wait(lambda: [h for h in sw.procs if sw.agents_on(h)], 30, "agents")[0]
        sw.signal(victim, signal.SIGKILL)         # one lapse is enough with inflame_at 1
        sw.wait(lambda: sw.get("/immune")["inflamed"], 60, "inflamed")
        sw.start(victim)
        g1 = sw.ask("during")
        g = sw.wait(lambda: sw.goal(g1)["status"] == "done" and sw.goal(g1), 120, "second goal")
        assert t1_reviewed(g), [(t["kind"], t.get("worker_tier")) for t in g["tasks"]]
    finally:
        sw.stop()


def scenario_poison_task(work: Path):
    """Inputs the models choke on: a subtask every host fails is given up after 3 tries
    and its charges refunded; a goal the orchestrator can't plan is closed unanswered.
    The swarm keeps answering, and no host is quarantined for it."""
    env = {"SLIME_FAULT": "poison:part 2 of,poisongoal:POISON"}
    sw = Swarm(work, {"a": {"slots": 4, "env": env}, "b": {"slots": 4, "env": env},
                      "c": {"slots": 3, "env": env}})
    try:
        sw.assembled()
        g1, g2 = sw.ask("an ordinary question"), sw.ask("POISON pill")
        g = sw.wait(lambda: sw.goal(g1)["status"] == "done" and sw.goal(g1), 180, "goal 1 done")
        bad = [t for t in g["tasks"] if t["kind"] == "answer" and "part 2 of" in t["prompt"]]
        assert bad and all(t["status"] == "failed" for t in bad), [t["status"] for t in bad]
        assert "[stand-in tier2]" in g["answer"], g["answer"]
        p = sw.wait(lambda: sw.goal(g2)["status"] == "done" and sw.goal(g2), 120, "goal 2 closed")
        assert p["answer"].startswith("(no answer"), p["answer"]
        im = sw.get("/immune")
        assert not any(h["quarantined"] for h in im["hosts"].values()), im["hosts"]
        assert all(h["score"] < 1.0 for h in im["hosts"].values()), im["hosts"]
        assert any(e.get("task") for e in im["poison"]) and any(
            e.get("goal") and not e.get("task") for e in im["poison"]), im["poison"]
        again = sw.ask("poison   PILL")              # adaptive memory: refused on sight
        a = sw.wait(lambda: sw.goal(again)["status"] == "done" and sw.goal(again), 5, "refused")
        assert "known poison" in a["answer"], a["answer"]
        g3 = sw.ask("still answering?")
        sw.wait(lambda: sw.goal(g3)["status"] == "done", 180, "a goal after the poison")
    finally:
        sw.stop()


def scenario_lapse_vs_leave(work: Path):
    """SIGTERM: agents leave on purpose (no charge). SIGKILL: the lapse is charged."""
    sw = Swarm(work, {"a": {"slots": 4}, "b": {"slots": 4}, "c": {"slots": 4}})
    try:
        sw.assembled()
        busy = lambda: [h for h in sw.procs if sw.agents_on(h)]   # noqa: E731
        term = sw.wait(busy, 30, "a host with agents")[0]
        sw.signal(term, signal.SIGTERM)
        sw.wait(lambda: not sw.agents_on(term), 3, f"{term}'s agents gone at once (left)")
        st = sw.get("/state")
        for g in st["goals"]:   # work it was holding went back without costing an attempt
            assert not g.get("attempts"), g
            assert not any(t.get("attempts") for t in g["tasks"]), g["tasks"]
        kill = sw.wait(lambda: [h for h in busy() if h != term], 30, "another host with agents")[0]
        sw.past_grace()
        sw.signal(kill, signal.SIGKILL)
        sw.wait(lambda: sw.get("/immune")["hosts"].get(kill, {}).get("score", 0) >= 0.9, 40,
                f"{kill} charged for the lapse")
        time.sleep(LAPSE_WAIT)
        im = sw.get("/immune")
        assert im["hosts"].get(term, {}).get("score", 0) == 0, im["hosts"]
        assert im["hosts"][kill]["score"] < 1.2, f"one kill, one lapse: {im['hosts'][kill]}"
        assert not im["hosts"][kill]["quarantined"]
    finally:
        sw.stop()


def scenario_mass_lapse(work: Path):
    """Every host freezes at once (as when the coordinator's own link drops): nobody blamed."""
    sw = Swarm(work, {"a": {"slots": 4}, "b": {"slots": 4}, "c": {"slots": 4}})
    try:
        sw.assembled()
        sw.past_grace()
        for n in sw.procs:
            sw.signal(n, signal.SIGSTOP)
        time.sleep(14)
        for n in sw.procs:
            sw.signal(n, signal.SIGCONT)
        sw.wait(lambda: any(e["type"] == "mass_lapse" for e in sw.get("/immune")["incidents"]),
                30, "mass lapse recognised")
        assert all(h["score"] == 0 for h in sw.get("/immune")["hosts"].values()), \
            sw.get("/immune")["hosts"]
        sw.assembled(60)
    finally:
        sw.stop()


def scenario_memory_pressure(work: Path):
    """Critical pressure: the host sheds its lowest agent and takes nothing new until ok."""
    sw = Swarm(work, {"a": {"slots": 4}, "b": {"slots": 4}, "c": {"slots": 4}})
    try:
        sw.assembled()
        sw.wait(lambda: sw.agents_on("a"), 30, "agents on a")
        n0 = len(sw.agents_on("a"))
        sw.pressure("a", "critical")
        sw.wait(lambda: len(sw.agents_on("a")) < n0, 20, "a shed an agent")
        sw.wait(lambda: sw.get("/state")["nodes"]["a"]["pressure"] == "critical", 15,
                "a reports critical pressure (its /node announce is every 5 s)")
        sw.pressure("a", "ok")
        sw.wait(lambda: sw.get("/state")["nodes"]["a"]["pressure"] == "ok", 15, "pressure ok")
        assert sw.get("/immune")["hosts"].get("a", {}).get("score", 0) == 0, "not an incident"
    finally:
        sw.stop()


def scenario_escalation_forced(work: Path):
    """Admin escalation: tier 2 grows to a quorum of carriers, tier 3 is stitched and takes
    over orchestration (tier 2 serves under it); relaxed, tier 3 retires and tier 2 leads
    again; escalated again, the stitched tier comes straight back."""
    sw = Swarm(work, {"a": {"slots": 5}, "b": {"slots": 5}, "c": {"slots": 4}})
    try:
        sw.assembled()
        assert "3" not in sw.get("/state")["ready"], "on-demand tier stays dormant"
        sw.admin("/control", {"escalate": "on"})
        sw.wait(lambda: "3" in sw.get("/state")["ready"], 60, "tier 3 stitched")
        sw.wait(lambda: sw.get("/state")["lead"] == 3, 30, "tier 3 leads")
        g = sw.goal(sw.ask("escalated question"))
        g = sw.wait(lambda: sw.goal(g["id"])["status"] == "done" and sw.goal(g["id"]), 60, "answer")
        assert "[stand-in tier3]" in g["answer"], g["answer"]
        assert "ESCALATED" in slime.render(sw.get("/state"))   # status copes with escalation
        sw.admin("/control", {"escalate": "off"})
        sw.wait(lambda: not sw.get("/state")["counts"].get("3"), 30, "tier 3 retired")
        sw.wait(lambda: sw.get("/state")["lead"] == 2, 30, "tier 2 leads again")
        g2 = sw.ask("relaxed question")
        g2 = sw.wait(lambda: sw.goal(g2)["status"] == "done" and sw.goal(g2), 60, "answer")
        assert "[stand-in tier2]" in g2["answer"], g2["answer"]
        slime.render(sw.get("/state"))
        t0 = time.time()
        sw.admin("/control", {"escalate": "on"})
        sw.wait(lambda: sw.get("/state")["lead"] == 3, 30, "tier 3 back")
        assert time.time() - t0 < 15, "re-escalation re-embodies, no new quorum needed"
        sw.admin("/control", {"escalate": "off"})
        sw.wait(lambda: not sw.get("/control")["escalation"]["on"], 10, "off")
        sw.admin("/control", {"escalate": "auto"})   # no disagreement: auto must stay calm
        time.sleep(6)
        assert not sw.get("/control")["escalation"]["on"], sw.get("/control")["escalation"]
    finally:
        sw.stop()


def scenario_escalation_auto(work: Path):
    """Reviewers keep rejecting answers: the swarm escalates by itself, and relaxes once
    the disagreement has aged out of the window and the hold has passed."""
    m = json.loads(json.dumps(STANDIN))
    m["control"].update(escalate_min_reviews=3, escalate_window=60, escalate_hold=15)
    env = {"SLIME_FAULT": "disagree"}
    sw = Swarm(work, {"a": {"slots": 5, "env": env}, "b": {"slots": 5, "env": env},
                      "c": {"slots": 4, "env": env}}, manifest=m)
    try:
        sw.assembled()
        ids = [sw.ask(f"contested {i}") for i in range(2)]
        sw.wait(lambda: (sw.get("/control")["escalation"] or {}).get("on"), 90, "escalated")
        why = sw.get("/control")["escalation"]["why"]
        assert "disagreement" in why, why
        sw.wait(lambda: sw.get("/state")["lead"] == 3, 60, "tier 3 leads")
        sw.wait(lambda: all(sw.goal(i)["status"] == "done" for i in ids), 90, "goals done")
        sw.wait(lambda: not sw.get("/control")["escalation"]["on"], 150, "relaxed")
        sw.wait(lambda: sw.get("/state")["lead"] == 2, 30, "tier 2 leads again")
    finally:
        sw.stop()


def scenario_coordinator_failover(work: Path):
    """Primary dies; the standby (replicating every 3 s) is promoted by hand and carries
    on with the same agents, ledger and settings; the old primary, restarted, sees the newer
    epoch and comes back as standby."""
    sw = Swarm(work, {"a": {"slots": 4}, "b": {"slots": 4}, "c": {"slots": 4}}, standby=True)
    try:
        sw.assembled()
        g0 = sw.ask("before failover")
        sw.wait(lambda: sw.goal(g0)["status"] == "done", 60, "answered by the primary")
        sw.admin("/control", {"down_after": 77})              # a setting that must survive
        time.sleep(5)                                          # a replication round
        n_agents = len(sw.get("/state")["agents"])
        sw.coord.kill()
        sw.coord.wait(5)
        sw.url = sw.standby_url
        r = sw.admin("/promote", {})
        assert r["role"] == "primary" and r["epoch"] >= 2, r
        st = sw.wait(lambda: sw.get("/state"), 10, "promoted standby answers")
        assert len(st["agents"]) >= n_agents - 1, (len(st["agents"]), n_agents)
        assert sw.get("/control")["control"]["down_after"] == 77
        g1 = sw.ask("after failover")
        sw.wait(lambda: sw.goal(g1)["status"] == "done", 90, "answered by the new primary")
        sw.coord = sw.start_coord(sw.primary_url, "primary", sw.standby_url, "state.json")
        old = sw.primary_url + "/role"
        sw.wait(lambda: json.load(urllib.request.urlopen(old, timeout=3))["role"] == "standby",
                20, "old primary came back as standby")
        g2 = sw.ask("after the old primary returned")
        sw.wait(lambda: sw.goal(g2)["status"] == "done", 90, "still answering, no split brain")
    finally:
        sw.stop()


UNITS = [unit_screen, unit_quarantine_and_decay, unit_mass_lapse, unit_sleep_excused, unit_poison_refund, unit_antibodies,
         unit_old_state_upgrade, unit_snapshot_order, unit_slow_disk_never_blocks,
         unit_report_dedupe, unit_scale_in_order, unit_speed_placement, unit_lead_relocation, unit_canaries,
         unit_avoid_only_with_alternatives, unit_no_doubled_orchestrators,
         unit_withdraw_and_no_evict]
SCENARIOS = [scenario_assemble_and_answer, scenario_control_burst,
             scenario_bad_output_quarantine, scenario_inflammation, scenario_poison_task,
             scenario_lapse_vs_leave, scenario_mass_lapse, scenario_memory_pressure,
             scenario_escalation_forced, scenario_escalation_auto,
             scenario_coordinator_failover]


def main() -> int:
    args = [a for a in sys.argv[1:] if not a.startswith("-")]
    keep = "-k" in sys.argv
    tests = [t for t in UNITS + SCENARIOS
             if not args or any(a in t.__name__ for a in args)]
    root = Path(tempfile.mkdtemp(prefix="slime-selftest-"))
    failed = 0
    for t in tests:
        t0 = time.time()
        try:
            if t.__name__.startswith("scenario_"):
                work = root / t.__name__
                work.mkdir()
                t(work)
            else:
                with open(os.devnull, "w") as quiet:   # coord prints its immune log lines
                    out, sys.stdout = sys.stdout, quiet
                    try:
                        t()
                    finally:
                        sys.stdout = out
            print(f"PASS {t.__name__} ({time.time() - t0:.0f}s)", flush=True)
        except Exception:
            failed += 1
            print(f"FAIL {t.__name__} ({time.time() - t0:.0f}s)\n"
                  + "".join("     " + ln for ln in traceback.format_exc().splitlines(True)),
                  flush=True)
    print(f"\n{len(tests) - failed}/{len(tests)} passed" + (f"; logs in {root}" if keep or failed
                                                             else ""))
    if not keep and not failed:
        shutil.rmtree(root, ignore_errors=True)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
