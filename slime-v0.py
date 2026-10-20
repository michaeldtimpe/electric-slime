#!/usr/bin/env python3
"""
electric-slime v0 — a self-assembling model scaffold.

A pool of "containers" (here: processes) share one arena (a directory). Each base
agent carries a shard of the next, larger model. When a quorum of shards exists and
resources permit, one agent is elected to stitch the next tier and invoke it; lower
tiers then trim themselves to free resources, while always leaving enough shard files
on disk to rebuild (the rebuild floor) and enough live replicas to keep serving (the
serve floor). The top tier uses the lower tiers as subagents.

v0 is deliberately model-agnostic: a "shard" is a deterministic content-hashed blob
and a "model" is the verified concatenation of its shards. Swap those two functions
(make_shard_bytes / stitch) for real GGUF shards + llama.cpp and the scaffold is
unchanged.

Commands:
    slime.py seed    --arena DIR        initialize arena from manifest/genome, launch tier-0 pool
    slime.py agent   --arena DIR --tier N   (internal) run one agent process
    slime.py status  --arena DIR        one-shot census
    slime.py watch   --arena DIR        live census until the top tier reports a result
    slime.py stop    --arena DIR        signal all agents to exit
"""
from __future__ import annotations
import argparse, contextlib, fcntl, hashlib, json, os, random, signal, subprocess, sys, time
from pathlib import Path

TICK = 0.4          # agent loop period (s)
HB_TIMEOUT = 3.0    # a member is dead if its heartbeat is older than this (s)
HERE = Path(__file__).resolve().parent


# --------------------------------------------------------------------------- utils
def now() -> float:
    return time.time()


def log(arena: Path, msg: str) -> None:
    line = f"{time.strftime('%H:%M:%S')} {msg}\n"
    p = arena / "events.log"
    with open(p, "a") as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        f.write(line)
        fcntl.flock(f, fcntl.LOCK_UN)


@contextlib.contextmanager
def arena_lock(arena: Path):
    """Global mutex for the small critical sections that touch state.json."""
    lp = arena / "locks" / "arena.lock"
    lp.parent.mkdir(parents=True, exist_ok=True)
    f = open(lp, "w")
    try:
        fcntl.flock(f, fcntl.LOCK_EX)
        yield
    finally:
        fcntl.flock(f, fcntl.LOCK_UN)
        f.close()


def read_json(p: Path, default=None):
    try:
        return json.loads(p.read_text())
    except FileNotFoundError:
        return default


def write_json_atomic(p: Path, obj) -> None:
    tmp = p.with_suffix(p.suffix + f".tmp.{os.getpid()}")
    tmp.write_text(json.dumps(obj))
    os.replace(tmp, p)


# ------------------------------------------------------------------- shards / models
def make_shard_bytes(provides_tier: int, index: int) -> bytes:
    """Deterministic stand-in for a real weight shard. Identical across replicas."""
    seed = hashlib.sha256(f"slime-v0:tier{provides_tier}:shard{index}".encode()).digest()
    return seed * 64  # 2 KiB, content-addressed


def shard_sha(provides_tier: int, index: int) -> str:
    return hashlib.sha256(make_shard_bytes(provides_tier, index)).hexdigest()


def stitch(shard_blobs: list[bytes]) -> bytes:
    """Stand-in for assembling weights. Real version: map sharded tensors into a graph."""
    return b"".join(shard_blobs)


# ------------------------------------------------------------------------- the state
# state.json (guarded by arena_lock) is the authoritative live view:
#   { "agents": { id: {tier, cost, last_hb, status, provides, shard_index} } }
# status in {"alive","retire"}; dead agents are removed by reap().
def load_state(arena: Path) -> dict:
    return read_json(arena / "state.json", {"agents": {}})


def save_state(arena: Path, st: dict) -> None:
    write_json_atomic(arena / "state.json", st)


def used_units(st: dict) -> int:
    return sum(a["cost"] for a in st["agents"].values() if a["status"] != "dead")


def alive_by_tier(st: dict) -> dict[int, list[str]]:
    out: dict[int, list[str]] = {}
    for aid, a in st["agents"].items():
        if a["status"] == "alive" and now() - a["last_hb"] < HB_TIMEOUT:
            out.setdefault(a["tier"], []).append(aid)
    return out


def reap(arena: Path, st: dict) -> bool:
    """Remove agents whose heartbeat went stale. Returns True if anything changed."""
    dead = [aid for aid, a in st["agents"].items()
            if now() - a["last_hb"] >= HB_TIMEOUT]
    for aid in dead:
        del st["agents"][aid]
    return bool(dead)


# ----------------------------------------------------------------------------- tiers
def tier_spec(manifest: dict, tier: int) -> dict | None:
    for t in manifest["tiers"]:
        if t["tier"] == tier:
            return t
    return None


# =============================================================================
# AGENT
# =============================================================================
class Agent:
    def __init__(self, arena: Path, tier: int):
        self.arena = arena
        self.tier = tier
        self.manifest = read_json(arena / "manifest.json")
        self.genome = read_json(arena / "genome.json")
        self.spec = tier_spec(self.manifest, tier)
        self.id = f"t{tier}-{os.getpid()}-{random.randint(0, 9999):04d}"
        self.shard_index: int | None = None
        self.provides = self.spec.get("provides")
        self._left = False

    # -- membership / resources -------------------------------------------------
    def join(self) -> bool:
        """Claim resources FCFS and pick a shard index. False if no resources."""
        cost = self.spec["cost"]
        with arena_lock(self.arena):
            st = load_state(self.arena)
            reap(self.arena, st)
            if used_units(st) + cost > self.manifest["resource_total"]:
                save_state(self.arena, st)
                return False
            # pick the next-tier shard index with the fewest live holders (balance redundancy)
            if self.provides:
                n = self.provides["n_shards"]
                holders = {i: 0 for i in range(n)}
                for a in st["agents"].values():
                    if a["status"] == "alive" and a.get("shard_index") is not None \
                            and a.get("provides_tier") == self.provides["tier"]:
                        holders[a["shard_index"]] = holders.get(a["shard_index"], 0) + 1
                self.shard_index = min(holders, key=lambda i: holders[i])
            st["agents"][self.id] = {
                "tier": self.tier, "cost": cost, "last_hb": now(), "status": "alive",
                "provides_tier": self.provides["tier"] if self.provides else None,
                "shard_index": self.shard_index,
            }
            save_state(self.arena, st)
        return True

    def deposit_shard(self) -> None:
        """Write this agent's shard of the next tier into the arena (idempotent)."""
        if not self.provides:
            return
        pt, idx = self.provides["tier"], self.shard_index
        d = self.arena / "shards"
        d.mkdir(exist_ok=True)
        f = d / f"tier{pt}.shard{idx}.bin"
        if not f.exists():
            tmp = f.with_suffix(".tmp." + str(os.getpid()))
            tmp.write_bytes(make_shard_bytes(pt, idx))
            os.replace(tmp, f)
            log(self.arena, f"[{self.id}] deposited shard {idx} for tier{pt}")

    def heartbeat(self) -> str:
        """Refresh heartbeat; return our current status ('alive'/'retire'/'gone')."""
        with arena_lock(self.arena):
            st = load_state(self.arena)
            reap(self.arena, st)
            me = st["agents"].get(self.id)
            if me is None:
                save_state(self.arena, st)
                return "gone"
            me["last_hb"] = now()
            status = me["status"]
            save_state(self.arena, st)
        return status

    def leave(self) -> None:
        if self._left:
            return
        with arena_lock(self.arena):
            st = load_state(self.arena)
            st["agents"].pop(self.id, None)
            save_state(self.arena, st)
        log(self.arena, f"[{self.id}] left (freed {self.spec['cost']}u)")

    # -- promotion: stitch the next tier ---------------------------------------
    def shard_files(self, provides_tier: int) -> int:
        d = self.arena / "shards"
        return len(list(d.glob(f"tier{provides_tier}.shard*.bin"))) if d.exists() else 0

    def try_promote(self) -> None:
        """If a quorum of shards for the next tier exists, elect ourselves assembler."""
        if not self.provides:
            return
        nxt = self.provides["tier"]
        nspec = tier_spec(self.manifest, nxt)
        if nspec is None:
            return
        ready = self.arena / "models" / f"tier{nxt}" / "READY"
        if ready.exists():
            return
        k = self.provides["k"]
        with arena_lock(self.arena):
            st = load_state(self.arena)
            reap(self.arena, st)
            alive = alive_by_tier(st)
            save_state(self.arena, st)
        if len(alive.get(self.tier, [])) < k:
            return
        if self.shard_files(nxt) < k:
            return
        # elect: exactly one assembler via O_EXCL
        lockp = self.arena / "locks" / f"assemble.tier{nxt}.lock"
        try:
            fd = os.open(lockp, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            os.write(fd, self.id.encode())
            os.close(fd)
        except FileExistsError:
            return  # someone else owns this transition
        try:
            self.assemble(nxt, nspec, k)
        except Exception as e:  # leave the lock as a done/aborted marker in v0
            log(self.arena, f"[{self.id}] ASSEMBLE tier{nxt} FAILED: {e!r}")

    def assemble(self, nxt: int, nspec: dict, k: int) -> None:
        log(self.arena, f"[{self.id}] elected assembler for tier{nxt} ({nspec['label']})")
        # 1. verify + stitch
        blobs = []
        for i in range(k):
            f = self.arena / "shards" / f"tier{nxt}.shard{i}.bin"
            data = f.read_bytes()
            if hashlib.sha256(data).hexdigest() != shard_sha(nxt, i):
                raise ValueError(f"shard {i} hash mismatch")
            blobs.append(data)
        model = stitch(blobs)
        mdir = self.arena / "models" / f"tier{nxt}"
        mdir.mkdir(parents=True, exist_ok=True)
        (mdir / "model.bin").write_bytes(model)
        (mdir / "model.sha").write_text(hashlib.sha256(model).hexdigest())
        log(self.arena, f"[{self.id}] stitched tier{nxt}: {len(blobs)} shards verified, "
                        f"{len(model)} bytes, sha {hashlib.sha256(model).hexdigest()[:12]}")
        # 2. invoke. READY is written FIRST: it is the signal that tells every lower
        #    tier it has been superseded, so they self-retire to their serve floor and
        #    free the resources these new replicas need. The spawns retry-join until
        #    that room appears. Shard files stay on disk = the rebuild floor.
        (mdir / "READY").write_text(str(now()))
        for _ in range(nspec["target"]):
            self.spawn_agent(nxt)
        log(self.arena, f"[{self.id}] invoked tier{nxt} ×{nspec['target']} (lower tiers will trim to floor)")

    def superseded(self) -> bool:
        """True if any higher tier has been assembled — our cue to trim to the floor."""
        for t in self.manifest["tiers"]:
            if t["tier"] > self.tier and (self.arena / "models" / f"tier{t['tier']}" / "READY").exists():
                return True
        return False

    def maybe_self_retire(self) -> bool:
        """Right-to-die lease: if superseded and above serve_floor, atomically remove
        self and exit. The lock serializes the count so we never drop below the floor."""
        if not self.superseded():
            return False
        floor = self.spec["serve_floor"]
        with arena_lock(self.arena):
            st = load_state(self.arena)
            reap(self.arena, st)
            alive = alive_by_tier(st)
            mine = [a for a in alive.get(self.tier, []) if st["agents"][a]["status"] == "alive"]
            if self.id in mine and len(mine) > floor:
                del st["agents"][self.id]
                save_state(self.arena, st)
                self._left = True
                log(self.arena, f"[{self.id}] self-retire: tier{self.tier} superseded, "
                                f"{len(mine)}>{floor}, freed {self.spec['cost']}u")
                return True
            save_state(self.arena, st)
        return False

    def spawn_agent(self, tier: int) -> None:
        logp = self.arena / "logs"
        logp.mkdir(exist_ok=True)
        out = open(logp / f"spawn-tier{tier}-{random.randint(0,9999):04d}.log", "a")
        subprocess.Popen(
            [sys.executable, str(HERE / "slime.py"), "agent",
             "--arena", str(self.arena), "--tier", str(tier)],
            stdout=out, stderr=subprocess.STDOUT, start_new_session=True,
        )

    # -- directives (the "DNA") ------------------------------------------------
    def directives_for_me(self) -> list[dict]:
        return [d for d in self.genome["directives"] if d["complexity"] <= self.tier]

    def run_directive_tick(self) -> None:
        """Execute the highest-complexity directive licensed to this tier for one tick."""
        action = self.directives_for_me()[-1]["action"]
        if action == "orchestrate":
            self.orchestrate_tick()
        else:  # "serve": claim one work task if any
            self.serve_tick()

    def serve_tick(self) -> None:
        tdir = self.arena / "tasks"
        if not tdir.exists():
            return
        for todo in sorted(tdir.glob("*.todo")):
            doing = todo.with_suffix(f".doing-{self.id}")
            try:
                os.rename(todo, doing)  # atomic claim; loser raises
            except (FileNotFoundError, OSError):
                continue
            task = read_json(doing)
            if task["op"] == "square":
                task["result"] = task["x"] * task["x"]
            task["worker"] = self.id
            done = todo.with_suffix(".done")
            write_json_atomic(done, task)
            doing.unlink(missing_ok=True)
            log(self.arena, f"[{self.id}] computed task {task['id']} -> {task['result']}")
            return  # one unit of work per tick

    def orchestrate_tick(self) -> None:
        tdir = self.arena / "tasks"
        tdir.mkdir(exist_ok=True)
        goal = self.genome["goal"]
        posted_flag = self.arena / "models" / f"tier{self.tier}" / "POSTED"
        if not posted_flag.exists():
            for i, x in enumerate(goal["inputs"]):
                write_json_atomic(tdir / f"task-{i}.todo", {"id": i, "op": goal["op"], "x": x})
            posted_flag.write_text(str(now()))
            log(self.arena, f"[{self.id}] (top) posted {len(goal['inputs'])} '{goal['op']}' tasks for subagents")
            return
        done = sorted(tdir.glob("*.done"))
        if len(done) == len(goal["inputs"]):
            results = {}
            for f in done:
                t = read_json(f)
                results[t["x"]] = t["result"]
            resp = self.arena / "RESULT.json"
            if not resp.exists():
                write_json_atomic(resp, {"op": goal["op"], "results": results,
                                         "by": self.id, "ts": now()})
                log(self.arena, f"[{self.id}] (top) GOAL COMPLETE: {results}")

    # -- main loop -------------------------------------------------------------
    def run(self) -> None:
        # join (retry while resources are momentarily full)
        for _ in range(30):
            if self.arena.joinpath("SHUTDOWN").exists():
                return
            if self.join():
                break
            time.sleep(TICK)
        else:
            log(self.arena, f"[{self.id}] could not join (no resources) — exiting")
            return
        self.deposit_shard()
        log(self.arena, f"[{self.id}] joined tier{self.tier} ({self.spec['label']}), "
                        f"shard_index={self.shard_index}")
        try:
            while True:
                if self.arena.joinpath("SHUTDOWN").exists():
                    break
                status = self.heartbeat()
                if status in ("retire", "gone"):
                    break
                if self.maybe_self_retire():
                    break
                self.try_promote()
                self.run_directive_tick()
                time.sleep(TICK)
        finally:
            self.leave()


# =============================================================================
# CONTROL PLANE
# =============================================================================
def cmd_seed(args) -> None:
    arena = Path(args.arena).resolve()
    if arena.exists() and any(arena.iterdir()) and not args.force:
        sys.exit(f"arena {arena} not empty (use --force to reset)")
    for sub in ("locks", "shards", "models", "tasks", "logs"):
        (arena / sub).mkdir(parents=True, exist_ok=True)
    # copy manifest + genome into the arena (the shared medium)
    (arena / "manifest.json").write_text((HERE / "manifest.json").read_text())
    (arena / "genome.json").write_text((HERE / "genome.json").read_text())
    arena.joinpath("SHUTDOWN").unlink(missing_ok=True)
    save_state(arena, {"agents": {}})
    arena.joinpath("events.log").write_text("")
    manifest = read_json(arena / "manifest.json")
    t0 = tier_spec(manifest, 0)
    log(arena, f"=== seed: launching {t0['seed']}× tier0 ({t0['label']}), "
               f"resource_total={manifest['resource_total']} ===")
    for _ in range(t0["seed"]):
        out = open(arena / "logs" / f"seed-{random.randint(0,99999):05d}.log", "a")
        subprocess.Popen(
            [sys.executable, str(HERE / "slime.py"), "agent",
             "--arena", str(arena), "--tier", "0"],
            stdout=out, stderr=subprocess.STDOUT, start_new_session=True,
        )
    print(f"seeded arena at {arena} with {t0['seed']} tier0 agents")
    print(f"watch it:  python3 {HERE/'slime.py'} watch --arena {arena}")


def census(arena: Path) -> dict:
    manifest = read_json(arena / "manifest.json")
    with arena_lock(arena):
        st = load_state(arena)
        reap(arena, st)
        save_state(arena, st)
    alive = alive_by_tier(st)
    rows = []
    for t in manifest["tiers"]:
        tier = t["tier"]
        n = len(alive.get(tier, []))
        pv = t.get("provides")
        shards = 0
        if pv:
            d = arena / "shards"
            shards = len(list(d.glob(f"tier{pv['tier']}.shard*.bin"))) if d.exists() else 0
        ready = (arena / "models" / f"tier{tier}" / "READY").exists() or tier == 0
        rows.append({"tier": tier, "label": t["label"], "live": n, "cost": t["cost"],
                     "shards_for_next": shards, "ready": ready})
    return {"manifest": manifest, "rows": rows, "used": used_units(st),
            "total": manifest["resource_total"],
            "result": read_json(arena / "RESULT.json")}


def render(c: dict) -> str:
    lines = [f"resources: {c['used']}/{c['total']}u used"]
    for r in c["rows"]:
        bar = "#" * r["live"]
        extra = f"  shards→next:{r['shards_for_next']}" if r["shards_for_next"] else ""
        rdy = "" if r["ready"] else "  (not assembled)"
        lines.append(f"  tier{r['tier']} {r['label']:>5} live:{r['live']:2d} [{bar:<8}] "
                     f"{r['live']*r['cost']:2d}u{extra}{rdy}")
    if c["result"]:
        lines.append(f"  RESULT: {c['result']['results']}")
    return "\n".join(lines)


def cmd_status(args) -> None:
    print(render(census(Path(args.arena).resolve())))


def cmd_watch(args) -> None:
    arena = Path(args.arena).resolve()
    t_end = now() + args.timeout
    last = None
    while now() < t_end:
        c = census(arena)
        frame = render(c)
        if frame != last:
            print("\033[2J\033[H" + time.strftime("%H:%M:%S") + "\n" + frame, flush=True)
            last = frame
        if c["result"]:
            print("\n-- top tier reported a result; scaffold is fully assembled --")
            return
        time.sleep(0.5)
    print("\n-- watch timeout --")


def cmd_stop(args) -> None:
    arena = Path(args.arena).resolve()
    arena.joinpath("SHUTDOWN").write_text(str(now()))
    print("SHUTDOWN flag set; agents will exit within a few ticks")


def cmd_agent(args) -> None:
    Agent(Path(args.arena).resolve(), args.tier).run()


def main() -> None:
    ap = argparse.ArgumentParser(prog="slime")
    sub = ap.add_subparsers(dest="cmd", required=True)
    for name in ("seed", "status", "stop"):
        p = sub.add_parser(name); p.add_argument("--arena", required=True)
    sub._name_parser_map["seed"].add_argument("--force", action="store_true")
    pa = sub.add_parser("agent"); pa.add_argument("--arena", required=True); pa.add_argument("--tier", type=int, required=True)
    pw = sub.add_parser("watch"); pw.add_argument("--arena", required=True); pw.add_argument("--timeout", type=float, default=90.0)
    args = ap.parse_args()
    {"seed": cmd_seed, "agent": cmd_agent, "status": cmd_status,
     "watch": cmd_watch, "stop": cmd_stop}[args.cmd](args)


if __name__ == "__main__":
    main()
