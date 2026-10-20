#!/usr/bin/env python3
"""
electric-slime node client — talks to the coordinator (coord.py) over HTTP.

A "runner" is a generic slot. It repeatedly asks the coordinator what tier to embody
(first-come-first-served), becomes that agent until it retires, then asks again. Open
movement + FCFS resources, exactly: idle runners claim whichever tier the swarm wants.

Embodying a tier with a real model (v1): carry this agent's shard of the next tier (fetched
from the depot by sha), obtain the tier's own model (the base model whole; a larger one
stitched from its shards and checked against the publisher's SHA-256), then run it with a
native llama-server and act on the genome: lower tiers serve subtasks, the top tier
orchestrates goals through them. Tiers without a `model` are stand-ins (v0 behaviour).

    slime.py seed   --coord URL [--manifest F] [--genome F]   # push manifest+genome
    slime.py pool   --coord URL --slots N [--node NAME]       # N runner slots (one host)
    slime.py runner --coord URL [--node NAME]                 # one slot
    slime.py ask    --coord URL "question" [--wait]           # post a goal
    slime.py status --coord URL
    slime.py watch  --coord URL [--timeout S]
    slime.py metrics --coord URL                               # load, latency, review quality
    slime.py promote --coord URL --admin-token-file F          # make a standby the primary
    slime.py control --coord URL [--set K=V ... --admin-token-file F]   # the scaling loop
    slime.py immune  --coord URL [--pardon HOST] [--set K=V] [--admin-token-file F]  # health
    slime.py report  --coord URL [--hours 8]                   # what happened, from events.jsonl

--coord may list a primary and a standby: http://A:8700,http://B:8700.

Environment: SLIME_CACHE (model/shard cache), LLAMA_SERVER (binary), SLIME_THREADS,
SLIME_CAPACITY_MB (this host's memory for agents; default 80% of RAM or cgroup limit),
SLIME_GPU_LAYERS (default 0: CPU only), SLIME_KEY_FILE (this host's signing key; default
next to the cache), SLIME_MAX_TIER (highest tier this host may take), SLIME_PREFETCH (default 1: warm the
cache with every model this host may run while idle).
"""
from __future__ import annotations
import argparse, base64, hashlib, hmac, json, os, platform, random, signal, socket, subprocess, sys, threading, time
from http.client import HTTPConnection, HTTPException
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import urlsplit

TICK = 0.5
HERE = Path(__file__).resolve().parent
CACHE = Path(os.environ.get("SLIME_CACHE", Path.home() / ".cache" / "electric-slime"))
LLAMA_SERVER = os.environ.get("LLAMA_SERVER", "llama-server")
GPU_LAYERS = os.environ.get("SLIME_GPU_LAYERS", "0")
MAX_TIER = int(os.environ.get("SLIME_MAX_TIER", "99"))   # highest tier this host may embody


def log(msg: str) -> None:
    print(f"{time.strftime('%H:%M:%S')} {msg}", flush=True)


# --- stand-in model primitives ------------------------------------------------
def make_shard_bytes(provides_tier: int, index: int) -> bytes:
    seed = hashlib.sha256(f"slime-v0:tier{provides_tier}:shard{index}".encode()).digest()
    return seed * 64


def shard_sha(provides_tier: int, index: int) -> str:
    return hashlib.sha256(make_shard_bytes(provides_tier, index)).hexdigest()


def stitch(blobs: list[bytes]) -> bytes:
    return b"".join(blobs)


# --- coordinator client -------------------------------------------------------
class Standby(ConnectionError):
    """The coordinator answered, but as a standby: try the other one."""


SIGNER = {"host": None, "key": None}   # this host's identity, once start_host loads its key


def sign_headers(method: str, path: str, body: bytes) -> dict:
    if not SIGNER["key"]:
        return {}
    ts = f"{time.time():.3f}"
    msg = f"{method}\n{path}\n{ts}\n{hashlib.sha256(body).hexdigest()}".encode()
    return {"X-Slime-Host": SIGNER["host"], "X-Slime-Time": ts,
            "X-Slime-Sig": hmac.new(SIGNER["key"], msg, hashlib.sha256).hexdigest()}


class Coord:
    """The coordinator, which may be a primary and a standby (`URL1,URL2`): calls go to the
    one that last answered as primary and move on when it is unreachable or a standby.
    One keep-alive connection per thread and coordinator: Docker Desktop's network proxy
    now and then swallowed new connections, and a hung call must stay well inside the
    coordinator's heartbeat window. Runner calls are signed when this host has a key."""
    TIMEOUT = 3.0

    def __init__(self, url: str):
        self.urls = [u.strip().rstrip("/") for u in url.split(",") if u.strip()]
        self.active = 0
        self.local = threading.local()

    @property
    def url(self) -> str:
        return self.urls[self.active]

    @property
    def host(self) -> str:
        return urlsplit(self.url).hostname

    def _request(self, method: str, path: str, body: bytes | None = None,
                 extra: dict | None = None) -> dict:
        last: Exception | None = None
        for _ in self.urls:
            base = self.url
            try:
                return self._request_one(base, method, path, body, extra)
            except HTTPError:
                raise                       # a live primary said no: that's the answer
            except (OSError, HTTPException) as e:
                last = e
                if len(self.urls) > 1 and self.url == base:
                    self.active = (self.active + 1) % len(self.urls)
        raise last if isinstance(last, OSError) else ConnectionError(f"{path}: {last!r}")

    def _request_one(self, base: str, method: str, path: str, body: bytes | None,
                     extra: dict | None) -> dict:
        conns = self.local.__dict__.setdefault("conns", {})
        u = urlsplit(base)
        while True:
            headers = {"Content-Type": "application/json"} if body is not None else {}
            if method == "POST":
                headers.update(sign_headers(method, path, body or b""))
            headers.update(extra or {})
            conn = conns.get(base)
            reused = conn is not None
            if not reused:
                conn = conns[base] = HTTPConnection(u.hostname, u.port or 80, timeout=self.TIMEOUT)
            try:
                conn.request(method, path, body=body, headers=headers)
                r = conn.getresponse()
                data = r.read()
            except (OSError, HTTPException) as e:
                conn.close()
                conns.pop(base, None)
                # a reused connection the server already closed never delivered the request,
                # so that alone is safe to resend; anything else (a timeout) might have landed
                if reused and isinstance(e, ConnectionError):
                    continue
                raise
            if r.status == 503:
                raise Standby(f"{base} is a standby")
            if r.status >= 400:
                raise HTTPError(base + path, r.status, f"{r.reason}: {data[:200]!r}", r.headers, None)
            return json.loads(data)

    def post(self, path: str, obj: dict, headers: dict | None = None) -> dict:
        return self._request("POST", path, json.dumps(obj).encode(), headers)

    def get(self, path: str) -> dict:
        return self._request("GET", path)

    def download(self, sha: str, dest: Path, host: str = "", depot: bool = True) -> str:
        """Fetch a blob into `dest`: from peers that hold it first (agents carry shards, so
        they hand them on), the depot last. Verified by sha whatever the source, so a bad
        peer costs time, never correctness. Returns where it came from."""
        try:
            peers = self.get(f"/where?sha={sha}&host={host}").get("peers", [])
        except (OSError, ValueError):
            peers = []
        # one entry per host (its addresses, fastest first); hosts that delivered fast before
        # go first, unmeasured ones mid-pack, so a 2 MB/s wifi node isn't picked by luck
        random.shuffle(peers)
        peers.sort(key=lambda urls: -PEER_RATES.get(urls[0], UNMEASURED_RATE))
        sources = [(urls[0], u) for urls in peers for u in urls]
        if depot:
            sources.append((self.url, self.url))
        for key, src in sources:
            t0 = time.time()
            try:
                fetch_verified(src, sha, dest)
            except (OSError, HTTPException, ValueError) as e:
                log(f"blob {sha[:12]} from {src} failed ({type(e).__name__}: {e}); next source")
                if isinstance(e, ValueError) and src != self.url:
                    try:   # it served bytes that aren't what it claimed: tell the immune system
                        self.post("/report", {"kind": "bad_blob", "peer": src, "sha": sha,
                                              "host": host})
                    except (OSError, ValueError):
                        pass
                continue
            rate = dest.stat().st_size / max(time.time() - t0, 0.01)
            PEER_RATES[key] = 0.5 * PEER_RATES.get(key, rate) + 0.5 * rate
            return "depot" if src == self.url else urlsplit(src).hostname
        raise ConnectionError(f"blob {sha[:12]}: no source could deliver it")


PEER_RATES: dict[str, float] = {}   # bytes/s each source delivered to this host (smoothed)
UNMEASURED_RATE = 10e6


def fetch_verified(base: str, sha: str, dest: Path) -> None:
    """Stream base/blob/<sha> to `dest`, hashing on the way; atomic on success. A partial
    `<dest>.part` from an earlier attempt (any source, any restart) is resumed with a Range
    request: content is addressed by sha, so every source's bytes are the same bytes."""
    u = urlsplit(base)
    part = dest.with_name(dest.name + ".part")
    have = part.stat().st_size if part.exists() else 0
    conn = HTTPConnection(u.hostname, u.port or 80, timeout=5)   # a dead peer fails fast
    try:
        conn.connect()
        conn.sock.settimeout(30)   # a live one gets patience mid-transfer
        conn.request("GET", f"/blob/{sha}", headers={"Range": f"bytes={have}-"} if have else {})
        r = conn.getresponse()
        if r.status == 416:   # the part is already whole (or bigger): start over
            r.read()
            part.unlink()
            raise ConnectionError(f"blob {sha[:12]}: stale partial discarded")
        if r.status not in (200, 206):
            raise HTTPError(base, r.status, r.reason, r.headers, None)
        h = hashlib.sha256()
        if r.status == 206:
            with open(part, "rb") as f:
                while chunk := f.read(1 << 22):
                    h.update(chunk)
        else:
            have = 0
        got = 0
        with open(part, "ab" if r.status == 206 else "wb") as f:
            while chunk := r.read(1 << 20):
                h.update(chunk)
                f.write(chunk)
                got += len(chunk)
        want = int(r.getheader("Content-Length") or -1)
        if got != want:   # the source went away mid-transfer: keep the part, resume later
            raise ConnectionError(f"blob {sha[:12]} truncated at {have + got} bytes")
        if h.hexdigest() != sha:
            part.unlink()
            raise ValueError(f"blob {sha[:12]} failed verification")
        os.replace(part, dest)
    finally:
        conn.close()


# --- peer serving: this host's cache, read-only, by sha -----------------------
PEER_PORT = int(os.environ.get("SLIME_PEER_PORT", "8701"))


def send_blob_file(h: BaseHTTPRequestHandler, p: Path) -> None:
    """Serve a whole file, or its tail for `Range: bytes=N-` (a resumed download)."""
    size, start = p.stat().st_size, 0
    rng = h.headers.get("Range", "")
    if rng.startswith("bytes=") and rng.endswith("-") and rng[6:-1].isdigit():
        start = int(rng[6:-1])
        if start >= size:
            h.send_response(416)
            h.send_header("Content-Range", f"bytes */{size}")
            h.send_header("Content-Length", "0")
            h.end_headers()
            return
    h.send_response(206 if start else 200)
    h.send_header("Content-Type", "application/octet-stream")
    if start:
        h.send_header("Content-Range", f"bytes {start}-{size - 1}/{size}")
    h.send_header("Content-Length", str(size - start))
    h.end_headers()
    with open(p, "rb") as f:
        f.seek(start)
        try:
            while chunk := f.read(1 << 20):
                h.wfile.write(chunk)
        except (BrokenPipeError, ConnectionResetError):
            h.close_connection = True   # the fetcher gave up; it resumes elsewhere


class BlobHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    timeout = 60

    def log_message(self, *a):
        pass

    def do_GET(self):
        name = self.path[len("/blob/"):] if self.path.startswith("/blob/") else ""
        if len(name) == 64 and all(ch in "0123456789abcdef" for ch in name):
            for p in (CACHE / "blobs" / name, CACHE / "models" / f"{name}.gguf"):
                if p.is_file():
                    return send_blob_file(self, p)
        self.send_response(404)
        self.send_header("Content-Length", "0")
        self.end_headers()


def advertise_ips(coord_host: str) -> list[str]:
    """Addresses peers may dial, fastest first: the LAN one (plain TCP), then the tailnet
    one (reachable from every node, including macOS launchd jobs, which may not open LAN
    connections and fail those instantly). A UDP connect sends nothing."""
    out = []
    for target in ("192.168.1.1", coord_host, "100.100.100.100"):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
                s.connect((target, 53))
                ip = s.getsockname()[0]
        except OSError:
            continue
        if ip not in out and not ip.startswith("127."):
            out.append(ip)
    return sorted(out, key=lambda ip: ip.startswith("100."))


def start_peer_server(coord: Coord) -> list[str]:
    ips = advertise_ips(coord.host)
    try:
        srv = ThreadingHTTPServer(("0.0.0.0", PEER_PORT), BlobHandler)
    except OSError as e:
        log(f"peer serving off ({e}); this host will only download")
        return []
    srv.daemon_threads = True
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return [f"http://{ip}:{PEER_PORT}" for ip in ips]


# --- model store: content-addressed, shared by every slot on this host ---------
_locks: dict[str, threading.Lock] = {}
_locks_guard = threading.Lock()


def _lock(key: str) -> threading.Lock:
    with _locks_guard:
        return _locks.setdefault(key, threading.Lock())


NODE = {"host": "", "peer": []}   # this host, as peers and the coordinator see it


def blob(c: Coord, sha: str) -> Path:
    """A blob in the local cache: fetched once (peers first, depot last), verified, then
    trusted by name."""
    p = CACHE / "blobs" / sha
    with _lock(sha):
        if not p.exists():
            p.parent.mkdir(parents=True, exist_ok=True)
            t0 = time.time()
            src = c.download(sha, p, NODE["host"])
            log(f"fetched {sha[:12]} ({p.stat().st_size / 1e6:.0f} MB) from {src} "
                f"in {time.time() - t0:.0f}s")
    return p


def carve_shard(m: dict, s: dict) -> bool:
    """A host that already holds the stitched model can cut a shard out of it by offset
    instead of downloading it; the cut is verified against the shard's sha like any blob."""
    src = CACHE / "models" / f"{m['sha256']}.gguf"
    dst = CACHE / "blobs" / s["sha"]
    if dst.exists():
        return True
    if not src.exists():
        return False
    offset = sum(x["size"] for x in m["shards"] if x["index"] < s["index"])
    with _lock(s["sha"]):
        with open(src, "rb") as f:
            f.seek(offset)
            data = f.read(s["size"])
        if hashlib.sha256(data).hexdigest() != s["sha"]:
            return False
        tmp = dst.with_name(dst.name + ".stitch")
        tmp.write_bytes(data)
        os.replace(tmp, dst)
    log(f"carved shard {s['sha'][:12]} ({s['size'] / 1e6:.0f} MB) from the local model")
    return True


def model_file(c: Coord, m: dict, label: str) -> Path:
    """The tier's model: the base ships whole; a larger one is stitched from its shards
    and must hash to the publisher's SHA-256 before anything runs it."""
    if m.get("blob"):
        return blob(c, m["blob"])
    p = CACHE / "models" / f"{m['sha256']}.gguf"
    with _lock(m["sha256"]):
        if not p.exists():
            p.parent.mkdir(parents=True, exist_ok=True)
            try:   # a peer that already stitched it can hand the whole model over
                t0 = time.time()
                src = c.download(m["sha256"], p, NODE["host"], depot=False)
                log(f"fetched {label} whole from {src} in {time.time() - t0:.0f}s; "
                    f"sha256 matches the publisher")
                return p
            except ConnectionError:
                pass
            parts = [blob(c, s["sha"]) for s in sorted(m["shards"], key=lambda s: s["index"])]
            tmp = p.with_name(p.name + ".stitch")
            h = hashlib.sha256()
            with open(tmp, "wb") as out:
                for part in parts:
                    with open(part, "rb") as f:
                        while chunk := f.read(1 << 22):
                            h.update(chunk)
                            out.write(chunk)
            if h.hexdigest() != m["sha256"]:
                tmp.unlink()
                raise ValueError(f"stitched {label} does not match the publisher's sha")
            os.replace(tmp, p)
            log(f"stitched {label}: {len(parts)} shards -> {m['size']/1e9:.2f} GB, "
                f"sha256 {m['sha256'][:12]} matches the publisher")
    return p


# --- inference ----------------------------------------------------------------
class LlamaServer:
    """A native llama-server on loopback for one agent's model."""

    def __init__(self, model: Path, ctx: int, threads: int, tag: str):
        with socket.socket() as s:
            s.bind(("127.0.0.1", 0))
            self.port = s.getsockname()[1]
        logf = open(CACHE / f"llama-{tag}.log", "ab")
        cmd = [LLAMA_SERVER, "-m", str(model), "--host", "127.0.0.1", "--port", str(self.port),
               "-c", str(ctx), "-t", str(threads), "-np", "1", "-ngl", GPU_LAYERS,
               "--jinja", "--reasoning-budget", "0"]
        self.proc = subprocess.Popen(cmd, stdout=logf, stderr=logf, stdin=subprocess.DEVNULL)
        deadline = time.time() + 300
        while time.time() < deadline:
            if self.proc.poll() is not None:
                raise RuntimeError(f"llama-server exited {self.proc.returncode} (see {logf.name})")
            try:
                if self._call("GET", "/health", None, 2).get("status") == "ok":
                    return
            except (OSError, HTTPException, ValueError):
                pass
            time.sleep(0.5)
        self.close()
        raise RuntimeError("llama-server did not come up")

    def _call(self, method: str, path: str, obj: dict | None, timeout: float) -> dict:
        conn = HTTPConnection("127.0.0.1", self.port, timeout=timeout)
        try:
            conn.request(method, path, body=json.dumps(obj).encode() if obj else None,
                         headers={"Content-Type": "application/json"})
            r = conn.getresponse()
            data = r.read()
            if r.status >= 400:
                raise ValueError(f"llama-server {r.status}: {data[:200]!r}")
            return json.loads(data)
        finally:
            conn.close()

    def chat(self, system: str, user: str, max_tokens: int = 384,
             schema: dict | None = None) -> str:
        body = {"messages": [{"role": "system", "content": system},
                             {"role": "user", "content": user}],
                "max_tokens": max_tokens, "temperature": 0.3,
                # Qwen3 thinks by default; its template's switch is the reliable off
                "chat_template_kwargs": {"enable_thinking": False}}
        if schema:
            body["response_format"] = {"type": "json_object", "schema": schema}
        r = self._call("POST", "/v1/chat/completions", body, 900)
        self.last_tps = (r.get("timings") or {}).get("predicted_per_second")
        text = r["choices"][0]["message"]["content"]
        return text.split("</think>")[-1].strip()   # if any reasoning leaked, drop it

    def close(self) -> None:
        if self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(10)
            except subprocess.TimeoutExpired:
                self.proc.kill()


class StandIn:
    """Inference for stand-in tiers: shaped like the real thing, instant unless
    SLIME_STANDIN_DELAY (seconds per call) makes it slow enough to load-test the swarm."""
    last_tps = None

    def __init__(self, tier: int):
        self.tier = tier

    def chat(self, system: str, user: str, max_tokens: int = 384,
             schema: dict | None = None) -> str:
        time.sleep(float(os.environ.get("SLIME_STANDIN_DELAY", "0")))
        # SLIME_FAULT (tests), comma-separated: error | garbage | disagree | poison:<text>
        # (subagent answers choke on prompts containing it) | poisongoal:<text> (the
        # orchestrator does)
        faults = set(filter(None, os.environ.get("SLIME_FAULT", "").split(",")))
        lead = system in (PLAN_SYSTEM, SYNTH_SYSTEM)
        if "error" in faults or any(
                f.startswith("poisongoal:" if lead else "poison:") and f.split(":", 1)[1] in user
                for f in faults):
            raise RuntimeError("injected fault")
        if "garbage" in faults and not (schema and system is not REVIEW_SYSTEM):
            return "" if system is REVIEW_SYSTEM else "lorem ipsum " * 40
        if system is REVIEW_SYSTEM:   # disagree: reviewers reject every answer
            return json.dumps({"own_answer": f"[stand-in tier{self.tier}] own answer",
                               "agrees": self.tier % 2 == 1 and "disagree" not in faults})
        if schema:
            return json.dumps({"subtasks": [
                {"question": f"part {i + 1} of: {user[:60]}", "difficulty": ("easy", "hard")[i % 2]}
                for i in range(4)]})
        return f"[stand-in tier{self.tier}] answer to: {user[:80]}"

    def close(self) -> None:
        pass


PLAN_SCHEMA = {
    "type": "object",
    "properties": {"subtasks": {"type": "array", "minItems": 2, "maxItems": 6, "items": {
        "type": "object",
        "properties": {"question": {"type": "string"},
                       "difficulty": {"type": "string", "enum": ["easy", "hard"]}},
        "required": ["question", "difficulty"]}}},
    "required": ["subtasks"],
}
PLAN_SYSTEM = ("You lead a team of small assistant models. Split the goal into 3 to 5 "
               "independent sub-questions that together cover it. Each must be answerable on "
               "its own in a few sentences. Mark a sub-question 'hard' only if it needs real "
               "reasoning or judgement; facts, definitions and simple lists are 'easy'. Most "
               "goals have both kinds, and the easy ones go to your smallest, fastest helpers.")
EXPERT_NOTE = (" You also have a capable deputy: mark at most one sub-question 'expert' if it "
               "needs careful multi-step reasoning or weighing evidence.")
LEVELS = {"easy": 0, "hard": 1, "expert": 2}


def plan_schema(levels: list[str]) -> dict:
    s = json.loads(json.dumps(PLAN_SCHEMA))
    s["properties"]["subtasks"]["items"]["properties"]["difficulty"]["enum"] = levels
    return s


SERVE_SYSTEM = "You are a concise expert assistant. Answer in at most 4 sentences."
REVIEW_SYSTEM = ("You check a smaller model's answer. First answer the question yourself, "
                 "independently, in at most 3 sentences. Then compare: does the proposed answer "
                 "agree with yours on every fact and number?")
# own answer first, verdict second: asked "is this right?" cold, a small model says yes
REVIEW_SCHEMA = {
    "type": "object",
    "properties": {"own_answer": {"type": "string"}, "agrees": {"type": "boolean"}},
    "required": ["own_answer", "agrees"],
}
SYNTH_SYSTEM = ("You lead a team of small assistant models. Using their answers to the "
                "sub-questions (a reviewer's correction outranks the answer it corrects; fix "
                "anything still wrong), write one clear, well-organised answer to the goal.")


def review_correction(rv: dict | None) -> str | None:
    """The reviewer's own answer when it disagrees with the one it checked, else None."""
    if not rv:
        return None
    try:
        v = json.loads(rv["result"])
        return None if v.get("agrees") else v.get("own_answer")
    except (ValueError, AttributeError):
        text = rv["result"].strip()
        return None if text.rstrip(".").upper() == "OK" else text


# --- the agent an idle runner embodies ----------------------------------------
class Agent:
    def __init__(self, coord: Coord, a: dict, threads: int):
        self.c = coord
        self.id = a["agent_id"]
        self.tier = a["tier"]
        self.shard_index = a["shard_index"]
        self.spec = a["spec"]
        self.manifest = a["manifest"]
        self.genome = a["genome"]
        self.provides = self.spec.get("provides")
        self.threads = threads
        self.stop = threading.Event()
        self.phase = "embodying"
        self.llm = None
        self.lead = None   # the tier that orchestrates, as the coordinator last said

    def tier_model(self, tier: int) -> dict | None:
        return next(t for t in self.manifest["tiers"] if t["tier"] == tier).get("model")

    # liveness runs on its own thread: a model download or a long generation must
    # never look like death to the coordinator
    def heartbeat_loop(self):
        while not self.stop.is_set():
            try:
                r = self.c.post("/heartbeat", {"agent_id": self.id, "phase": self.phase})
                if r.get("status") == "gone":
                    self.stop.set()
                    return
            except (OSError, ValueError):
                pass
            self.stop.wait(1.0)

    def carry_shard(self):
        if not self.provides:
            return
        pt, i = self.provides["tier"], self.shard_index
        m = self.tier_model(pt)
        if m:
            s = next(s for s in m["shards"] if s["index"] == i)

            def fetch_and_deposit():
                carve_shard(m, s) or blob(self.c, s["sha"])
                self.c.post("/deposit", {"provides_tier": pt, "index": i, "sha": s["sha"],
                                         "agent_id": self.id})
            if (CACHE / "blobs" / s["sha"]).exists() or s["size"] < 1e9:
                fetch_and_deposit()
            else:   # a GB-sized shard (a larger tier's): serve meanwhile, deposit once held
                def bg():
                    try:
                        fetch_and_deposit()
                    except (OSError, ValueError) as e:
                        log(f"[{self.id}] carrying shard {i} of tier{pt} failed: {e}")
                threading.Thread(target=bg, daemon=True).start()
        else:
            data = base64.b64encode(make_shard_bytes(pt, i)).decode()
            self.c.post("/deposit", {"provides_tier": pt, "index": i, "sha": shard_sha(pt, i),
                                     "blob": data, "agent_id": self.id})

    def embody(self):
        self.carry_shard()
        m = self.spec.get("model")
        if m:
            self.phase = "loading"
            path = model_file(self.c, m, self.spec["label"])
            self.llm = LlamaServer(path, self.spec.get("ctx", 2048), self.threads, self.id)
        else:
            self.llm = StandIn(self.tier)
        self.phase = "ready"
        log(f"[{self.id}] embodied tier{self.tier} ({self.spec['label']})"
            + (f", carrying tier{self.provides['tier']} shard {self.shard_index}" if self.provides else ""))

    def try_promote(self):
        if not self.provides:
            return
        nxt = self.provides["tier"]
        r = self.c.post("/elect", {"tier": nxt, "agent_id": self.id})
        if not r.get("won"):
            return
        self.phase = f"assembling tier{nxt}"
        label = next(t for t in self.manifest["tiers"] if t["tier"] == nxt)["label"]
        m = self.tier_model(nxt)
        if m:
            sha = m["sha256"]
            model_file(self.c, m, label)   # raises unless it matches the publisher
        else:
            k = r["k"]
            blobs = []
            for i in range(k):
                s = self.c.get(f"/shard?tier={nxt}&index={i}")
                data = base64.b64decode(s["blob"])
                if not (hashlib.sha256(data).hexdigest() == s["sha"] == shard_sha(nxt, i)):
                    raise ValueError(f"shard {i} failed verification")
                blobs.append(data)
            sha = hashlib.sha256(stitch(blobs)).hexdigest()
        self.c.post("/model_ready", {"tier": nxt, "sha": sha, "agent_id": self.id})
        self.phase = "ready"
        log(f"[{self.id}] promoted the swarm: tier{nxt} ({label}) assembled, verified, ready")

    def directive_action(self) -> str:
        """The most complex directive this tier is licensed for; orchestrating only while
        this is the leading tier (when a larger model takes over, this one serves)."""
        licensed = [d for d in self.genome["directives"] if d["complexity"] <= self.tier]
        if self.lead is not None and self.tier < self.lead:
            licensed = [d for d in licensed if d["action"] != "orchestrate"] or licensed[:1]
        return licensed[-1]["action"]

    def serve(self):
        r = self.c.post("/task/claim", {"agent_id": self.id, "tier": self.tier})
        if not r.get("claimed"):
            return
        t = r["task"]
        self.phase = f"task {t['id']}"
        t0 = time.time()
        try:
            if t.get("kind") == "review":
                answer = self.llm.chat(REVIEW_SYSTEM, t["prompt"], max_tokens=320,
                                       schema=REVIEW_SCHEMA)
            else:
                answer = self.llm.chat(SERVE_SYSTEM, t["prompt"], max_tokens=320)
        except (OSError, HTTPException, ValueError, RuntimeError, KeyError) as e:
            # the coordinator re-queues it elsewhere, and gives up on it if it keeps failing
            self.phase = "ready"
            self.c.post("/task/fail", {"id": t["id"], "agent_id": self.id,
                                       "error": f"{type(e).__name__}: {e}"[:200]})
            log(f"[{self.id}] task {t['id']} failed: {type(e).__name__}: {e}")
            return
        tps = self.llm.last_tps
        r = self.c.post("/task/done", {"id": t["id"], "result": answer, "agent_id": self.id,
                                       "seconds": round(time.time() - t0, 1),
                                       "tps": round(tps, 1) if tps else None})
        self.phase = "ready"
        if r.get("rejected"):
            log(f"[{self.id}] task {t['id']} output rejected by the coordinator: {r['rejected']}")
            return
        log(f"[{self.id}] {t.get('kind', 'answer')} task {t['id']} done in "
            f"{time.time() - t0:.0f}s" + (f" ({tps:.1f} tok/s)" if tps else ""))

    def orchestrate(self):
        r = self.c.post("/goal/claim", {"agent_id": self.id})
        if not r.get("claimed"):
            return
        g, inflamed = r["goal"], r.get("inflamed", False)
        log(f"[{self.id}] orchestrating goal {g['id']}: {g['text'][:70]}"
            + (" (swarm inflamed: cross-checking every answer)" if inflamed else ""))
        if not g["tasks"]:
            self.phase = f"planning goal {g['id']}"
            # a lead above tier 2 has tier 2 serving under it: offer it the expert questions
            levels = ["easy", "hard", "expert"] if self.tier >= 3 else ["easy", "hard"]
            try:
                plan = json.loads(self.llm.chat(
                    PLAN_SYSTEM + (EXPERT_NOTE if "expert" in levels else ""), g["text"],
                    max_tokens=600, schema=plan_schema(levels)))
                # each helper sees the whole question as context: a sub-question cut loose
                # from it ("what does the ball cost?") is unanswerable for a small model
                tasks = [{"prompt": f"Overall question (for context): {g['text']}\n\n"
                                    f"Answer only this part: {s['question']}",
                          "q": s["question"], "min_tier": LEVELS.get(s["difficulty"], 0)}
                         for s in plan["subtasks"]]
            except (OSError, HTTPException, ValueError, RuntimeError, KeyError, TypeError) as e:
                return self.goal_failed(g["id"], "planning", e)
            self.c.post("/task/post", {"goal_id": g["id"], "tasks": tasks, "agent_id": self.id})
            log(f"[{self.id}] planned {len(tasks)} subtasks for the lower tiers")
        # how answers are checked: the smallest model's answers go up a tier for review, on
        # another host than the one that wrote them (while the swarm is inflamed, every
        # tier's are); each is sent the moment it lands, not after the slowest answer
        checked_tiers = tuple(range(self.tier)) if inflamed else (0,)
        g = self.await_and_review(g["id"], checked_tiers)
        if g is None:
            return
        answers = [t for t in g["tasks"] if t.get("kind", "answer") == "answer"]
        reviews = {t["of"]: t for t in g["tasks"] if t.get("kind") == "review"}
        self.phase = f"synthesising goal {g['id']}"
        parts = []
        for i, t in enumerate(answers):
            if t["status"] == "failed":
                parts.append(f"Q{i + 1}: {t.get('q') or t['prompt']}\nA{i + 1}: (no answer: the subtask failed "
                             f"repeatedly and was given up)")
                continue
            part = (f"Q{i + 1}: {t.get('q') or t['prompt']}\n"
                    f"A{i + 1} (tier{t.get('worker_tier')}): {t['result']}")
            correction = review_correction(reviews.get(t["id"]))
            if correction:
                part += (f"\nReviewer (tier{reviews[t['id']].get('worker_tier')}) disagrees; "
                         f"its own answer: {correction}")
            parts.append(part)
        findings = "\n\n".join(parts)
        try:
            answer = self.llm.chat(SYNTH_SYSTEM, f"Goal: {g['text']}\n\n{findings}",
                                   max_tokens=1000)
        except (OSError, HTTPException, ValueError, RuntimeError, KeyError) as e:
            return self.goal_failed(g["id"], "synthesis", e)
        tps = self.llm.last_tps
        r = self.c.post("/goal/done", {"goal_id": g["id"], "answer": answer, "agent_id": self.id,
                                       "tps": round(tps, 1) if tps else None})
        self.phase = "ready"
        if r.get("rejected"):
            log(f"[{self.id}] goal {g['id']} synthesis rejected ({r['rejected']}); retrying")
            return
        log(f"[{self.id}] goal {g['id']} answered")

    def goal_failed(self, gid: str, step: str, e: Exception) -> None:
        """This agent's model failed the goal: hand it back (the coordinator retries it,
        elsewhere if it can, and gives up on it if it keeps failing)."""
        self.phase = "ready"
        self.c.post("/goal/fail", {"goal_id": gid, "agent_id": self.id,
                                   "error": f"{step}: {type(e).__name__}: {e}"[:200]})
        log(f"[{self.id}] goal {gid} {step} failed: {type(e).__name__}: {e}")

    def await_and_review(self, gid: str, checked_tiers: tuple) -> dict | None:
        """Wait for the goal's subtasks, posting a review for each answer from a checked
        tier as soon as it is done; returns the goal once everything is settled."""
        self.phase = f"awaiting goal {gid}"
        while not self.stop.is_set():
            g = self.c.get(f"/goal?id={gid}")
            reviewed = {t.get("of") for t in g["tasks"] if t.get("kind") == "review"}
            fresh = [t for t in g["tasks"] if t.get("kind", "answer") == "answer"
                     and t["status"] == "done" and t["id"] not in reviewed
                     and t.get("worker_tier") in checked_tiers]
            if fresh:
                self.c.post("/task/post", {"goal_id": gid, "append": True, "agent_id": self.id,
                                           "tasks": [
                    {"kind": "review", "of": t["id"],
                     "min_tier": max(1, min(t.get("worker_tier", 0) + 1, self.tier - 1)),
                     "avoid": [t["worker_host"]] if t.get("worker_host") else [],
                     "prompt": f"Question: {t['prompt']}\n\nProposed answer: {t['result']}"}
                    for t in fresh]})
                log(f"[{self.id}] sent {len(fresh)} answer(s) up a tier for review")
                continue
            if g["tasks"] and all(t["status"] in ("done", "failed") for t in g["tasks"]):
                return g
            self.stop.wait(1.0)
        return None

    def shed_for_pressure(self) -> bool:
        """Under critical memory pressure this host gives up its lowest-tier agent, one
        every SHED_EVERY seconds, until the pressure eases (the swarm re-places it)."""
        if memory_pressure() != "critical":
            return False
        with AGENTS_LOCK:
            if time.time() - SHED["t"] < SHED_EVERY:
                return False
            if self.tier > min(a.tier for a in AGENTS):
                return False
            SHED["t"] = time.time()
        self.c.post("/leave", {"agent_id": self.id, "reason": "pressure"})
        log(f"[{self.id}] left the swarm: this host is under critical memory pressure")
        return True

    def run(self):
        with AGENTS_LOCK:
            AGENTS.add(self)
        threading.Thread(target=self.heartbeat_loop, daemon=True).start()
        try:
            err = ""
            for attempt in range(3):
                try:
                    self.embody()
                    break
                except (OSError, ValueError, RuntimeError, KeyError, StopIteration) as e:
                    err = f"{type(e).__name__}: {e}"[:200]
                    log(f"[{self.id}] embody failed ({attempt + 1}/3): {err}")
                    if self.stop.wait(3):
                        return
            else:
                # this host can't hold the tier: give it back rather than sit on it
                self.c.post("/leave", {"agent_id": self.id, "reason": "embody_failed",
                                       "error": err})
                self.stop.wait(30)
                return
            while not self.stop.is_set():
                try:
                    if self.shed_for_pressure():
                        return
                    r = self.c.post("/retire", {"agent_id": self.id})
                    self.lead = r.get("lead", self.lead)
                    if r.get("retire"):
                        why = (f"making room for tier{r['for_tier']}" if "for_tier" in r
                               else f"tier{self.tier} above its want")
                        log(f"[{self.id}] retired ({why})")
                        return
                    self.try_promote()
                    if self.directive_action() == "orchestrate":
                        self.orchestrate()
                    else:
                        self.serve()
                except (OSError, KeyError, ValueError, RuntimeError) as e:
                    log(f"[{self.id}] {type(e).__name__}: {e}")
                    self.phase = "ready"
                self.stop.wait(TICK)
        finally:
            self.stop.set()
            with AGENTS_LOCK:
                AGENTS.discard(self)
            if self.llm:
                self.llm.close()


# --- runner slots -------------------------------------------------------------
AGENTS: set = set()            # agents embodied in this process
AGENTS_LOCK = threading.Lock()
STOPPING = threading.Event()   # set on SIGTERM
SHED = {"t": 0.0}
SHED_EVERY = 30.0
_PRESSURE = {"t": 0.0, "level": "ok", "why": ""}
_THROTTLE: list[tuple[float, int]] = []   # (when, package throttle count) samples, Linux
THROTTLE_WARN = 50                        # throttle events in 30 s that mean "running hot"


def thermal_pressure() -> str:
    """"warn" while the CPU is being thermally throttled, else "ok". Linux: the package
    throttle counter rising fast, or the package at 95 C+; macOS: a CPU speed limit."""
    try:
        if platform.system() == "Darwin":
            out = subprocess.check_output(["pmset", "-g", "therm"], text=True, timeout=2)
            for line in out.splitlines():
                if "CPU_Speed_Limit" in line and int(line.split("=")[1]) < 100:
                    return "warn"
            return "ok"
        base = Path("/sys/devices/system/cpu/cpu0/thermal_throttle/package_throttle_count")
        if base.exists():
            now_t, n = time.time(), int(base.read_text())
            _THROTTLE.append((now_t, n))
            while _THROTTLE and now_t - _THROTTLE[0][0] > 30:
                _THROTTLE.pop(0)
            if n - _THROTTLE[0][1] > THROTTLE_WARN:
                return "warn"
        for z in Path("/sys/class/thermal").glob("thermal_zone*"):
            if (z / "type").read_text().strip() == "x86_pkg_temp" \
                    and int((z / "temp").read_text()) >= 95000:
                return "warn"
    except (OSError, ValueError, IndexError, subprocess.SubprocessError):
        pass
    return "ok"


def memory_pressure() -> str:
    """ok | warn | critical, from the OS's own pressure signal (sampled every 5 s):
    macOS's memorystatus level; Linux PSI (share of the last 10 s that tasks stalled on
    memory). SLIME_PRESSURE_FILE, when set, is read instead (tests). A thermally throttled
    CPU counts as warn (nothing new), never critical: heat is no reason to shed work."""
    if time.time() - _PRESSURE["t"] < 5:
        return _PRESSURE["level"]
    level = "ok"
    try:
        override = os.environ.get("SLIME_PRESSURE_FILE")
        if override:
            p = Path(override)
            level = (p.read_text().strip() if p.exists() else "") or "ok"
        elif platform.system() == "Darwin":
            v = int(subprocess.check_output(
                ["sysctl", "-n", "kern.memorystatus_vm_pressure_level"], timeout=2))
            level = {2: "warn", 4: "critical"}.get(v, "ok")
        else:
            avg10 = {}
            for line in Path("/proc/pressure/memory").read_text().splitlines():
                kind, rest = line.split(None, 1)
                avg10[kind] = float(dict(kv.split("=") for kv in rest.split())["avg10"])
            level = ("critical" if avg10.get("full", 0) >= 10
                     else "warn" if avg10.get("some", 0) >= 20 else "ok")
    except (OSError, ValueError, KeyError, subprocess.SubprocessError):
        pass
    if level not in ("ok", "warn", "critical"):
        level = "ok"
    why = "memory" if level != "ok" else ""
    if level == "ok" and not os.environ.get("SLIME_PRESSURE_FILE") and thermal_pressure() == "warn":
        level, why = "warn", "thermal"
    if level != _PRESSURE["level"]:
        log(f"host pressure: {level}" + (f" ({why})" if why else ""))
    _PRESSURE.update(t=time.time(), level=level, why=why)
    return level


def capacity_mb() -> int:
    """Memory this host offers agents: SLIME_CAPACITY_MB, else 80% of RAM (or the
    container's cgroup limit when there is one)."""
    if os.environ.get("SLIME_CAPACITY_MB"):
        return int(os.environ["SLIME_CAPACITY_MB"])
    total = None
    try:
        v = Path("/sys/fs/cgroup/memory.max").read_text().strip()
        if v != "max":
            total = int(v) // 2**20
    except OSError:
        pass
    if total is None:
        if platform.system() == "Darwin":
            total = int(subprocess.check_output(["sysctl", "-n", "hw.memsize"])) // 2**20
        else:
            for line in Path("/proc/meminfo").read_text().splitlines():
                if line.startswith("MemTotal:"):
                    total = int(line.split()[1]) // 1024
    return int(total * 0.8)


def cached_shas() -> list[str]:
    """What this host already holds, so the coordinator can send work to the data."""
    out = []
    for sub, suffix in (("blobs", ""), ("models", ".gguf")):
        try:
            out += [n[:64] for n in os.listdir(CACHE / sub) if len(n) == 64 + len(suffix)
                    and n.endswith(suffix)]
        except OSError:
            pass
    return out


IDLE_POLL = 2.0   # seconds between /assign polls once a slot has been idle a while


def runner_loop(coord: Coord, host: str, slot: str, threads: int, cap: int,
                stop: threading.Event | None = None):
    empty = 0   # consecutive "nothing for you": after 10 the slot polls every IDLE_POLL s
    while not (stop and stop.is_set()):
        if STOPPING.is_set():
            time.sleep(1)
            continue
        if memory_pressure() != "ok":   # take on nothing new while the host is squeezed
            time.sleep(2)
            continue
        try:
            a = coord.post("/assign", {"node": slot, "host": host, "capacity_mb": cap,
                                       "cpus": os.cpu_count(), "cached": cached_shas(),
                                       "peer": NODE["peer"], "max_tier": MAX_TIER,
                                       "pressure": "ok"})
        except (OSError, ValueError):
            time.sleep(TICK)
            continue
        if a.get("assigned") and STOPPING.is_set():
            coord.post("/leave", {"agent_id": a["agent_id"], "reason": "shutdown"})
        elif a.get("assigned"):
            empty = 0
            Agent(coord, a, threads).run()
        else:
            empty += 1
            time.sleep(IDLE_POLL if empty > 10 else TICK)


# --- control plane ------------------------------------------------------------
def cmd_seed(args):
    c = Coord(args.coord)
    manifest = json.loads(Path(args.manifest).read_text())
    genome = json.loads(Path(args.genome).read_text())
    token = os.environ.get("SLIME_ADMIN_TOKEN", "")
    if not token and args.admin_token_file:
        token = Path(args.admin_token_file).read_text().strip()
    r = c.post("/seed", {"manifest": manifest, "genome": genome,
                         "budget": manifest["resource_total"]},
               {"X-Slime-Admin": token} if token else None)
    print(f"seeded coordinator: {manifest['version']}, budget {r['budget']}, wants {r['wants']}")


def prepare_cache() -> None:
    """One runner process per host owns the cache. Partial downloads (`.part`) are kept
    and resumed; a half-written stitch, or a part named by an older client, is garbage."""
    for sub in ("blobs", "models"):
        (CACHE / sub).mkdir(parents=True, exist_ok=True)
        for p in [*(CACHE / sub).glob("*.stitch"), *(CACHE / sub).glob("*.part?*")]:
            p.unlink(missing_ok=True)


def host_and_threads(args) -> tuple[str, int]:
    host = args.node or socket.gethostname().split(".")[0]
    threads = int(os.environ.get("SLIME_THREADS", max(2, (os.cpu_count() or 2) // 2)))
    return host, threads


SLEPT: list[list[float]] = []   # [from, to] wall-clock windows this host was suspended


def node_loop(coord: Coord, host: str, cap: int):
    """Announce this host every few seconds (cache + peer address), busy slots or not.
    Also notices suspend: the monotonic clock stops while the machine sleeps and the wall
    clock doesn't, so a gap between them is a sleep, reported (until acknowledged) so the
    lapse it caused isn't held against this host."""
    wall, mono = time.time(), time.monotonic()
    while True:
        w, m = time.time(), time.monotonic()
        if (w - wall) - (m - mono) > 15:
            SLEPT.append([wall, w])
            del SLEPT[:-5]
            log(f"this host was asleep for {(w - wall) - (m - mono):.0f}s")
        wall, mono = w, m
        try:
            r = coord.post("/node", {"host": host, "capacity_mb": cap, "cpus": os.cpu_count(),
                                     "cached": cached_shas(), "peer": NODE["peer"],
                                     "max_tier": MAX_TIER, "pressure": memory_pressure(),
                                     "pressure_why": _PRESSURE["why"], "slept": SLEPT})
            if r.get("ok") and SLEPT:
                SLEPT.clear()
        except (OSError, ValueError):
            pass
        time.sleep(5)


def prefetch_loop(c: Coord, cap: int) -> None:
    """Warm this host's cache with every model it may embody, one at a time, so that being
    assigned a tier (or a failover landing here) costs seconds, not a multi-GB download."""
    time.sleep(20)
    while True:
        try:
            tiers = {t["tier"]: t for t in c.get("/manifest").get("tiers", [])}
            for n, t in sorted(tiers.items()):
                if n > MAX_TIER or not t.get("model") or t["cost"] > cap:
                    continue
                model_file(c, t["model"], t["label"])
                # an agent of this tier carries a shard of the next: hold them all, so a
                # promotion here waits on nothing
                nxt = (tiers.get((t.get("provides") or {}).get("tier")) or {}).get("model") or {}
                for s in nxt.get("shards", []):
                    carve_shard(nxt, s) or blob(c, s["sha"])
        except (OSError, ValueError, KeyError) as e:
            log(f"prefetch paused: {type(e).__name__}: {e}")
        time.sleep(300)


def load_identity(host: str) -> None:
    """This host's key (SLIME_KEY_FILE, default next to the cache): with it every runner
    call is signed; without it calls go unsigned, which a strict coordinator refuses."""
    path = Path(os.environ.get("SLIME_KEY_FILE", CACHE.parent / "host.key"))
    try:
        SIGNER.update(host=host, key=bytes.fromhex(path.read_text().strip()))
        log(f"signing as {host} (key {path})")
    except (OSError, ValueError):
        log(f"no host key at {path}: calls go unsigned")


def shutdown(signum, frame) -> None:
    """SIGTERM (a deploy, a service stop): leave on purpose, so the coordinator re-places
    these agents at once rather than waiting out their heartbeats (and holding the lapse
    against this host)."""
    STOPPING.set()   # slots take nothing new; an /assign already in flight is handed back
    gone: list = []
    for _ in range(2):
        with AGENTS_LOCK:
            live = [a for a in AGENTS if a not in gone]
        for a in live:
            a.stop.set()
            try:
                a.c.post("/leave", {"agent_id": a.id, "reason": "shutdown"})
            except (OSError, ValueError):
                pass
        gone += live
        time.sleep(1.0)
    for a in gone:
        if a.llm:
            a.llm.close()
    log(f"shutting down: {len(gone)} agents left the swarm")
    os._exit(0)


def start_host(c: Coord, host: str, cap: int) -> None:
    signal.signal(signal.SIGTERM, shutdown)
    prepare_cache()
    load_identity(host)
    NODE.update(host=host, peer=start_peer_server(c))
    threading.Thread(target=node_loop, args=(c, host, cap), daemon=True).start()
    if os.environ.get("SLIME_PREFETCH", "1") != "0":
        threading.Thread(target=prefetch_loop, args=(c, cap), daemon=True).start()


def cmd_runner(args):
    c = Coord(args.coord)
    host, threads = host_and_threads(args)
    cap = capacity_mb()
    start_host(c, host, cap)
    runner_loop(c, host, host, threads, cap)


def cmd_pool(args):
    c = Coord(args.coord)
    host, threads = host_and_threads(args)
    cap = capacity_mb()
    start_host(c, host, cap)
    stop = threading.Event()
    threads_ = [threading.Thread(target=runner_loop, args=(c, host, f"{host}-{i}", threads, cap, stop),
                                 daemon=True) for i in range(args.slots)]
    for t in threads_:
        t.start()
    log(f"pool of {args.slots} runner slots on {host}: {cap} MB, {threads} threads per model, "
        f"cache {CACHE}, serving peers at {NODE['peer']}")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        stop.set()


def cmd_ask(args):
    c = Coord(args.coord)
    r = c.post("/goal/post", {"text": args.text})
    if not r.get("ok"):
        raise SystemExit(r.get("error"))
    gid = r["goal_id"]
    print(f"goal {gid} posted")
    if not args.wait:
        return
    seen = set()
    while True:
        g = c.get(f"/goal?id={gid}")
        for t in g["tasks"]:
            key = (t["id"], t["status"])
            if key not in seen and t["status"] != "todo":
                seen.add(key)
                print(f"  subtask {t['id']} [{t['status']}] tier{t.get('worker_tier')} "
                      f"{t.get('worker_node')}: {t['prompt'][:70]}")
        if g["status"] == "done":
            print(f"\nanswer (by {g.get('by')}):\n{g['answer']}")
            return
        time.sleep(1)


def render(st: dict) -> str:
    lines = [f"resources: {st['used']}/{st['budget']} used  "
             f"(coordinator {st.get('role', 'primary')}, epoch {st.get('epoch', 1)})"]
    labels = st.get("labels", {})
    for tier in sorted({int(k) for k in {**st['counts'], **st['wants']}}):
        live = st["counts"].get(str(tier), 0)
        want = st["wants"].get(str(tier), 0)
        ready = "ready" if str(tier) in st["ready"] or tier == 0 else "     "
        base = st.get("base", {}).get(str(tier), want)
        scaled = f" (base {base})" if want != base else ""
        lines.append(f"  tier{tier} live:{live:2d}/want:{want:<2d} [{'#' * live:<8}] {ready}"
                     f"  {labels.get(str(tier), '')}{scaled}")
    by_host: dict[str, list[str]] = {}
    for a in st["agents"].values():
        by_host.setdefault(a.get("host", a["node"]), []).append(f"t{a['tier']}:{a.get('phase', '')}")
    for host, cap in sorted(st.get("nodes", {}).items()):
        used = st.get("node_used", {}).get(host, 0)
        squeeze = cap.get("pressure", "ok")
        squeeze = "" if squeeze == "ok" else f"  [{squeeze} {cap.get('pressure_why', '')}]".rstrip("]") + "]"
        lines.append(f"  {host:<16} {used:>5}/{cap.get('capacity_mb')} MB  "
                     + ", ".join(sorted(by_host.get(host, []))) + squeeze)
    im = st.get("immune") or {}
    if im.get("inflamed") or im.get("hosts"):
        lines.append("  immune: " + ("INFLAMED; " if im.get("inflamed") else "") + ", ".join(
            f"{h} {v[0]}" + (" QUARANTINED" if v[1] else "") for h, v in sorted(im["hosts"].items())))
    if st.get("scaling"):
        lines.append("  control " + ("on" if st.get("control_on") else "off") + ": " + "; ".join(
            f"{time.strftime('%H:%M:%S', time.localtime(e['t']))} "
            + (f"t{e['tier']} {e['from']}->{e['to']}" if "from" in e else f"{e['type']} t{e['tier']}")
            + f" ({e['why']})" for e in st["scaling"][-2:]))
    esc = st.get("escalation") or {}
    if esc.get("on"):
        lines.append(f"  ESCALATED (lead tier{st.get('lead')}): {esc.get('why')}")
    for g in st.get("goals", []):
        done = sum(t["status"] == "done" for t in g["tasks"])
        lines.append(f"  goal {g['id']} [{g['status']}] {done}/{len(g['tasks'])} subtasks: "
                     f"{g['text'][:60]}")
        if g.get("answer"):
            lines.append("    answer: " + g["answer"][:200].replace("\n", " ") + "…")
    return "\n".join(lines)


def cmd_promote(args):
    """Make the coordinator at --coord (a standby, or a primary failing back) the primary."""
    token = Path(args.admin_token_file).read_text().strip()
    r = Coord(args.coord).post("/promote", {}, {"X-Slime-Admin": token})
    print(f"{args.coord}: {r['role']} at epoch {r['epoch']}")


def cmd_control(args):
    """Show the controller (settings, base and current wants, recent decisions), or change
    it: --set enabled=off, --set max.0=6, --set down_after=120, --set budget=24000,
    --set escalate=on|off|auto (admin token)."""
    c = Coord(args.coord)
    if not args.set:
        print(json.dumps(c.get("/control"), indent=1))
        return
    body: dict = {}
    for kv in args.set:
        k, _, v = kv.partition("=")
        if k == "enabled":
            body["enabled"] = v.lower() in ("1", "on", "true", "yes")
        elif k == "budget":
            body["budget"] = int(v)
        elif k == "escalate":
            body["escalate"] = v
        elif k.startswith("max."):
            body.setdefault("max", {})[k[4:]] = int(v)
        else:
            body[k] = float(v)
    token = Path(args.admin_token_file).read_text().strip() if args.admin_token_file else ""
    print(json.dumps(c.post("/control", body, {"X-Slime-Admin": token}), indent=1))


def cmd_immune(args):
    """Show the health ledger (scores, quarantines, recent incidents, poison), or pardon a
    host / change a setting: --pardon HOST, --set half_life=900 (admin token)."""
    c = Coord(args.coord)
    if not (args.pardon or args.set or args.forget):
        d = c.get("/immune")
        print(f"inflamed: {d['inflamed']}   settings: {d['config']}")
        for h, v in d["hosts"].items():
            print(f"  {h:<16} score {v['score']:5.2f}{'  QUARANTINED' if v['quarantined'] else ''}"
                  f"  {v['counts']}")
        for a in d.get("antibodies", [])[:5]:
            print(f"  remembered poison {a['id']}: {a['text']!r} (refused {a['seen']}x; {a['why']})")
        for e in d["incidents"][-10:] + d["poison"][-5:]:
            print(f"  {time.strftime('%H:%M:%S', time.localtime(e['t']))} {e['type']:<10} "
                  + " ".join(f"{k}={v}" for k, v in e.items() if k not in ("t", "type")))
        return
    body: dict = {k: float(v) for k, _, v in (kv.partition("=") for kv in args.set)}
    token = Path(args.admin_token_file).read_text().strip() if args.admin_token_file else ""
    hdr = {"X-Slime-Admin": token}
    for h in args.pardon:
        print(json.dumps(c.post("/immune", {"pardon": h}, hdr)["hosts"].get(h)))
    if args.forget:
        r = c.post("/immune", {"forget": args.forget}, hdr)
        print(f"{len(r.get('antibodies', []))} remembered poison inputs left")
    if body:
        print(json.dumps(c.post("/immune", body, hdr)["config"]))


def pct(xs: list, q: float):
    xs = sorted(x for x in xs if x is not None)
    return round(xs[min(len(xs) - 1, int(q * len(xs)))], 1) if xs else None


def cmd_report(args):
    """What the swarm did over the last --hours: goals, tasks per tier and host, reviews,
    scaling, escalation, and everything the immune system saw."""
    c = Coord(args.coord)
    d = c.get(f"/events?since={time.time() - args.hours * 3600:.0f}")
    evs = d["events"]
    by = lambda typ: [e for e in evs if e["type"] == typ]   # noqa: E731
    span = (f"{time.strftime('%m-%d %H:%M', time.localtime(evs[0]['t']))} to "
            f"{time.strftime('%m-%d %H:%M', time.localtime(evs[-1]['t']))}") if evs else "nothing"
    print(f"last {args.hours:g} h ({span}), {len(evs)} events")
    goals = by("goal")
    lat = [g["latency"] for g in goals]
    print(f"goals: {len(goals)} answered ({sum(bool(g.get('failed')) for g in goals)} given up); "
          f"latency p50 {pct(lat, .5)} s, p90 {pct(lat, .9)} s, max {max(lat, default=None)} s")
    tasks = [e for e in by("task") if e.get("kind") != "synth"]
    for tier in sorted({e.get("tier") for e in tasks if e.get("tier") is not None}):
        ts = [e for e in tasks if e.get("tier") == tier]
        hosts: dict = {}
        for e in ts:
            hosts.setdefault(e.get("host"), []).append(e.get("tps"))
        print(f"  tier{tier}: {len(ts)} subtasks, run p50 {pct([e['run'] for e in ts], .5)} s, "
              f"wait p90 {pct([e['wait'] for e in ts], .9)} s; by host "
              + ", ".join(f"{h} {len(v)} @{pct(v, .5)} tok/s" for h, v in sorted(hosts.items())))
    syn = [e for e in by("task") if e.get("kind") == "synth"]
    if syn:
        print(f"  synthesis: {len(syn)}, " + ", ".join(
            f"{h} @{pct([e['tps'] for e in syn if e.get('host') == h], .5)} tok/s"
            for h in sorted({e.get('host') for e in syn})))
    can = [e for e in by("canary") if not e.get("probe")]
    if can:
        print(f"canaries (whole swarm): {sum(e['ok'] for e in can)}/{len(can)} right; wrong: "
              + (", ".join(f"{e['id']} (lead t{e.get('lead')})" for e in can if not e["ok"]) or "none"))
    probes = [e for e in by("canary") if e.get("probe")]
    for tier in sorted({e.get("tier") for e in probes if e.get("tier") is not None}):
        ps = [e for e in probes if e.get("tier") == tier]
        print(f"  tier{tier} alone: {sum(e['ok'] for e in ps)}/{len(ps)} right; wrong: "
              + (", ".join(sorted({e["id"] for e in ps if not e["ok"]})) or "none"))
    rv = [e["agrees"] for e in tasks if e.get("kind") == "review" and e.get("agrees") is not None]
    if rv:
        print(f"reviews: {len(rv)}, disagreed {1 - sum(rv) / len(rv):.0%}")
    sc = by("scale")
    print(f"scaling: {len(sc)} decisions" + "".join(
        f"\n  {time.strftime('%H:%M', time.localtime(e['t']))} t{e['tier']} {e['from']}->{e['to']}: "
        f"{e['why']}" for e in sc[-args.last:]))
    for typ in ("escalate", "relax", "relocated"):
        for e in by(typ):
            print(f"  {time.strftime('%H:%M', time.localtime(e['t']))} {typ}: "
                  + " ".join(f"{k}={v}" for k, v in e.items() if k not in ("t", "type")))
    inc = [e for e in by("incident") if not e.get("repeat")]
    counts: dict = {}
    for e in inc:
        key = (e["host"], e["kind"] + (" (excused)" if e.get("excused") else ""))
        counts[key] = counts.get(key, 0) + 1
    print(f"immune: {len(inc)} incidents" + ("" if not counts else ": " + ", ".join(
        f"{h} {k} x{n}" for (h, k), n in sorted(counts.items()))))
    for typ in ("quarantine", "release", "pardon", "mass_lapse", "slept", "poison", "refund",
                "antibody", "leave"):
        es = by(typ)
        if es:
            print(f"  {typ}: {len(es)}" + "".join(
                f"\n    {time.strftime('%H:%M', time.localtime(e['t']))} "
                + " ".join(f"{k}={v}" for k, v in e.items() if k not in ("t", "type"))
                for e in es[-args.last:]))


def cmd_metrics(args):
    print(json.dumps(Coord(args.coord).get("/metrics"), indent=1))


def cmd_status(args):
    print(render(Coord(args.coord).get("/state")))


def cmd_watch(args):
    c = Coord(args.coord)
    t_end = time.time() + args.timeout
    last = None
    while time.time() < t_end:
        try:
            st = c.get("/state")
        except OSError:
            time.sleep(0.5)
            continue
        frame = render(st)
        if frame != last:
            print("\033[2J\033[H" + time.strftime("%H:%M:%S") + "\n" + frame, flush=True)
            last = frame
        if st["goals"] and all(g["status"] == "done" for g in st["goals"]):
            print("\n-- every goal answered by the assembled swarm --")
            return
        time.sleep(0.5)
    print("\n-- watch timeout --")


def main():
    ap = argparse.ArgumentParser(prog="slime")
    sub = ap.add_subparsers(dest="cmd", required=True)
    for name in ("seed", "runner", "pool", "ask", "status", "watch", "promote", "metrics",
                 "control", "immune", "report"):
        p = sub.add_parser(name)
        p.add_argument("--coord", required=True)
        if name == "seed":
            p.add_argument("--manifest", default=str(HERE / "manifest.json"))
            p.add_argument("--genome", default=str(HERE / "genome.json"))
            p.add_argument("--admin-token-file", default=None)
        if name in ("runner", "pool"):
            p.add_argument("--node", default=None)
        if name == "pool":
            p.add_argument("--slots", type=int, default=10)
        if name == "promote":
            p.add_argument("--admin-token-file", required=True)
        if name in ("control", "immune"):
            p.add_argument("--set", action="append", default=[], metavar="KEY=VALUE")
            p.add_argument("--admin-token-file", default=None)
        if name == "report":
            p.add_argument("--hours", type=float, default=8.0)
            p.add_argument("--last", type=int, default=8, help="list the last N of each kind")
        if name == "immune":
            p.add_argument("--pardon", action="append", default=[], metavar="HOST")
            p.add_argument("--forget", default=None, metavar="all|ID",
                           help="drop remembered poison inputs")
        if name == "ask":
            p.add_argument("text")
            p.add_argument("--wait", action="store_true")
        if name == "watch":
            p.add_argument("--timeout", type=float, default=90.0)
    args = ap.parse_args()
    {"seed": cmd_seed, "runner": cmd_runner, "pool": cmd_pool, "ask": cmd_ask,
     "status": cmd_status, "watch": cmd_watch, "promote": cmd_promote,
     "metrics": cmd_metrics, "control": cmd_control, "immune": cmd_immune,
     "report": cmd_report}[args.cmd](args)


if __name__ == "__main__":
    main()
