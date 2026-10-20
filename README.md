# electric-slime

A self-assembling model scaffold. A pool of small, generic runner slots share one
networked arena (the coordinator). Each base agent runs a small model **and carries a
shard of the next, larger one**. When a quorum of carried shards exists and resources
permit, one agent is elected to stitch the next model, verify it, and promote the swarm;
lower tiers then trim to free resources, always keeping enough replicas to serve and
enough shards to rebuild. The top tier uses the lower tiers as subagents: it splits a
goal into subtasks, the small models answer them, and it writes the final answer.

Design goals: **model-agnostic** (a shard is a content-hashed byte range; a model is the
verified concatenation of its shards), **hardware-agnostic** (amd64 bare metal, Apple
Silicon, anything with Python 3.9 and a llama.cpp build), **CPU-only** (no GPU assumed),
and **light**: native processes under the host's own supervisor, no VMs.

**Status (2026-10-20): paused.** After a 6-hour soak (57 goals, none failed) the swarm was
shut down: runner and coordinator services were removed or disabled on every host, model
caches kept. Host names and addresses below describe the home lab it ran on; the
[Measured](#measured-2026-10-19-and-the-night-after) section is the record of what it did.
To bring it back: `systemctl enable --now slime-coord` on the coordinator host, then each
host's `native/*/slime-node.sh install` (or `systemctl enable --now slime-runner` on a stick node).

## v1: real models (live 2026-10-19)

| Tier | Model (official GGUF, Apache-2.0) | Size | Cost (MB) | How it exists |
|---|---|---|---|---|
| 0 | Qwen3-0.6B Q8_0 | 0.64 GB | 1100 | whole blob in the depot |
| 1 | Qwen3-1.7B Q8_0 | 1.83 GB | 2600 | 4 shards, carried by tier 0 |
| 2 | Qwen3-4B Q4_K_M | 2.50 GB | 3600 | 3 shards, carried by tier 1 |
| 3 | Qwen3-8B Q4_K_M | 5.03 GB | 6400 | 2 shards, carried by tier 2; **on demand** (escalation) |

**Added overnight 2026-10-19 → 20** (each has its section below): the
[control loop](#control-loop-swarm-size) sizes the swarm; the [immune system](#immune-system-innate)
(health ledger, screening, poison cap, adaptive memory, sleep and pressure awareness);
[quality escalation](#quality-escalation-on-demand-tier) to an on-demand 8B, triggered by
known-answer canaries; [speed-aware placement](#scheduling); an event log, `slime.py
report` and a [dashboard](#run-it); snapshot writes outside the arena lock; and
[`selftest.py`](#self-test). Every bug the night's soak and chaos tests found is in
[Measured](#measured-2026-10-19-and-the-night-after) or beside the rule it changed.

A stitched model must hash to **the publisher's SHA-256** (from Hugging Face) before
anything runs it. Inference is a native `llama-server` per agent on loopback (llama.cpp
b11399, `--reasoning-budget 0`, CPU unless `SLIME_GPU_LAYERS` says otherwise).

```
          ┌───────────────────────── coordinator (coord.py, relay) ─────────────────────────┐
          │ wants[tier] · budget + per-host ledger · election · goals/tasks · depot /blob/<sha> │
          └───▲────────────▲─────────────▲────────────────────▲─────────────────────────────┘
              │ assign     │ deposit     │ elect/model_ready   │ heartbeat · /node (cache, peer)
   ┌──────────┴──┐  ┌──────┴──────┐  ┌───┴─────────┐  ┌───────┴─────┐
   │ m1 (launchd)│  │neo (launchd)│  │T450-a (sysd)│  │T450-b (sysd)│   runner pools: slots +
   │ llama-server│  │ llama-server│  │ llama-server│  │ llama-server│   a peer server for their
   └──────▲──────┘  └─────────────┘  └──────▲──────┘  └─────────────┘   cache (blobs by sha)
          └────────────── shards/models peer-to-peer, sha-verified ──┘
```

### The dance
```
seed → wants{t0:5} → runners claim t0: fetch the 0.6B, carry one 1.7B shard each
   → quorum: 4 distinct shards carried by live agents → one elected → stitch + verify
   → wants{t0:3, t1:3} → surplus t0 retire; idle runners embody t1 (cache-aware),
     each carrying one 4B shard → quorum(3) → stitch + verify → wants{t0:3, t1:2, t2:1}
     (a host that can fit the 4B only by shedding lower agents sheds them: eviction)
   → t2 claims a goal → plans 3–5 subtasks (JSON-schema constrained), easy→t0, hard→t1;
     each subtask carries the whole question as context (a sub-question cut loose from it
     — "what does the ball cost?" — is unanswerable for a small model)
   → subagents answer; every tier-0 answer is reviewed by tier 1 (own answer first, then
     a verdict), each review posted the moment its answer lands → t2 synthesises,
     preferring corrections → answer stored on the goal
   → from here the controller grows and shrinks each tier above that base with demand
   → quality stays poor (or an admin says so): escalate → t2 grows to 2 carriers →
     quorum(2) → stitch the 8B + verify → t3 leads (plans with an 'expert' level for t2,
     synthesises); t2 serves under it → relax after the hold: t3 retires, t2 leads again
```

### Invariants
- **Serve floor:** `/retire` is a right-to-die lease, honored only if `live > want`.
- **Quorum:** k *distinct* shards carried by *live* agents, plus k live replicas below.
- **One assembler per transition:** `/elect` returns `won` to exactly one caller; if the
  assembler dies mid-stitch its election is released and the tier re-elects.
- **Verify before invoke:** every blob is checked against its sha on arrival (from a peer
  or the depot), and the stitched model against the publisher's sha.
- **Nothing strands:** an agent that misses heartbeats for 10 s is reaped; its tier is
  refilled, its in-flight task re-queued, its goal re-claimable. **Nothing loops either:** a
  task or goal is given up after 3 failed attempts (see the immune system).
- **Leaving is explicit:** a runner stopped with SIGTERM (deploys, service stops) hands its
  agents back at once, so they are re-placed immediately and nobody is blamed; the task or
  goal an agent was holding goes back to the queue without costing it an attempt.
- **Admin gate:** `/seed` (wipes all state) and `/promote` need `X-Slime-Admin` matching that
  coordinator's own `admin.token` (mode 600, never leaves its host). Posting questions, GETs
  and model blobs stay open on the LAN/tailnet by design.
- **Liveness is separate from work:** heartbeats run on their own thread, so a multi-GB
  download or a long generation never looks like death.
- **Lower tiers make way:** if a higher tier has been short for 5 s and no eligible host has
  room, a host that could fit it by retiring lower-tier agents does so (fewest evictions),
  through the same `/retire` lease. The evicted tier refills elsewhere.

### Identity
Every host has a 32-byte key (`host.key`: user-only on the Macs and framework, root:slime 640
on the T450s), enrolled with both coordinators by `enroll-host.sh` over ssh — keys never hit
a screen. Every runner call carries `X-Slime-Host/Time/Sig`, an HMAC-SHA256 over method,
path, timestamp and body hash. The coordinators run `--auth required`: unsigned, unknown,
stale (>30 s skew), tampered and **replayed** calls get 401, and a host may act only on its
own agents (403 otherwise). Only the elected assembler may declare a model ready, and only a
goal's orchestrator may post its subtasks or its answer. New sticks get their key minted and
enrolled by `personalize.sh`. Revoke with `./enroll-host.sh --revoke HOST`.

### Coordinator pair
The relay is **primary**; framework runs a **standby** (`native/linux/coord-standby.sh`) that
copies the primary's state every 3 s, serves its own depot (its runner cache holds every
shard), and answers work with 503 until promoted. Runners list both
(`--coord http://relay:8700,http://framework:8700`) and follow whichever answers as primary.
- **Fail over** (relay down): on framework,
  `slime.py promote --coord http://127.0.0.1:8700 --admin-token-file ~/.local/share/electric-slime/admin.token`.
  Promotion gives every replicated agent a fresh lease, so nothing is re-embodied.
- **Fencing:** each promotion takes an epoch one past anything either side has seen. A
  coordinator that finds the other primary at a higher epoch (or equal, promoted later)
  steps down; a restarted old primary checks before serving and comes up as standby.
- **Fail back:** promote the relay the same way (its token, on the relay).
- Promotion is manual by design ("cold"): `--promote-after S` would automate it, at the cost
  of a split brain if the standby is the one that lost the network.

### Immune system (innate)
The coordinator keeps a **health ledger** per host. Incidents add to a score that halves
every 10 minutes:

| Incident | Weight | Raised when |
|---|---|---|
| lapse | 1.0 | a host's agents stop heartbeating without leaving |
| embody_failed | 1.0 | an agent can't load its model three times running |
| bad_output | 1.0 | an answer, review or synthesis fails **screening** |
| task_error | 0.5 | the host's model errors on a task or goal |
| bad_blob | 1.5 | a peer reports the host served bytes that failed their sha |

- **Quarantine** at 3.0, **release** below 1.0 (by itself, as the score decays). A
  quarantined host gets no agents, claims no tasks or goals, wins no elections and serves no
  blobs to peers; its agents retire — except the **last refuge** of a tier (better a suspect
  orchestrator than none: its outputs are still screened). A host with any recent score
  is offered new agents only after healthy hosts have had 10 s to take them.
- **Screening** (cheap, model-agnostic): empty output, degenerate repetition (compresses
  below 12%), mostly-repeated lines, a review that isn't `{own_answer, agrees}` JSON. A
  rejected task goes back to the queue and the host that failed it is **banned** from it for
  30 s, alternatives or not (otherwise a sick host holding the only agent of a tier burns all
  three attempts in three seconds before quarantine can move the work). None of the swarm's 100 real
  outputs to date would have been rejected.
- **Poison:** a task or goal that fails 3 times is given up (`failed`; the orchestrator
  synthesises without it, a goal closes with "no answer"). A host is charged once per
  task, and when a task failed on several hosts the **task is the culprit**: their charges
  are refunded.
- **Is it me or them:** lapses are charged 10 s later, and only if under half the active
  hosts lapsed together (otherwise it's the coordinator's own link: `mass_lapse`, nobody
  charged); never in the 30 s after a coordinator starts or is promoted. **One lapse per
  host per incident:** a host's agents die together but are reaped over a second or two
  (their last heartbeats differ); the first chaos kill of the overnight soak charged m1
  three times for one SIGKILL, inflamed the swarm and quarantined it — fixed, and the
  rerun on neo cost exactly one lapse.
- **Adaptive memory:** an input given up as poison (a task that failed on two or more hosts,
  a goal whose orchestrator's model kept erroring on it) is remembered by a hash of its
  normalised text and refused on sight next time: the task is born `failed`, the goal
  closes at once with "known poison input". Up to 500 inputs, a week each;
  `slime.py immune --forget all|ID` clears them.
- **Sleep isn't a fault:** a laptop that suspends (lid shut) stops heartbeating. On waking,
  its runner notices — the monotonic clock stops during suspend, the wall clock doesn't —
  and reports the window; lapses inside it are dropped or taken back (at their decayed
  weight) and don't count toward inflammation. (A host could claim to have slept to dodge
  a lapse; lapses are the mildest signal, and screening and reports don't rely on them.)
- **Reports** (bad blobs) count once per reporter and peer per 10 minutes, and never about
  oneself, so one host can't sink another alone.
- **Inflammation:** 3+ live incidents in 5 minutes inflame the swarm: quarantine comes at
  2.0 instead of 3.0, and orchestrators have **tier-1 answers cross-checked** too (calm:
  tier-0 only). Every review runs on another host than the answer it checks — while another host *can* take
it (when speed placement put both 1.7Bs on m1, reviews of m1's answers waited out the 30 s
avoid window: tier-1 wait p90 30.1 s, 1.4 s after the fix).
- **Memory pressure** (runner, from the OS's own signal — macOS memorystatus, Linux PSI):
  under `warn` the host takes no new agents; under `critical` it sheds its lowest-tier
  agent, one per 30 s, and the swarm re-places it. Pressure is not an incident.
- **Thermal pressure:** a CPU being throttled (Linux: the package throttle counter rising
  by 50+ in 30 s, or the package at 95 °C; macOS: a recorded CPU speed limit) is `warn`:
  nothing new lands there until it cools. Heat never sheds work. (T450-a had throttled
  ~4000 times since boot when this went in.) `status` shows a squeezed host as
  `[warn thermal]` / `[critical memory]`.
- **Scale-in** retires replicas on quarantined, then pressured, then least healthy, then
  slowest hosts first.

`slime.py immune --coord URL` shows the ledger, recent incidents and poison;
`--pardon HOST` clears one, `--set half_life=900` etc. change settings (admin token, so on
the relay). Settings and the ledger replicate to the standby.

### Persistence & metrics
Acknowledged work (seed, goals, plans, results, elections, promotions) is fsync'd before the
reply; the poll chatter (assign/claim/retire, several a second per slot) is batched into at
most one snapshot a second; heartbeats are never written. Snapshots are serialised under
the arena's lock (milliseconds) but written and fsync'd outside it, in order (an older one
never replaces a newer one; an acknowledged write overtaken by a newer snapshot still waits
for an fsync): a slow disk delays only the caller that needs durability, never heartbeats.
(Before this, an image build on the relay starved the coordinator for >10 s and every host
lapsed at once; under 3 GB of concurrent fsync'd writes afterwards, nothing lapsed.)
Finished goals beyond the newest 50 move, with their tasks, to `goals-archive.jsonl`. `slime.py metrics` (`GET /metrics`):
queue depth and oldest wait per tier, per-tier wait/run/tok/s over 5 minutes, work per host,
review disagreement rate, goal latency, and any unsigned callers. Every event (work done,
scaling, escalation, incidents, quarantines, sleeps, leaves) is also appended to
`events.jsonl` next to the state (50 MB, one rotation kept): `GET /events?since=T`, and
`slime.py report --hours 8` summarises it — goals and latency percentiles, subtasks and
tok/s per tier and host, review disagreement, every scaling decision, the immune record.

### Control loop (swarm size)
Assembly leaves each tier at its **base** want (the top tier's `target`, `serve_floor` below
it). From then on a controller in the primary coordinator (every 2 s) moves each want
between that base and the tier's `max` (manifest; default twice the base):
- **out:** work has waited `up_after` s and every agent that could take it is busy →
  +1 on the **cheapest tier licensed** for it (an easy subtask grows tier 0; a hard one or
  a review, tier 1; a waiting goal, the orchestrating tier). Only when the replica fits
  right now: within the budget, on an eligible host with memory and an idle slot.
  **Scaling out never evicts**; eviction is only for restoring a tier to its base.
- **in:** a scaled-out tier has had an idle agent and nothing it could take for
  `down_after` s → −1; the idle agent retires, busy ones finish first.
- **damping:** one raise per pass (subtasks of goals in flight before another
  orchestrator), one step per tier per `cooldown` s, none while a replica of that tier is
  still loading, and a raise nobody takes within `place_timeout` s is withdrawn.
- **no doubled orchestrators:** an extra orchestrator goes only to a host not already
  running one. Two on one CPU just split it: a burst of three finished at 73/93/125 s that
  way (one 4B synthesis took 70 s), where one orchestrator taking them in turn finishes the
  first two sooner.

Live settings: `up_after` 10 s, `down_after` 120 s, `cooldown` 30 s, `place_timeout` 90 s;
max 6/4/2; budget 30000 MB (hosts offer ~35.7 GB). A raise that was withdrawn isn't retried
for twice the placement timeout (no flapping). `slime.py control --coord URL` shows them,
the base and current wants, and the last decisions (also in `status` and `metrics`); `--set enabled=off|on`, `--set max.1=3`,
`--set down_after=300`, `--set budget=N` change them (admin token, so run it on the relay:
`ssh relay 'cd ~/electric-slime && python3 slime.py control --coord
http://127.0.0.1:8700 --admin-token-file admin.token --set …'`). Off returns every tier to
its base. Settings and decisions replicate to the standby.

### Quality escalation (on-demand tier)
A tier marked `on_demand` (tier 3, the 8B) is carried but not assembled: every 4B agent
holds one of its two 2.5 GB shards (fetched in the background so the 4B serves meanwhile;
`/elect` refuses the dormant tier). The controller escalates when **review disagreement**
(tier-1 reviewers rejecting tier-0 answers) is at least `escalate_at` 35% over at least
`escalate_min_reviews` 8 reviews in the last `escalate_window` 30 minutes — the small models
keep contradicting each other, so a bigger model should plan and synthesise — or when an
admin sets `escalate=on`:
- not yet stitched: tier 2's want rises to the quorum (2, and that is its base until the
  8B exists, so scale-in can't break the quorum) → elect → stitch → verify against Qwen's
  sha → tier 3's want 1.
- stitched before: tier 3's want goes straight to 1; the model is in the cache already.
- **Leadership follows readiness:** the tier that orchestrates is the highest one *up and
  running*, so the 4B keeps leading while the 8B loads, then serves under it (and takes
  the plan's `expert` sub-questions). Reviews go one tier up (tier 0 by 1, 1 by 2).
- **Relax** `escalate_hold` (30 min) after the last reason to stay, or when an admin sets
  `escalate=off`: tier 3's want 0, it retires, the 4B leads again. `escalate=auto` returns
  control to the quality signal.

**Canaries** are the sharper signal. Review disagreement only sees what tier 1 can tell is
wrong, and the small reviewers rubber-stamp (0% disagreement over every review on the first
night). So when the swarm is idle — no goal waiting, no subtask running — the coordinator
asks one of eight known-answer questions from `canaries.json` (bat and ball, the r's in
"strawberry", 5 machines/5 widgets, 17 × 23, Everest's boiling point, …) every
`canary_every` 15 minutes, and checks the answer against a pattern. If at least
`canary_fail_at` 50% of the last `canary_min` 4+ canaries answered by the *current* lead tier
were wrong, that escalates too. Judging only the current lead means that after a relax the
4B must fail fresh canaries before the 8B comes back: a periodic fair retry of the cheaper
setup rather than a flap on old results. Canaries never compete with real work and are left
out of goal latency; `report` and the dashboard show their pass rate. Alongside each, the
same question goes **straight to each serving tier** as a probe subtask only that tier may
take, so the report also says which model falls for which trap ("tier0 alone: wrong on
strawberry, months"). Probes inform; they never steer escalation or count as demand.

`slime.py control --coord URL` shows the escalation state, the current lead, and the
disagreement and canary failure it is judging on; `--set escalate=on|off|auto`,
`--set canary_every=0` (off) (admin token).

### Scheduling
- **Budget:** swarm-wide `resource_total` (30000 MB) and per host: each runner pool
  advertises a capacity (`SLIME_CAPACITY_MB`, default 80% of RAM or the cgroup limit) and
  the coordinator never places more than that on it.
- **Cache-aware FCFS:** for 30 s a short tier is kept for a live host that already holds its
  model and has room (its slot may be freeing that moment); after that, first come, first
  served. Carried shards prefer one the host has; a host holding the stitched model carves
  its shard out of it locally (verified) instead of downloading.
- **Per-host ceiling:** `SLIME_MAX_TIER` (T450s: 1), so the orchestrator never lands on two
  Broadwell cores while m1 could host it.
- **Speed-aware:** the coordinator learns each host's speed from the tok/s of its recent
  work (subtasks, and the leader's syntheses), as a score against the swarm's median per
  tier. A host offered a short tier yields for 8 s to an idle host with room that would run
  it 1.5× faster. And when a tier is calm with a replica on a host 2× slower than an idle
  eligible one, the controller adds a replica (it lands on the fast host) and the ordinary
  idle scale-in retires the slowest — at most once per tier per 15 minutes. Live scores
  (2026-10-20): m1 4.1, framework 1.5, T450-a 1.0, T450-b 0.6; `slime.py control` shows them.
- **The lead goes to the fastest host:** planning and synthesis are most of a goal's time,
  so when a host 2× faster than the leading tier's could fit it by shedding *idle*
  lower-tier agents that fit elsewhere, the controller reserves that host for the lead
  tier (it takes nothing else, nobody else takes the lead), retires those agents (their
  tiers refill elsewhere), adds a lead replica there, and lowers the want again once it
  is up. **Scale-in retires the worst-ranked replica even if it is busy** — it goes when
  its goal is done, and the others wait for it — so a fast newcomer is never the one cut.
- **Prefetch:** idle hosts warm their cache with every model they may run (that fits their
  capacity), one at a time, so embodiment and failover cost seconds, not GBs.
- **Peer-to-peer data:** every pool serves its cache read-only (`:8701`, `/blob/<sha>`),
  announcing LAN and tailnet addresses; downloads try holders first (fastest-measured
  first; unmeasured mid-pack), the depot last, and
  **resume** (`Range`) from wherever they stopped, across sources and restarts. Every byte
  is sha-verified whatever its source, so a bad or truncated peer costs time, never
  correctness. The depot (relay, on wifi) is the slowest link in the house, so this matters.

### Measured (2026-10-19 and the night after)
| What | Result |
|---|---|
| Stitch Qwen3-1.7B from 4 carried shards | 1.83 GB, sha256 == Qwen's published `061b54da…` |
| Stitch Qwen3-4B from 3 carried shards | 2.50 GB, sha256 == Qwen's published `7485fe6f…` |
| Question → answer (4B plans, 3–4 subtasks, review, 4B synthesises) | 34–44 s |
| Generation, CPU only (median tok/s) | m1: 0.6B 97–116, 1.7B 48 · neo: 1.7B 29 · T450: 0.6B 15, 1.7B 9 |
| Typical subtask | m1 0.3–1.5 s · neo 2–3 s · T450 4–12 s |
| Reseed → tier1 ready, caches warm (shards carved from local models) | 5 s |
| Reseed → tier2 ready, before carried-shard prefetch existed | 380 s (tier1 agents downloading 832 MB shards) |
| 3 questions queued at once (one orchestrator, subtasks across all 4 hosts) | answered 32 s, 97 s, 125 s after posting |
| m1's runner unloaded entirely | 4B re-embodied on framework from its cache in 14 s; a question answered in 38 s with no m1 |
| SIGKILL the whole m1 runner mid-goal (all 3 tiers incl. orchestrator) | launchd reaps its llama-servers, restarts it; re-embodied from cache in ~12 s; goal finished 38 s after posting |
| 6 goals at once, stand-in tiers at 4 s/call (local) | controller off 112 s · on 68 s (tier 1 2→4, tier 2 1→2, back to base after idle) |
| 4 real questions at once (live) | second 4B orchestrator on framework 2 s after the oldest goal had waited 12 s; answered at 73/84/125/148 s; back to one 4B 120 s after the queue emptied |
| Escalate to the 8B (live, admin-forced; both shards and the stitched model already prefetched) | 3 s from `escalate=on` to the 8B leading on framework; relax 3 s back to the 4B |
| First prefetch of the 8B | framework: shards from the relay depot at 11.5 and 4.7 MB/s (218 s, 538 s); m1 stitched it first, then framework took the whole verified model from m1 at ~20 MB/s (257 s) |
| "Bat and ball" puzzle, one run each | 8B leading: 111 s, opened with $0.10 then derived $0.05 · 4B leading: 67 s, $0.05 · 4B + subtasks with context: 81 s, $0.05, every sub-answer on topic (before: the 1.7B, asked "what is the cost of the ball?" alone, said $10) |
| Reseed → tier 2 ready, every model cached | 6 s |
| Speculative decoding for the 4B's synthesis (m1, CPU, 4 threads, real 337-token prompt) | baseline 19.8 tok/s · 0.6B draft ×8: 8.8–12.5 (30–47% accepted) · 0.6B draft ×3: 16.7 · n-gram (`ngram-simple`): 20.0–20.4 (noise) · `ngram-cache`: 15–16 → not adopted: on CPU the draft competes for the same cores |
| Hourly burst of 3 questions (soak, live), as tonight's fixes landed | 00:47 60/141/151 s (avg 117) · 01:47 50/111/161 (107) · 02:47 73/93/125 (97; after context + pipelined reviews + speed placement) · 03:47 53/78/119 (83; + review-host fix, no doubled orchestrators) · 04:47 36/82/91 (70; the lead 4B now on m1, the burst's second 4B on framework: two machines, real parallelism) |
| Coordinator load (relay i5-7300U) | ~38 MB; 9.6% of one core → 7.6% with idle slots polling every 2 s instead of 0.5 s; handlers ~0.02 ms each, a control pass ~0.4 ms, a snapshot ~2 ms (measured on m1) |
| Overnight soak, live (23:47–05:47: a question every 8 min, a burst of 3 hourly, two runner SIGKILLs, a coordinator failover drill, eleven deploys) | 57 goals answered, none given up, no failed subtask; p50 50 s, p90 111 s; median of the first 12 goals 60 s → last 12, 40 s |
| Canaries over the night: the swarm vs its parts on the same known-answer questions | **whole swarm 12/14** (wrong only on strawberry) · 1.7B alone 10/12 (Everest, strawberry) · 0.6B alone 7/12 (bat and ball, Everest, feathers, months, strawberry) — the swarm beats each of its members |
| Where a goal's time goes (after tonight's fixes) | plan 7–20 s, subtasks + reviews 5–9 s, synthesis 22–70 s (4B on framework, ~14 tok/s; answers ~230 words) |
| Chaos: SIGKILL a runner (soak, live) | m1 (01:47): its three agents re-placed on framework/T450-a within ~20 s, goal in flight finished — and a double-charging bug found (above) · neo after the fix: one lapse, re-placed in ~20 s |
| Coordinator failover drill (relay stopped → framework promoted → relay back → failback) | no agent lost; questions answered in 30 s (framework primary) and 38 s (after failback); restarted relay came up as standby by itself |
| Blob transfer: relay depot → T450 | 1–2 MB/s (relay wifi is the bottleneck) |
| Blob transfer: peer → peer | m1/neo → T450-b 21–30 MB/s; m1 → neo 11–12 MB/s |
| v1 stick image, SeaBIOS VM | boots, joins the live swarm in 19 s, fetches the 0.6B from a peer, answers in 1 s |

**Checking answers, honestly:** small-model review catches only errors the reviewer doesn't
share. Asked the boiling point of water on Everest, the 0.6B said 373 °C, then 100 °C on a
rerun; the 1.7B reviewer agreed with the 100 °C (same misconception); the 4B synthesiser
wrote ~70 °C both times. The top tier is the check that works; the review is cheap (~1 s) and
kept for the errors it does catch.

## Self-test
`python3 selftest.py` (about 25 minutes, all on loopback, nothing touches the live swarm):
unit checks of the screen, decay, mass and staggered lapses, sleep excuses, poison refund,
adaptive memory, upgrading old state, snapshot ordering and a slow disk, report dedupe,
scale-in order, speed-aware placement, lead relocation and controller withdraw; then
scenarios with a private coordinator (auth required) and
stand-in hosts made slow, faulty or squeezed on purpose (`SLIME_STANDIN_DELAY`,
`SLIME_FAULT=garbage|error|poison:TEXT|poisongoal:TEXT`, `SLIME_PRESSURE_FILE`): assemble
and answer, control burst, bad-output quarantine and release, inflammation, poison and
adaptive memory, SIGTERM vs SIGKILL, every host freezing at once, memory pressure, forced
and automatic escalation to an on-demand tier, and a coordinator pair failing over
(promote, same agents and settings, the old primary returning as standby).
`python3 selftest.py unit poison` runs a subset. Last full run: 28/28 (03:50).

## Run it
```sh
# first: cp site.env.example site.env and fill in your coordinator addresses (gitignored)
# relay: fill the depot once, then the coordinator runs as relay/slime-coord.service
python3 coord.py stage --manifest manifest-v1.json --depot depot
python3 slime.py seed  --coord http://127.0.0.1:8700 --manifest depot/manifest.staged.json \
                       --admin-token-file admin.token     # /seed wipes the swarm: token-gated

# runner hosts
native/macos/slime-node.sh install            # Macs: launchd agent (see native/README.md)
native/linux/slime-node.sh install            # Linux desktops: systemd --user, no root
# T450 USB nodes run slime-runner.service from the image (node-image/)

# dashboard: $COORD/ (LAN) or $COORD_TAILNET/ (tailnet), from site.env —
# tiers, hosts and their agents, the immune ledger, goals and answers, the controller's
# decisions, and a box to ask a question. Read-only apart from asking, like the CLI.
. ./site.env; C=$COORDS                                      # primary, standby
python3 slime.py ask     --coord $C "your question" --wait
python3 slime.py status  --coord $C
python3 slime.py metrics --coord $C
python3 slime.py control --coord $C           # scaling settings, base vs current wants, decisions
python3 slime.py immune  --coord $C           # health ledger, quarantines, poison
python3 slime.py report  --coord $C --hours 8 # what happened overnight
./deploy.sh all                               # push code changes (both coordinators + runners)
ssh HOST 'cat <its host.key>' | ./enroll-host.sh HOST    # admit a new host
```
Stand-in mode (no models, instant): seed `manifest.json` instead; same dance, stub
inference. `slime-v0.py` (the first single-host filesystem arena) is kept for reference.

## Roadmap
- **deterministic tools for the planner** (tested, not built): offered `count_letters` and
  `arithmetic` in its plan schema, the 4B (temperature 0) chose `count_letters r|strawberry`
  and `arithmetic 17*23` — fixing two canaries the swarm fails — and no tool for four
  ordinary questions; but for "17 sheep, all but 9 run away" it chose `17-9`, which would
  turn a right answer wrong. A tool executes the planner's reading faithfully, misreadings
  included, so results should reach the synthesiser as evidence beside the subagents'
  answers, not as overrides.
- **immune system, next:** a semantic screen (does the answer address the question?) by
  the cheapest model; antibodies shared with the standby are already replicated, but
  near-duplicate poison (paraphrases) still gets through.
- **quality signal, next:** a larger, rotating canary set (eight questions invite
  overfitting), and factual canaries — the overnight soak's answers were fluent and mostly
  right, but one had lithium ions moving anode→cathode *on charge* (backwards), which no
  reviewer caught and no reasoning-trap canary would.

## License

MIT (see `LICENSE`). The Qwen3 models it downloads are Apache-2.0 and llama.cpp is MIT; neither is
included in this repository.
