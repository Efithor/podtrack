# podtrack

A shared, ownership-aware registry for RunPod pods — so multiple agent streams
(Claude Code sessions) can share one RunPod account without losing, orphaning, or
killing each other's pods.

## The problem it solves

With a single mutable `pod_state.json` and no ownership, concurrent agent streams:
- **lose track of pods** — one deploy overwrites another's state → the first pod is
  orphaned and bills silently;
- **kill each other's pods** — teardown reads "the" pod with no owner check;
- **leave CPU/idle pods running for hours** — nothing verifies a pod is on GPU and busy.

podtrack replaces that file with a concurrency-safe **SQLite registry** (WAL mode) that
records *who owns each pod* and refuses cross-owner teardowns.

## Three jobs (v1)

1. **Registry** — one row per pod, shared across streams.
2. **Ownership** — every pod is owned by `(session-UUID, friendly-label)`; teardown is
   owner-guarded (refuses to kill another stream's pod without `--force`).
3. **Reconcile** — diff the registry against the live RunPod account: catch
   leaked/untracked pods, mark vanished ones terminated, flag GPU-vs-CPU / idle pods.

## Credential mandate

podtrack is the **custodian of the RunPod API key**. `podtrack adopt-key` moves
`~/.keys/runpod` into podtrack's private store (`~/.config/podtrack/runpod.key`, mode
600) and replaces the old path with a notice. Thereafter the *only* sanctioned way to
reach RunPod is through this module — so a pod cannot be created or destroyed without
being registered. Anything still reading the old path fails loudly with a pointer here.

## Identity

Each agent stream is `(owner_uuid, owner_label)`:
- **UUID** (for safety/uniqueness): `$PODTRACK_OWNER_UUID`, else `$CLAUDE_SESSION_ID`,
  else a persisted per-install fallback (warns).
- **Label** (for legibility): `$PODTRACK_LABEL` (e.g. `ti2mnfe-melt`).

Set both at session start:
```bash
export PODTRACK_OWNER_UUID=<your-cc-session-uuid>
export PODTRACK_LABEL=ti2mnfe-melt
```

## Usage

```bash
python podtrack.py adopt-key                 # one-time: take custody of the RunPod key
python podtrack.py whoami                     # show resolved owner identity
python podtrack.py register <pod_id> --gpu H100 --ssh-ip 1.2.3.4 --ssh-port 15863
python podtrack.py list [--all|--mine|--others]
python podtrack.py heartbeat <pod_id>
python podtrack.py teardown <pod_id>          # owner-guarded; --force to override (logged)
python podtrack.py reconcile                  # read-only diff vs RunPod; flags leaks/idle
python podtrack.py reconcile --terminate-untracked   # also kill leaked pods
```

Library:
```python
from podtrack import Registry
reg = Registry(owner_label="ti2mnfe-melt")
reg.register(pod_id, gpu_type="H100")
print(reg.reconcile())          # {'live':[...], 'untracked':[...], 'vanished':[...], 'idle_or_cpu':[...]}
```

## Storage

- Registry DB: `~/.local/share/podtrack/pods.db` (override with `$PODTRACK_HOME`).
  Tables: `pods` (one row per pod) and `events` (append-only audit of every action).
- Credential: `~/.config/podtrack/runpod.key` (after `adopt-key`).

## Integration (follow-up)

`dft2mlip-melter/infrastructure/pod_*.py` should be refactored to register on deploy,
heartbeat during runs, and teardown through podtrack — replacing `pod_state.json`.
`pod_audit.py` is largely subsumed by `reconcile`.

## Not yet (planned)

Heartbeat-driven orphan detection, GPU-utilization/idle auto-teardown, and cost
accounting/alerts. v1 is registry + ownership + reconcile.
