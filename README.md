# podtrack

A shared, ownership-aware registry **and autonomous reaper** for RunPod pods — so
multiple agent streams (Claude Code sessions) can share one RunPod account without
losing, orphaning, or killing each other's pods, and without leaving idle GPUs billing.

## The problems it solves

With the old single `pod_state.json` (one pod, no ownership, no locking), concurrent
streams **lose track of pods** (a deploy overwrites another's state → orphaned billing),
**kill each other's pods** (teardown with no owner check), and **leave CPU/idle pods
running for hours**. podtrack replaces it with a concurrency-safe **SQLite registry**
(WAL) plus an autonomous reaper.

## How it answers the hard cases

**UUID vs label on `/resume`.** The session UUID is *ephemeral* (changes when you exit
and resume, or start fresh to continue the same work); the **label is the stable task
identity**. Ownership continuity follows the **label**: a resumed session with a new UUID
but the same `PODTRACK_LABEL` automatically **reclaims** its pods (logged), while a
genuinely different label is refused. `podtrack claim` handles explicit handoffs/orphans.

**CPU-only / idle GPU.** Detected with two signals to avoid false kills: GPU utilization
(RunPod API `gpuUtilPercent`, or a definitive SSH `nvidia-smi` via `podtrack probe`) **and**
a job heartbeat. A pod is "idle" only when GPU util ≈ 0 **and** no recent heartbeat — so a
job legitimately busy on CPU isn't killed.

**`--kill-in X` without losing artifacts.** Two layers:
- *Continuous mirror* — the reaper rsyncs each live pod's artifacts to local every cycle,
  so even an ungraceful death loses at most one interval.
- *Safe teardown* — every teardown is **pull → verify non-empty → kill** (never a bare
  kill). On sync failure it leaves the pod UP and alerts.
- *Hard backstop* — artifacts live on the **RunPod network volume** (which survives the
  pod), so any kill is data-safe even if local sync hasn't run.

## Architecture: local brain + pod-side dead-man switch

- **Local reaper** (systemd timer on the home box) is the brain: it owns the key + registry
  + local artifacts, reconciles, mirrors, pets healthy pods, and safe-tears-down expired/
  idle ones. Runs whenever the machine is up.
- **Pod-side dead-man switch** (`deadman.sh`, planted by `arm`) is the backstop for when the
  home box is asleep: the pod self-terminates at its `deadline` **unless** the reaper keeps
  pushing the deadline forward (pets it). Default-to-death → the GPU bill is bounded no
  matter what; the network volume keeps the data.

## Credential mandate

podtrack is custodian of the RunPod key: `adopt-key` moves `~/.keys/runpod` into
`~/.config/podtrack/runpod.key` and leaves a notice, so the only sanctioned path to RunPod
is through podtrack. The dead-man switch keeps its credential on the pod's **tmpfs**
(`/dev/shm`, RAM only) and `shred`s it after firing. *Hardening:* pass `arm --token-file`
with a **restricted RunPod token** (create one in the RunPod console) so the full account
key never lands on rented hardware.

## Identity

`(owner_uuid, owner_label)`: UUID from `$PODTRACK_OWNER_UUID` / `$CLAUDE_SESSION_ID`
(fallback persisted, warns); label from `$PODTRACK_LABEL`. Set both at session start:
```bash
export PODTRACK_OWNER_UUID=<cc-session-uuid>
export PODTRACK_LABEL=ti2mnfe-melt
```

## Commands

```bash
podtrack adopt-key                          # one-time: take custody of the RunPod key
podtrack whoami
podtrack register <id> --gpu H100 --ssh-ip .. --ssh-port .. \
                       --remote-path /root/run/results --local-path ~/data/run
podtrack list [--all|--mine|--others]
podtrack claim <id>                         # explicit ownership handoff / adopt orphan
podtrack probe <id>                         # SSH nvidia-smi GPU-util check
podtrack heartbeat <id>                     # pod-level keepalive
podtrack arm <id> --kill-in 120 [--token-file RESTRICTED]   # plant dead-man switch
podtrack pet <id> --min 30                  # slide the dead-man deadline forward
podtrack sync <id>                          # rsync artifacts remote->local
podtrack teardown <id> [--force] [--skip-pull]   # owner-guarded, pull-verify-then-kill
podtrack reconcile [--terminate-untracked]  # diff vs RunPod; flag leaks/idle
podtrack reap [--no-mirror]                 # autonomous: reconcile+mirror+pet+safe-teardown
```

## Autonomous reaper (systemd)

```bash
cp systemd/podtrack-reap.* ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now podtrack-reap.timer
systemctl --user list-timers podtrack-reap.timer
```
Runs `podtrack reap` every 5 min (`Persistent=true` catches up missed runs). Acts under a
fixed `reaper` identity; reaper teardowns bypass *ownership* but keep the *pull-verify* gate.

**Reaper safety (v0.2.1).** Idle-kill requires ALL of: (a) the pod is past a **startup grace**
(`startup_grace_min`, default 15); (b) **sustained idle** — `idle_strikes_needed` (default 3)
*consecutive* idle reconciles, so a busy GPU resets the counter and a momentary 0%-GPU snapshot
never kills; (c) **no fresh job heartbeat**. A hard `kill_after` TTL still fires regardless.

**Job heartbeat.** A long job that may go GPU-idle (CPU phases) should keep a heartbeat file
fresh so the reaper sees it's alive — on the pod:
```bash
( while :; do date -u +%FT%TZ > /root/.podtrack_job_alive; sleep 60; done ) &
```
The reaper reads that file via SSH each cycle. (Local jobs can call `podtrack job-heartbeat <id>`.)

## Storage

- Registry DB: `~/.local/share/podtrack/pods.db` (override `$PODTRACK_HOME`). Tables: `pods`
  + append-only `events` audit.
- Credential: `~/.config/podtrack/runpod.key` (after `adopt-key`).

## Status

- **Validated live (2026-06-30, real H100):** deploy→`register`, `reconcile`, and autonomous
  `reap`→`podTerminate` all worked end-to-end. (The first run also exposed the v0.2.1 bugs below —
  the reaper killed an *active* pod that had no heartbeat and was caught at a single 0%-GPU
  snapshot. Fixed.)
- **Tested (logic):** v0.2.1 reaper safety — startup grace, sustained-idle strikes, heartbeat
  override, per-pod SSH-key resolution; plus ownership/label-continuity, claim, schema migration.
- **Still needs a live smoke:** `arm`/`pet`/`sync`/`probe` over real SSH+rsync to a RunPod pod
  (now use the correct RunPod key). Non-fatal if they fail — the grace+strikes safety net protects
  active pods regardless.

## Integration (follow-up)

Refactor `dft2mlip-melter/infrastructure/{runpod_deploy,pod_teardown,
retrieve_scan_and_teardown}.py` to register on deploy, `arm --kill-in`, heartbeat, and
teardown through podtrack — replacing `pod_state.json`. `pod_audit.py` ≈ `reconcile`;
`incremental_sync.sh` ≈ the reaper's mirror.
