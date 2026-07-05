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
- **Pod-side dead-man switch** (`deadman.sh` + `deadman_supervisor.sh`, planted by `arm`) is
  the backstop for when the home box is asleep: the pod self-terminates at its `deadline`
  **unless** the reaper keeps pushing the deadline forward (pets it). Default-to-death → the
  GPU bill is bounded no matter what; the network volume keeps the data. A **supervisor** (#3)
  respawns the watchdog if it is OOM-killed/crashes while the pod stays up.
- **RunPod-side TTL** (`terminateAfter`, #5) is the *always-on* outer backstop: RunPod
  terminates the pod server-side after a hard deadline even if BOTH the home box is asleep
  and the on-pod watchdog is gone (e.g. after a pod restart wiped /dev/shm). `runpod_deploy.py`
  sets it automatically to `PODTRACK_KILL_IN + 60 min` whenever a dead-man is armed.

Three nested backstops, tightest → loosest: **pet-able soft deadline** (reaper, while home box
awake) → **on-pod dead-man + supervisor** (while pod/RAM survive) → **RunPod `terminateAfter`**
(server-side, survives everything).

## Credential mandate

podtrack is custodian of the RunPod key: `adopt-key` moves `~/.keys/runpod` into
`~/.config/podtrack/runpod.key` and leaves a notice, so the only sanctioned path to RunPod
is through podtrack. The key is sent in an **`Authorization: Bearer` header**, never the URL
query string (#15), so it can't leak into logs/proxies. The dead-man switch keeps its
credential on the pod's **tmpfs** (`/dev/shm`, RAM only) and `shred`s it after firing.
*Hardening (#4):* the resolution order for the on-pod credential is `arm --token-file` >
`~/.config/podtrack/deadman.token` (a **restricted RunPod token**, ideally `podTerminate`-scoped)
> the full account key **with a loud warning**. Drop a restricted token at that path so the
full account key never lands on rented hardware.

## Shared registry (#13)

`DATA_DIR` defaults to `~/.local/share/podtrack` but honors `PODTRACK_HOME`. On a single
machine (today's setup) the default is fine. To share one registry across **multiple home
machines**, point `PODTRACK_HOME` at a common path — but note SQLite's WAL journal is unsafe
over NFS/network filesystems; use a single always-on host or migrate to a networked DB before
relying on it. Not wired today (single-machine).

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
podtrack arm <id> --kill-in 120 [--token-file RESTRICTED]   # plant + VERIFY dead-man switch
podtrack pet <id> --min 30                  # slide the dead-man deadline forward
podtrack sync <id>                          # rsync artifacts remote->local
podtrack teardown <id> [--force] [--skip-pull]   # owner-guarded, pull-verify-kill, delete eph volume
podtrack reconcile [--terminate-untracked]  # diff vs RunPod; flag leaks/idle
podtrack sweep-volumes [--force]            # delete leaked pt-eph-* network volumes (P-1)
podtrack reap [--no-mirror]                 # autonomous: reconcile+mirror+pet+re-arm+safe-teardown
```

**Dead-man credential (failure mode #4):** `arm` plants a token on the pod (`/dev/shm`)
so it can self-terminate. Resolution order: `--token-file` > `~/.config/podtrack/deadman.token`
(a restricted, ideally `podTerminate`-scoped token) > the full account key **with a loud
warning**. Prefer the restricted token so a leaked pod can only kill itself. `arm` now
`pgrep`-verifies the watchdog actually started (a silent no-op used to leave a "protected"
pod unprotected); `reap` re-verifies each cycle and re-arms a watchdog wiped by a pod
restart/OOM.

**Ephemeral volumes (failure mode #10 / P-2):** the melter's `runpod_deploy.py
--ephemeral-volume` provisions a fresh network volume in a DC that has the GPU, pins the
pod there, verifies the `/workspace` mount or aborts+cleans up, registers `volume_id`, and
`teardown` deletes the volume so nothing persists. `sweep-volumes` is the backstop leak sweep.

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

**Reaper v0.3 (Phase 1) additions:**
- **Heartbeat-primary idle (#7):** a fresh job heartbeat now *zeroes* idle strikes and blocks
  idle-kill outright — GPU-util is only the fallback signal for pods with no heartbeat.
- **TTL grace (#6):** a soft `kill_after` that hits while a heartbeat is fresh is *extended once
  and warned*, not insta-killed. Use `arm --kill-absolute-min` for a hard cap that ignores
  heartbeats.
- **Unreachable-escalation (#8):** an idle pod SSH-unreachable for `--unreachable-reaps` (default
  3) cycles past the startup grace is force-terminated — the one leak nothing else can reap.
- **Untracked auto-reap (#12):** pods on the account but in no tracked session are terminated once
  older than `--untracked-grace` (default 30 min). *Assumes every deploy path registers with
  podtrack* — otherwise a non-podtrack pod is at risk; raise the grace or gate it via env.
- **Vanished-guard (#11):** a pod is only marked gone after 2 consecutive absent fetches, and a
  0-pod API response with known-live pods is treated as a bad fetch (sweep skipped).
- **First-class SSH (#14):** `arm`/`pet`/`probe`/`sync` retry with backoff and WARN; a busy pod
  we can't pet is surfaced loudly instead of silently drifting into a dead-man kill.
- **Artifactless warn (#9):** tearing down a >1 h pod with no `remote_path` warns loudly (silence
  with `register --no-artifacts`).

**Job heartbeat.** The melter's `pod_bootstrap.sh` now **auto-starts** the writer below, so every
job emits liveness without opt-in. To do it manually on any pod:
```bash
( while :; do date -u +%FT%TZ > /root/.podtrack_job_alive; sleep 60; done ) &
```
The reaper reads that file via SSH each cycle. (Local jobs can call `podtrack job-heartbeat <id>`.)

## Recommended settings

The reaper knobs are tunable **per-run** (`podtrack reap --startup-grace .. --idle-strikes ..
--hb-grace .. --pet-min .. --unreachable-reaps .. --untracked-grace ..`) or via **env** (in the
systemd unit or shell): `PODTRACK_STARTUP_GRACE_MIN`, `PODTRACK_IDLE_STRIKES`,
`PODTRACK_HB_GRACE_MIN`, `PODTRACK_PET_MIN`, `PODTRACK_UNREACHABLE_REAPS`,
`PODTRACK_UNTRACKED_GRACE_MIN`. Precedence: CLI arg > env > default.

**Key relationship:** the *sustained-idle window before a kill* = **timer cadence × idle-strikes**.
At the default 5-min cadence, `idle_strikes=3` ⇒ ~15 min of continuous GPU-idle required. Change
the cadence in `systemd/podtrack-reap.timer` (`OnUnitActiveSec`).

| Workload | grace | strikes | hb-grace | cadence | `kill_after` TTL | Rationale |
|---|---|---|---|---|---|---|
| **Long GPU jobs (DFT)** — *default* | 15 | 3 | 20 | 5 min | runtime + margin | GPU busy ~continuously; 15-min sustained-idle is safe. Set a TTL as the hard backstop; heartbeat during CPU phases. |
| **Interactive / smoke tests** | 5 | 2 | 10 | 5 min | 30–60 min | Reap fast to cap spend on throwaway pods. |
| **Jobs with long CPU stages** (data prep, I/O) | 15 | 4–6 | 60 | 5 min | runtime + margin | GPU legitimately idle for stretches → **job heartbeats are essential**; widen hb-grace + strikes. |

**Always set a `kill_after` TTL** at deploy (`PODTRACK_KILL_IN=<minutes>` or `podtrack arm
--kill-in <minutes>`). It's the hard, idle-detection-independent cost backstop (the on-pod
dead-man switch), and it's what protects you when the home box is asleep and can't run the reaper.

## Failure-mode register

The numbered `#N` references throughout this README, the code comments, and the audit log
name entries in this register — the known ways a pod-tracking system can silently burn money
or lose data, and what podtrack does about each:

| # | Failure mode | Countermeasure |
|---|---|---|
| 1 | `arm` launches a watchdog that silently fails to start → "protected" pod isn't | `arm` pgrep-verifies the watchdog post-launch (one relaunch, then hard-fail); `reap` re-verifies each cycle |
| 2 | Dead-man fires terminate once, exits regardless of outcome | fire path retries until the API *confirms* the pod gone (capped backoff, never gives up) |
| 3 | Watchdog is one unsupervised RAM-state process (OOM/crash = unprotected) | on-pod supervisor respawns it; reaper re-arms if wiped |
| 4 | Full account key sits plaintext on rented hardware | restricted-token resolution (`--token-file` > `deadman.token` > full key with loud WARN) |
| 5 | Local reaper only runs while the home machine is awake | RunPod-side `terminateAfter` TTL set at deploy = always-on outer backstop |
| 6 | Hard TTL kills a busy, heartbeating job | soft TTL + fresh heartbeat → extend & warn; `--kill-absolute-min` for a true hard cap |
| 7 | 0%-GPU snapshots idle-kill CPU-bound jobs | heartbeat-primary: fresh job heartbeat zeroes idle strikes; GPU-util is the fallback signal |
| 8 | Idle + SSH-unreachable pod can never be reaped (pull-verify refuses) | escalate to force-terminate after N unreachable reaps past grace |
| 9 | Teardown silently skips artifact pull when no spec registered | loud warn on artifact-less teardown of >1 h pods; `--no-artifacts` to opt out |
| 10 | Network volume silently unmounted (DC mismatch) → artifacts on ephemeral disk | ephemeral per-job volume created in a DC that has the GPU, pod pinned there, mount verified-or-abort |
| 11 | Partial API response marks live pods "terminated" (invisible leak) | vanish requires 2 consecutive absent fetches; 0-pod response with known-live pods skips the sweep |
| 12 | Leaked/untracked pods bill until a human notices | reaper auto-terminates untracked pods older than a grace |
| 13 | Registry is per-machine, not per-account | documented; `PODTRACK_HOME` for shared placement (see caveats) |
| 14 | SSH ops fail silently (busy pod drifts into dead-man kill) | retry+backoff+WARN on all SSH; failed pet on a busy pod raises an ALERT |
| 15 | API key in URL query string leaks to logs/proxies | `Authorization: Bearer` header everywhere |

Policies: **P-1** store nothing on the provider long-term (volumes are per-job, deleted at
teardown); **P-2** jobs >1 h get a fresh small network volume, verified at deploy, swept on leak.

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
- **v0.3 (2026-07-05) — the full failure-mode register implemented** (#1–#15 above; motivated by
  a real overnight idle-burn where the dead-man never fired). Verified against the live API:
  Bearer-header auth, `dataCenters`/`networkVolumes` queries, schema migration. Watchdog
  liveness check uses a bracketed pgrep pattern (a plain `pgrep -f <path>` over SSH matches its
  own `bash -c` wrapper and reports alive unconditionally).
- **Still needs a live smoke on a real pod:** `arm`-verify/supervisor-respawn/`pet`/`sync`, the
  ephemeral-volume deploy path end-to-end, and a real dead-man fire-to-confirmation. Non-fatal
  if they fail — the grace+strikes safety net protects active pods regardless.

## Integration (follow-up)

Refactor `dft2mlip-melter/infrastructure/{runpod_deploy,pod_teardown,
retrieve_scan_and_teardown}.py` to register on deploy, `arm --kill-in`, heartbeat, and
teardown through podtrack — replacing `pod_state.json`. `pod_audit.py` ≈ `reconcile`;
`incremental_sync.sh` ≈ the reaper's mirror.
