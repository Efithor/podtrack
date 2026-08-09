# podtrack

[![PyPI](https://img.shields.io/pypi/v/podtrack)](https://pypi.org/project/podtrack/)
[![Python 3.9+](https://img.shields.io/badge/python-3.9%2B-blue)](https://pypi.org/project/podtrack/)
[![License: MIT](https://img.shields.io/badge/license-MIT-green)](LICENSE)

An ownership-aware registry **and autonomous reaper** for [RunPod](https://runpod.io)
pods. It lets several independent workers share one RunPod account without losing,
orphaning, or killing each other's pods — and it terminates idle or expired GPUs on
its own so a forgotten pod can't quietly bill for hours.

It's built for anyone who launches RunPod pods from automation — CI jobs, batch
pipelines, or multiple concurrent sessions against a single account — where "who owns
this pod" and "is anything still using it" are easy to lose track of.

## Install

```bash
pip install podtrack
# or from source:
pip install .
```

Requires Python 3.9+ and, for remote checks/teardowns, `ssh` + `rsync` on the host
running the reaper.

## Quick start

```bash
podtrack adopt-key                 # one-time: take custody of the RunPod API key
export PODTRACK_LABEL=my-job       # stable identity for this worker (see Identity)

# after you deploy a pod, register it (an artifact spec is REQUIRED —
# it's what makes every teardown pull-verify and every reap cycle mirror;
# pods that truly produce nothing must say --no-artifacts explicitly):
podtrack register <pod-id> --gpu H100 --ssh-ip <ip> --ssh-port <port> \
    --remote-path /root/run/results --local-path ~/data/run --kill-in 120

podtrack list --mine               # what you own
podtrack reconcile                 # diff the registry against the live account
```

Then enable the reaper (below) and it handles idle/expiry teardown for you.

## Identity and ownership

Every pod is owned by an `(uuid, label)` pair:

- **label** (`$PODTRACK_LABEL`) is the *stable task identity* — it's what ownership
  follows.
- **uuid** (`$PODTRACK_OWNER_UUID`, else a persisted fallback) is a per-process id.

The split matters because a process id is often ephemeral — a worker can restart and
come back with a new uuid but the same job. In that case podtrack sees the matching
label and **reclaims** the pods automatically (logged); a genuinely different label is
refused. Use `podtrack claim <id>` for explicit handoffs or to adopt an orphan.

```bash
export PODTRACK_LABEL=my-job
export PODTRACK_OWNER_UUID=$(uuidgen)   # optional; a stable value is persisted if unset
```

### Provenance stamping (whose pod is this, really?)

Registry ownership answers "which *session* owns this pod" — but on an account other
**people** also use, the prior question is "did our tooling create this pod at all?"
podtrack answers it with a creation-time env marker: deploy tooling injects
`PODTRACK_STAMP=<label>@<host>` into every pod it creates. Pod env is immutable after
deploy and returned by the pod query, so the stamp is provenance, not convention (a
name prefix can collide or be edited; env can't).

Reconcile then classifies every unknown live pod:

- **stamped + untracked** → provably *our* leak (a deploy died before `register`);
  eligible for the reaper's untracked sweep, on any account.
- **unstamped** → **FOREIGN**: someone else's pod. Recorded (`owner NOT-OURS`), logged
  once, and excluded from every automatic action — the untracked sweep, idle-kill, and
  `reconcile --terminate-untracked` all skip it. Nothing in podtrack will ever
  terminate a pod it can't prove it created.

For accounts shared with other humans, list them (one name per line) in
`~/.config/podtrack/shared-accounts`: legacy unknown rows *without* stamp proof on
those accounts get warn-only treatment instead of the sweep.

## Commands

```bash
podtrack adopt-key                          # one-time: take custody of the RunPod key
podtrack adopt-key --account pro --from ~/.keys/runpod-pro   # additional accounts
podtrack accounts                           # list configured accounts + custody paths
podtrack whoami
podtrack register <id> --gpu H100 --ssh-ip .. --ssh-port .. \
                       --remote-path /root/run/results --local-path ~/data/run \
                       [--account pro]      # default: $PODTRACK_ACCOUNT or main
podtrack list [--all|--mine|--others]
podtrack claim <id>                         # explicit ownership handoff / adopt orphan
podtrack probe <id>                         # SSH nvidia-smi GPU-util check
podtrack heartbeat <id>                     # pod-level keepalive
podtrack arm <id> --kill-in 120 [--token-file RESTRICTED]   # plant + verify dead-man switch
podtrack pet <id> --min 30                  # slide the dead-man deadline forward
podtrack sync <id>                          # rsync artifacts remote -> local
podtrack teardown <id> [--force] [--skip-pull]   # owner-guarded pull-verify-kill
podtrack reconcile [--terminate-untracked]  # diff vs ALL live accounts; flag leaks/idle
podtrack sweep-volumes [--force]            # delete leaked ephemeral network volumes
podtrack reap [--no-mirror]                 # autonomous: reconcile+mirror+pet+re-arm+teardown
```

## The autonomous reaper

`podtrack reap` is the brain. Each cycle it reconciles the registry against the live
account, mirrors artifacts off every live pod, keeps healthy pods alive, and safely
tears down anything that's expired or sustained-idle. Run it on a timer:

```bash
cp systemd/podtrack-reap.* ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now podtrack-reap.timer
systemctl --user list-timers podtrack-reap.timer
```

It runs `podtrack reap` every 5 minutes (`Persistent=true` catches up missed runs after
the machine was off). Reaper teardowns act under a fixed `reaper` identity — they bypass
the *ownership* check but keep the *pull-verify* gate below.

### Idle detection (why it won't kill a busy pod)

A kill for idleness requires **all** of:

- the pod is past a **startup grace** (`startup_grace_min`, default 15 min);
- **sustained idle** — `idle_strikes_needed` (default 3) *consecutive* idle cycles, so a
  single momentary 0%-GPU snapshot never kills and any activity resets the counter;
- **no fresh job heartbeat**.

Idle is judged from two signals so a CPU-bound stage isn't mistaken for a dead pod: GPU
utilization (from the RunPod API, or a definitive SSH `nvidia-smi` via `podtrack probe`)
**and** a job heartbeat. A fresh heartbeat zeroes the idle strikes outright; GPU
utilization is only the fallback signal for pods that emit no heartbeat.

**An idle-kill is never executed on API telemetry alone** (0.6.0, hardened in 0.6.1):
when strikes say "kill", the reaper interrogates the pod itself — **six `nvidia-smi`
samples over ~5 s plus device `memory.used`**. Any nonzero utilization sample OR ≥1 GiB
resident device memory vetoes the kill and resets strikes with a loud ALERT. Why both:
`utilization.gpu` is bursty — a healthy launch-bound MD job measured samples of
100, 30, 0, 0, 0, 0, 0, 100 in sequence, so single samples (including the RunPod API's)
are coin flips on gappy workloads — while resident memory is steady for any live job and
visible through container PID namespaces (unlike `--query-compute-apps`). Unreachable →
deferred to the unreachable-escalation path. Only "all samples 0 AND <1 GiB resident,
confirmed over SSH" proceeds. TTL kills are unaffected — the cost backstop stays
absolute.

A hard `kill_after` TTL still fires regardless of activity — it's the cost backstop that
doesn't depend on idle detection at all. **Always set one** at deploy
(`--kill-in <minutes>` on `register`/`arm`). A soft TTL that lapses while a heartbeat is
fresh is extended once and warned rather than killed instantly; `arm --kill-absolute-min`
sets a true hard cap that ignores heartbeats.

### Job heartbeat

A job signals liveness by touching a file the reaper reads over SSH each cycle:

```bash
( while :; do date -u +%FT%TZ > /root/.podtrack_job_alive; sleep 60; done ) &
```

Local jobs can call `podtrack job-heartbeat <id>` instead.

## Data safety on teardown

Teardown is never a bare kill. Three layers protect artifacts:

- **Continuous mirror** — the reaper rsyncs each live pod's artifacts to local every
  cycle, so even an ungraceful death loses at most one interval.
- **Pull → verify → kill** — every teardown pulls artifacts, verifies the local copy is
  non-empty, and only then terminates. On sync failure it leaves the pod up and alerts.
- **Network-volume backstop** — jobs longer than an hour get a fresh per-job RunPod
  network volume, created in a data center that has the GPU, with the mount verified at
  deploy or the deploy aborts. The volume survives the pod, and `teardown` deletes it so
  nothing persists on the provider. `sweep-volumes` cleans up any leaked volumes.

## The three backstops

Cost is bounded by three nested deadlines, tightest to loosest:

1. **Pet-able soft deadline** — the reaper slides a healthy pod's deadline forward each
   cycle while the host machine is awake.
2. **On-pod dead-man switch** — a watchdog planted by `arm` self-terminates the pod at
   its deadline *unless* the reaper keeps petting it forward. This is the backstop for
   when the host machine is asleep and can't run the reaper. A supervisor respawns the
   watchdog if it's OOM-killed or crashes while the pod stays up.
3. **RunPod-side TTL** — `terminateAfter` is set server-side at deploy, so RunPod
   terminates the pod even if both the host is asleep *and* the on-pod watchdog is gone
   (e.g. after a pod restart wiped its RAM state).

Default-to-death at every layer: the GPU bill is bounded no matter which layers are
alive, and the network volume keeps the data.

## Credential handling

podtrack is the custodian of every RunPod API key. `adopt-key` moves a key into
`~/.config/podtrack/` so the only sanctioned path to RunPod is through podtrack. The key
is sent in an `Authorization: Bearer` header, never in a URL query string, so it can't
leak into request logs or proxies.

### Multiple accounts

Accounts are named credential slots: the original account is `main`
(`runpod.key`); any other account `NAME` keeps its key at `runpod.NAME.key` after
`adopt-key --account NAME [--from PATH]`. Every pod row records its account, all
API calls about a pod authenticate with that account's key, and `reconcile` /
`reap` / `sweep-volumes` sweep **all** configured accounts each cycle — a pod on
a second account is never invisible to leak detection. `register` refuses an
account with no key in custody (such a pod could never be reconciled or reaped).

The on-pod dead-man switch needs a credential to terminate its own pod — one
belonging to the pod's *own account*. To avoid putting a full account key on
rented hardware, `arm` resolves the on-pod credential in this order:

1. `arm --token-file <path>`
2. `~/.config/podtrack/deadman.token` (account `main`) or
   `~/.config/podtrack/deadman.<account>.token` — a **restricted RunPod token**,
   ideally scoped to `podTerminate` only
3. that account's full key, **with a loud warning**

Drop a restricted token at that path so a leaked pod can only ever terminate itself. The
on-pod credential lives on tmpfs (`/dev/shm`, RAM only) and is `shred`ded after the
switch fires.

## Configuration

Reaper knobs are settable per run (`podtrack reap --startup-grace .. --idle-strikes ..
--hb-grace .. --pet-min .. --unreachable-reaps .. --untracked-grace ..`) or via
environment variables (`PODTRACK_STARTUP_GRACE_MIN`, `PODTRACK_IDLE_STRIKES`,
`PODTRACK_HB_GRACE_MIN`, `PODTRACK_PET_MIN`, `PODTRACK_UNREACHABLE_REAPS`,
`PODTRACK_UNTRACKED_GRACE_MIN`). Precedence: CLI arg > env > default.

The sustained-idle window before a kill = **timer cadence × idle-strikes**. At the
default 5-minute cadence, `idle_strikes=3` means ~15 minutes of continuous GPU-idle is
required before an idle teardown. Change the cadence in `systemd/podtrack-reap.timer`
(`OnUnitActiveSec`).

| Workload | grace | strikes | hb-grace | cadence | `kill_after` TTL | Notes |
|---|---|---|---|---|---|---|
| **Long GPU jobs** (default) | 15 | 3 | 20 | 5 min | runtime + margin | GPU busy nearly continuously; 15-min sustained-idle is safe. Set a TTL as the hard backstop. |
| **Interactive / smoke tests** | 5 | 2 | 10 | 5 min | 30–60 min | Reap fast to cap spend on throwaway pods. |
| **Jobs with long CPU stages** (data prep, I/O) | 15 | 4–6 | 60 | 5 min | runtime + margin | GPU legitimately idle for stretches, so **job heartbeats are essential**; widen hb-grace and strikes. |

## Failure modes it guards against

The known ways a pod-tracking system can silently burn money or lose data, and what
podtrack does about each:

| Failure mode | Countermeasure |
|---|---|
| `arm` launches a watchdog that silently fails to start → "protected" pod isn't | `arm` verifies the watchdog started (one relaunch, then hard-fail); `reap` re-verifies each cycle |
| Dead-man fires terminate once and exits regardless of outcome | fire path retries until the API confirms the pod is gone (capped backoff, never gives up) |
| Watchdog is one unsupervised RAM-state process (OOM/crash = unprotected) | on-pod supervisor respawns it; reaper re-arms if it's wiped |
| Full account key sits in plaintext on rented hardware | restricted-token resolution (`--token-file` > `deadman.token` > full key with a warning) |
| The reaper only runs while the host machine is awake | RunPod-side `terminateAfter` TTL, set at deploy, is the always-on outer backstop |
| A hard TTL kills a busy, heartbeating job | soft TTL + fresh heartbeat extends and warns; `--kill-absolute-min` for a true hard cap |
| 0%-GPU snapshots idle-kill CPU-bound jobs | a fresh job heartbeat zeroes idle strikes; GPU-util is only the fallback signal |
| An idle **and** SSH-unreachable pod can never be reaped (pull-verify refuses) | escalate to force-terminate after N unreachable cycles past the startup grace |
| Teardown silently skips the artifact pull when no path was registered | **register REFUSES without an artifact spec** (or explicit `--no-artifacts`); artifact-less teardown of an old pod still warns loudly |
| RunPod API reports 0% GPU for a busy pod → reaper kills a live job | idle-kills require SSH `nvidia-smi` confirmation; API-only idleness can never terminate a pod |
| Network volume silently unmounts (data-center mismatch) → artifacts land on ephemeral disk | per-job volume created in a DC that has the GPU, pod pinned there, mount verified or abort |
| A partial API response marks live pods "terminated" (invisible leak) | a pod is only marked gone after two consecutive absent fetches; a 0-pod response with known-live pods skips the sweep |
| Leaked/untracked pods bill until a human notices | reaper auto-terminates untracked pods older than a configurable grace — but only pods carrying our `PODTRACK_STAMP` creation marker |
| Another user's pod on a shared account looks like "our leak" → reaper destroys their work | provenance stamping: unstamped pods are FOREIGN — recorded, surfaced, and never auto-terminated; `shared-accounts` adds a warn-only brake for legacy stampless rows |
| Registry is per-machine, not per-account | documented; `PODTRACK_HOME` for shared placement (see Storage) |
| SSH ops fail silently → a busy pod drifts into a dead-man kill | retry + backoff + warning on all SSH; a failed pet on a busy pod raises an alert |
| API key in a URL query string leaks to logs/proxies | `Authorization: Bearer` header everywhere |
| Pods on a second RunPod account are invisible to leak detection | per-account key custody; reconcile/reap/sweep cover every configured account each cycle; `register` refuses an account with no key; a failed per-account fetch skips only that account's vanish sweep |

## Storage

- **Registry DB:** `~/.local/share/podtrack/pods.db` (override with `$PODTRACK_HOME`).
  Holds a `pods` table plus an append-only `events` audit log.
- **Credentials:** `~/.config/podtrack/runpod.key` (account `main`) and
  `~/.config/podtrack/runpod.<account>.key` per additional account (after `adopt-key`).

`PODTRACK_HOME` can point the registry at a shared path to coordinate multiple machines,
but SQLite's WAL journal is unsafe over NFS/network filesystems — use a single always-on
host, or migrate to a networked database, before relying on that.

## Status

podtrack has been validated live against a real RunPod H100: deploy → `register`,
`reconcile`, and an autonomous `reap` → terminate all work end to end. The reaper's
idle-kill safety (startup grace, sustained-idle strikes, heartbeat override), ownership
and label continuity, `claim`, and schema migration are covered by tests. The dead-man
`arm`/supervisor/`pet`/`sync` path and the ephemeral-volume deploy path have been
exercised individually; a full unattended dead-man fire-to-confirmation is still on the
list. The grace + strikes safety net protects active pods regardless.

## Disclaimer

podtrack terminates paid cloud resources automatically. It is provided **as is, with no
warranty of any kind**. You are responsible for how you configure and run it, and for any
cost, data loss, or terminated pod that results from using it — including bugs, missed
reaps, or an unintended teardown. Test it against throwaway pods before trusting it with
anything expensive. This is the plain-language version of the warranty and liability
terms in the [LICENSE](LICENSE), which govern.

## License

MIT — see [LICENSE](LICENSE).
