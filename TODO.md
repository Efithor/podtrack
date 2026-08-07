# podtrack — TODO

Deferred improvements from the 2026-07-05 live-fire session (the first real
exercise of v0.3 on a paid pod). The P0 items from that session are already in
`podtrack.py` (data-guard on volume delete, stdin-detached deadman launches,
ConnectTimeout 30, noexec-safe `bash` invocation).

## P1 — semantics

- [ ] **Separate the rolling dead-man window from the owner TTL.** `pet`
  currently overwrites `kill_after` (the owner's deliberate TTL) with
  `now + pet_min`, destroying the owner's intent in the DB. Add a
  `deadman_deadline` column; `pet` slides only that (and the on-pod file);
  `kill_after` stays what the owner set. Reap: TTL check against `kill_after`,
  pet-freshness against `deadman_deadline`.

- [ ] **Split `arm`'s install into short piecewise SSH calls** (write scripts →
  write state → launch → verify). Some pods' sshd (observed: EU-RO-1 A100 under
  load) reliably chokes on long compound commands while short ones work; arm's
  single giant heredoc is the most fragile command we send.

## P1 — docs / operational footguns (README section)

- [ ] **pkill/pgrep self-match**: any `pkill -f <pattern>` sent over SSH matches
  the remote wrapper shell's own cmdline — including the *suicidal* variant
  where it kills its own session (exit 255 with no output). Always bracket:
  `pkill -f "patt[e]rn"`. (podtrack's own `_deadman_alive` already does this.)

- [ ] **Idempotent job launches**: retried SSH launch attempts can be *queued*,
  not lost — when a wedged sshd recovers, all of them execute and race (observed:
  six concurrent chains fighting over one GPU). Job launches must be guarded
  (pidfile + `kill -0`, or `flock`) so a duplicate exits immediately.

- [ ] **Session-hang pattern**: a backgrounded remote child that inherits the
  SSH session's stdin/stdout keeps the session open until timeout. Launch
  detached jobs with `setsid nohup ... </dev/null >log 2>&1 &`.

## P2 / later

- [ ] Restricted dead-man token: generate + document the console workflow for a
  `podTerminate`-scoped token at `~/.config/podtrack/deadman.token` (the arm
  WARN fires on every use of the full key).
- [ ] `sweep-volumes` in the reap cycle (currently manual).
- [ ] Registry `terminate_after` column mirroring the RunPod-side TTL set at
  deploy, so `list` shows the true outermost backstop.
- [ ] Live-fire test of an actual dead-man FIRE (let a throwaway cheap pod's
  deadline lapse with the host machine "asleep" and confirm terminate + confirm-gone
  loop + volume survival).

## 2026-08-07 — Incident: #12 untracked-reap killed a coworker's pods (pro account)

Overnight Aug 6→7 the reaper auto-terminated three untracked RTX PRO 6000 pods
($4.18/h) on 'pro' — they belonged to a coworker (accounts are per-team, not
per-person). The coworker purged ALL API keys on the account at 12:21 CDT in
response; podtrack lost access (401s from 12:26).

Fixed same day: `shared_accounts()` reading `~/.config/podtrack/shared-accounts`
('pro' listed) — untracked pods on shared accounts are warn-only in reap (#12)
and refused in `reconcile --terminate-untracked`. Failure-mode register
addendum: #12's premise ("every deploy registers") only holds for accounts
where podtrack-driven automation is the ONLY user. Follow-ups:
- [ ] adopt new pro key once minted (`podtrack adopt-key --account pro`) —
      verify shared-accounts guard logs the NOTE path on first reap afterwards
- [ ] consider defaulting NEW accounts to shared until explicitly marked solo

## 2026-08-07 (pm) — Provenance stamping (PODTRACK_STAMP) shipped

Kyle's spec after the coworker incident: pods need forced creation-time
metadata so ownership is provable, prefix-free. Implemented via an env var
(env is immutable post-deploy and returned by the pod query — the strongest
per-pod marker RunPod offers; it has no first-class label/tag API):
- runpod_deploy.py injects PODTRACK_STAMP=<label>@<host> at creation
- fetch_remote_pods() requests env, exposes `stamped`
- reconcile: unknown+stamped -> UNTRACKED (our leak, reapable);
  unknown+unstamped -> FOREIGN/NOT-OURS row, logged once, NEVER auto-touched
  (excluded from #12, idle-kill, and terminate_untracked)
- reap #12: stamp is the authority — stamped rows reap on ANY account;
  shared-accounts file now only brakes LEGACY unknown rows (no stamp proof)
- offline test: scratchpad test_stamp_logic2.py (3 paths, ALL PASS 2026-08-07)
- [ ] first real-pod validation: after next deploy, `podtrack probe` the pod
      and confirm reconcile shows it stamped; then delete this line
