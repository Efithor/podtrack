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
  deadline lapse with the home box "asleep" and confirm terminate + confirm-gone
  loop + volume survival).
