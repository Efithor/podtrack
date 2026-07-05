#!/usr/bin/env bash
# podtrack dead-man SUPERVISOR — runs ON the RunPod pod (failure mode #3).
#
# The watchdog (deadman.sh) is a single unsupervised process holding only RAM
# state. If it is OOM-killed or crashes while the pod itself stays up, the pod
# becomes silently unprotected until the home reaper notices and re-arms (up to
# one reap cycle later — and the reaper only runs while the home box is awake).
#
# This supervisor closes that window from inside the pod: it respawns deadman.sh
# whenever it exits, for as long as the deadline state survives. It deliberately
# does NOT persist the key or survive a pod restart — a restart wipes /dev/shm
# (key included), at which point the RunPod-side `terminateAfter` TTL (#5) and the
# home reaper's re-arm (#1) are the backstops. This only hardens the common case:
# pod alive, watchdog process died.
set -u
D="${PODTRACK_SHM:-/dev/shm/podtrack}"
LOG="$D/supervisor.log"

log() { echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) $*" >> "$LOG" 2>/dev/null || true; }

log "supervisor up (pid $$)"
while [ -f "$D/deadline" ]; do
  if [ ! -x "$D/deadman.sh" ]; then
    log "deadman.sh missing/not-executable; supervisor exiting"
    break
  fi
  "$D/deadman.sh"
  rc=$?
  # deadman.sh returns 0 only after a CONFIRMED fire (it also removes the
  # deadline file), so the loop guard above then exits. Any other exit means it
  # died unexpectedly while the pod is still up — pause briefly and respawn.
  [ -f "$D/deadline" ] || { log "deadline cleared (fired); supervisor exiting"; break; }
  log "deadman.sh exited rc=$rc while pod alive — respawning in 5s"
  sleep 5
done
