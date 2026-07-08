#!/usr/bin/env bash
# podtrack dead-man's switch — runs ON the RunPod pod.
#
# Self-terminates the pod once the deadline passes, UNLESS the deadline file
# keeps getting pushed forward (the local reaper "pets" it while the job is
# healthy). Default-to-death: if the host machine goes dark and stops petting,
# the pod kills itself at its last deadline, bounding the GPU bill. Artifacts
# survive on the RunPod network volume regardless.
#
# failure mode #2: the old version fired podTerminate ONCE and `exit 0`d
# unconditionally — a dropped request or transient API error left the pod up
# and billing while the switch reported success. Now we RETRY-UNTIL-CONFIRMED:
# terminate, poll desiredStatus, and only shred the key + exit once the API
# confirms the pod is gone. Never exit on an unconfirmed kill.
#
# State (RAM-only, never disk):
#   /dev/shm/podtrack/deadline   ISO-8601 UTC; pushed forward by `podtrack pet`
#   /dev/shm/podtrack/rpk        RunPod key (or restricted token); shredded on fire
#   /dev/shm/podtrack/pod_id     this pod's id
set -u
D="${PODTRACK_SHM:-/dev/shm/podtrack}"
API="https://api.runpod.io/graphql"
UA="Mozilla/5.0 (podtrack-deadman)"
LOG="$D/deadman.log"

log() {
  local msg
  msg="$(date -u +%Y-%m-%dT%H:%M:%SZ) $*"
  echo "$msg" >> "$LOG" 2>/dev/null || true
  # Also mirror the outcome to the network volume if one is mounted, so a
  # post-mortem survives the pod (RAM state does not).
  if mountpoint -q /workspace 2>/dev/null; then
    echo "$msg" >> /workspace/.podtrack_deadman.log 2>/dev/null || true
  fi
}

# Returns 0 (confirmed gone) once the API reports the pod absent or not RUNNING.
confirm_gone() {
  local key="$1" pod_id="$2" resp pstatus
  resp=$(curl -s -A "$UA" -H "Content-Type: application/json" \
    -H "Authorization: Bearer $key" \
    -d "{\"query\":\"query{pod(input:{podId:\\\"$pod_id\\\"}){desiredStatus}}\"}" \
    "$API" 2>/dev/null)
  # pod:null  -> the pod no longer exists -> confirmed terminated.
  if echo "$resp" | grep -q '"pod":[[:space:]]*null'; then
    return 0
  fi
  # A terminal desiredStatus also confirms it's on its way out.
  pstatus=$(echo "$resp" | grep -o '"desiredStatus":"[^"]*"' | head -1 | cut -d'"' -f4)
  case "$pstatus" in
    EXITED|TERMINATED) return 0 ;;
    *) return 1 ;;   # RUNNING, empty, or an API/network error -> not yet confirmed
  esac
}

fire() {
  local pod_id key attempt=0 backoff=5
  pod_id=$(cat "$D/pod_id" 2>/dev/null || echo "${RUNPOD_POD_ID:-}")
  key=$(cat "$D/rpk" 2>/dev/null || true)
  if [ -z "$pod_id" ] || [ -z "$key" ]; then
    log "FIRE aborted: missing pod_id or key (pod_id='${pod_id}')"
    return 1
  fi
  log "FIRE: deadline passed, terminating $pod_id (retry until confirmed)"
  while true; do
    attempt=$((attempt + 1))
    curl -s -A "$UA" -H "Content-Type: application/json" \
      -H "Authorization: Bearer $key" \
      -d "{\"query\":\"mutation{podTerminate(input:{podId:\\\"$pod_id\\\"})}\"}" \
      "$API" >/dev/null 2>&1
    sleep 3
    if confirm_gone "$key" "$pod_id"; then
      log "CONFIRMED terminated after $attempt attempt(s)"
      shred -u "$D/rpk" 2>/dev/null || rm -f "$D/rpk"
      rm -f "$D/deadline"          # signal the supervisor (#3) to stop respawning us
      return 0
    fi
    log "terminate unconfirmed (attempt $attempt); retrying in ${backoff}s"
    sleep "$backoff"
    backoff=$(( backoff < 120 ? backoff * 2 : 120 ))   # cap at 120s; never give up
  done
}

while true; do
  sleep 60
  [ -f "$D/deadline" ] || continue
  DEADLINE=$(cat "$D/deadline" 2>/dev/null)
  NOW=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  # Lexicographic compare is valid for zero-padded ISO-8601 UTC timestamps.
  if [[ -n "$DEADLINE" && "$NOW" > "$DEADLINE" ]]; then
    fire
    exit 0   # only reached once fire() confirms the pod is gone
  fi
done
