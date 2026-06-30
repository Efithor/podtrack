#!/usr/bin/env bash
# podtrack dead-man's switch — runs ON the RunPod pod.
#
# Self-terminates the pod once the deadline passes, UNLESS the deadline file
# keeps getting pushed forward (the local reaper "pets" it while the job is
# healthy). Default-to-death: if the home machine goes dark and stops petting,
# the pod kills itself at its last deadline, bounding the GPU bill. Artifacts
# survive on the RunPod network volume regardless.
#
# State (RAM-only, never disk):
#   /dev/shm/podtrack/deadline   ISO-8601 UTC; pushed forward by `podtrack pet`
#   /dev/shm/podtrack/rpk        RunPod key (or restricted token); shredded on fire
#   /dev/shm/podtrack/pod_id     this pod's id
set -u
D=/dev/shm/podtrack
API="https://api.runpod.io/graphql"

while true; do
  sleep 60
  [ -f "$D/deadline" ] || continue
  DEADLINE=$(cat "$D/deadline" 2>/dev/null)
  NOW=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  # Lexicographic compare is valid for zero-padded ISO-8601 UTC timestamps.
  if [[ -n "$DEADLINE" && "$NOW" > "$DEADLINE" ]]; then
    POD_ID=$(cat "$D/pod_id" 2>/dev/null || echo "${RUNPOD_POD_ID:-}")
    KEY=$(cat "$D/rpk" 2>/dev/null || true)
    if [ -n "$POD_ID" ] && [ -n "$KEY" ]; then
      curl -s -A "Mozilla/5.0 (podtrack-deadman)" \
        -H "Content-Type: application/json" \
        -d "{\"query\":\"mutation{podTerminate(input:{podId:\\\"$POD_ID\\\"})}\"}" \
        "$API?api_key=$KEY" >/dev/null 2>&1
    fi
    shred -u "$D/rpk" 2>/dev/null || rm -f "$D/rpk"
    exit 0
  fi
done
