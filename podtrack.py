#!/usr/bin/env python3
"""podtrack — a shared, ownership-aware registry + autonomous reaper for RunPod pods.

Why this exists
---------------
Multiple agent streams (Claude Code sessions) share one RunPod account. Without
ownership they clobber each other's pods (lost/orphaned -> silent billing), tear
down each other's live pods, and leave idle/CPU pods running for hours. podtrack
replaces the old single `pod_state.json` with a concurrency-safe SQLite registry
that records WHO owns each pod, refuses cross-owner teardowns, and autonomously
reaps idle/expired pods WITHOUT losing artifacts.

v0.2 capabilities
-----------------
1. Registry        — one row per pod, shared across streams (SQLite, WAL).
2. Ownership       — owner = (session-UUID, friendly-label). Continuity follows the
                     LABEL (UUID is ephemeral across /resume); teardown is guarded.
3. Reconcile       — diff registry vs live RunPod; catch leaks, mark vanished,
                     flag GPU-vs-CPU / idle.
4. Artifact safety — every teardown is pull -> verify -> kill; a periodic mirror
                     bounds loss on ungraceful death. Artifacts live on the RunPod
                     network volume so any kill is data-safe.
5. Autonomous reap — `reap` (run by a local systemd timer) reconciles, mirrors,
                     pets healthy pods' dead-man switch, and safe-tears-down
                     TTL-expired / idle pods.
6. Dead-man switch — `arm` plants an on-pod self-destruct (default-to-death) so a
                     pod self-terminates at its deadline even if the home machine
                     is asleep. The local reaper "pets" it (slides the deadline)
                     while a job is healthy.

Credential mandate: podtrack is custodian of the RunPod key (`adopt-key`).
"""
from __future__ import annotations

import argparse
import json
import os
import socket
import sqlite3
import subprocess
import sys
import urllib.error
import urllib.request
import uuid as _uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

# ---------------------------------------------------------------- paths / config
DATA_DIR = Path(os.environ.get("PODTRACK_HOME", Path.home() / ".local/share/podtrack"))
DB_PATH = DATA_DIR / "pods.db"
CRED_DIR = Path.home() / ".config/podtrack"
CRED_PATH = CRED_DIR / "runpod.key"
LEGACY_KEY = Path.home() / ".keys/runpod"
RUNPOD_API = "https://api.runpod.io/graphql"
SSH_KEYS = [Path.home() / ".runpod/ssh/RunPod-Key-Go",   # RunPod pods
            Path.home() / ".ssh/id_ed25519"]             # home box / generic
SSH_USER = "root"                              # RunPod direct-SSH user


def _ssh_key_for(pod) -> str:
    """Resolve the SSH key for a pod: explicit per-pod key, else first that exists."""
    if pod.get("ssh_key") and Path(pod["ssh_key"]).expanduser().exists():
        return str(Path(pod["ssh_key"]).expanduser())
    for k in SSH_KEYS:
        if k.exists():
            return str(k)
    return str(SSH_KEYS[-1])
LIVE_STATUSES = ("provisioning", "running", "exited")
DEADMAN_SH = Path(__file__).with_name("deadman.sh")
JOB_HB_FILE = "/root/.podtrack_job_alive"      # job writes ISO-UTC ts here; reaper reads it


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _ts(dt: datetime | None = None) -> str:
    return (dt or _now()).strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse(ts: str | None) -> datetime | None:
    if not ts:
        return None
    return datetime.strptime(ts, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)


# ----------------------------------------------------------------------- identity
def whoami(uuid: str | None = None, label: str | None = None) -> tuple[str, str]:
    """Resolve (owner_uuid, owner_label). UUID is ephemeral (changes across
    /resume); LABEL is the stable task identity that ownership continuity follows."""
    u = uuid or os.environ.get("PODTRACK_OWNER_UUID") or os.environ.get("CLAUDE_SESSION_ID")
    if not u:
        marker = DATA_DIR / "owner_id"
        if marker.exists():
            u = marker.read_text().strip()
        else:
            u = f"host-{socket.gethostname()}-{_uuid.uuid4().hex[:8]}"
            DATA_DIR.mkdir(parents=True, exist_ok=True)
            marker.write_text(u)
        print(f"# podtrack: no session UUID in env; using fallback '{u}'. "
              f"Set PODTRACK_OWNER_UUID + PODTRACK_LABEL to distinguish streams.",
              file=sys.stderr)
    return u, (label or os.environ.get("PODTRACK_LABEL") or "(unlabeled)")


# --------------------------------------------------------------- key custodian
def adopt_key() -> str:
    CRED_DIR.mkdir(parents=True, exist_ok=True)
    if CRED_PATH.exists():
        return f"already adopted: {CRED_PATH}"
    if not LEGACY_KEY.exists():
        raise SystemExit(f"no key at {LEGACY_KEY} to adopt (and none at {CRED_PATH})")
    CRED_PATH.write_text(LEGACY_KEY.read_text())
    CRED_PATH.chmod(0o600)
    LEGACY_KEY.write_text(
        "# MOVED. The RunPod API key is now managed by podtrack.\n"
        "# Do NOT read this file or call the RunPod API directly.\n"
        f"# Use: podtrack <cmd>.  Key custodian: {CRED_PATH}\n")
    LEGACY_KEY.chmod(0o600)
    return f"adopted: {LEGACY_KEY} -> {CRED_PATH} (legacy path now a notice)"


def runpod_key() -> str:
    if CRED_PATH.exists():
        return CRED_PATH.read_text().strip()
    if LEGACY_KEY.exists() and "MOVED" not in LEGACY_KEY.read_text(errors="replace"):
        raise SystemExit(f"RunPod key still at {LEGACY_KEY}; run `podtrack adopt-key`.")
    raise SystemExit(f"no RunPod key at {CRED_PATH}; run `podtrack adopt-key`.")


# ------------------------------------------------------------------ RunPod API
def gql(query: str, variables: dict | None = None) -> dict:
    body = json.dumps({"query": query, "variables": variables or {}}).encode()
    req = urllib.request.Request(
        f"{RUNPOD_API}?api_key={runpod_key()}", data=body,
        headers={"Content-Type": "application/json", "User-Agent": "Mozilla/5.0 (podtrack)"})
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            out = json.loads(r.read().decode())
    except urllib.error.HTTPError as e:
        raise SystemExit(f"RunPod HTTP {e.code}: {e.read().decode(errors='replace')[:300]}")
    if out.get("errors"):
        raise SystemExit(f"RunPod GraphQL error: {out['errors']}")
    return out.get("data", {})


def fetch_remote_pods() -> list[dict]:
    q = """query { myself { pods {
        id name desiredStatus costPerHr
        machine { gpuDisplayName }
        runtime { uptimeInSeconds gpus { id gpuUtilPercent }
                  ports { ip publicPort privatePort type } } } } }"""
    pods = (gql(q).get("myself") or {}).get("pods") or []
    norm = []
    for p in pods:
        rt = p.get("runtime") or {}
        gpus = rt.get("gpus") or []
        ssh_ip = ssh_port = None
        for port in (rt.get("ports") or []):
            if port.get("privatePort") == 22:
                ssh_ip, ssh_port = port.get("ip"), port.get("publicPort")
        norm.append({
            "pod_id": p.get("id"), "name": p.get("name"),
            "desired": p.get("desiredStatus"), "cost_per_hr": p.get("costPerHr"),
            "gpu_type": (p.get("machine") or {}).get("gpuDisplayName"),
            "n_gpus": len(gpus), "uptime_s": rt.get("uptimeInSeconds"),
            "gpu_util": max((g.get("gpuUtilPercent") or 0) for g in gpus) if gpus else None,
            "ssh_ip": ssh_ip, "ssh_port": ssh_port})
    return norm


def terminate_remote(pod_id: str):
    gql("mutation($i:PodTerminateInput!){podTerminate(input:$i)}", {"i": {"podId": pod_id}})


# ----------------------------------------------------------------- ssh helpers
def _ssh_base(pod):
    return ["ssh", "-i", _ssh_key_for(pod), "-p", str(pod["ssh_port"]),
            "-o", "StrictHostKeyChecking=no", "-o", "ConnectTimeout=12",
            f"{SSH_USER}@{pod['ssh_ip']}"]


def ssh_run(pod, remote_cmd, timeout=60):
    if not pod.get("ssh_ip"):
        return 255, "", "no ssh endpoint in registry (run reconcile)"
    try:
        p = subprocess.run(_ssh_base(pod) + [remote_cmd],
                           capture_output=True, text=True, timeout=timeout)
        return p.returncode, p.stdout, p.stderr
    except subprocess.TimeoutExpired:
        return 255, "", "ssh timeout"


def rsync_pull(pod, remote_path, local_path, timeout=1800):
    Path(local_path).mkdir(parents=True, exist_ok=True)
    ssh = (f'ssh -i {_ssh_key_for(pod)} -p {pod["ssh_port"]} '
           f'-o StrictHostKeyChecking=no -o ConnectTimeout=12')
    p = subprocess.run(
        ["rsync", "-az", "--partial", "-e", ssh,
         f'{SSH_USER}@{pod["ssh_ip"]}:{remote_path.rstrip("/")}/',
         f'{local_path.rstrip("/")}/'],
        capture_output=True, text=True, timeout=timeout)
    return p.returncode, p.stderr


# -------------------------------------------------------------------- registry
BASE_SCHEMA = """
CREATE TABLE IF NOT EXISTS pods (
    pod_id TEXT PRIMARY KEY, owner_uuid TEXT, owner_label TEXT, gpu_type TEXT,
    ssh_ip TEXT, ssh_port INTEGER, status TEXT, cost_per_hr REAL,
    gpu_verified_at TEXT, deployed_at TEXT, last_heartbeat TEXT,
    terminated_at TEXT, notes TEXT);
CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT, ts TEXT, pod_id TEXT,
    actor_uuid TEXT, actor_label TEXT, action TEXT, detail TEXT);
"""
# v0.2 columns added by migration
NEW_COLS = {
    "kill_after": "TEXT", "idle_kill_min": "INTEGER", "last_job_heartbeat": "TEXT",
    "remote_path": "TEXT", "local_path": "TEXT", "deadman": "INTEGER DEFAULT 0",
    "last_gpu_util": "INTEGER",
    "idle_strikes": "INTEGER DEFAULT 0",   # consecutive idle reconciles (sustained-idle gate)
    "ssh_key": "TEXT",                     # per-pod SSH key (RunPod vs home-box differ)
}


class Registry:
    def __init__(self, owner_uuid=None, owner_label=None):
        self.uuid, self.label = whoami(owner_uuid, owner_label)
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(DB_PATH, timeout=30)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA busy_timeout=30000")
        self.db.executescript(BASE_SCHEMA)
        self._migrate()
        self.db.commit()

    def _migrate(self):
        have = {r["name"] for r in self.db.execute("PRAGMA table_info(pods)")}
        for col, typ in NEW_COLS.items():
            if col not in have:
                self.db.execute(f"ALTER TABLE pods ADD COLUMN {col} {typ}")

    def _log(self, pod_id, action, detail=""):
        self.db.execute("INSERT INTO events(ts,pod_id,actor_uuid,actor_label,action,detail)"
                        " VALUES (?,?,?,?,?,?)",
                        (_ts(), pod_id, self.uuid, self.label, action, detail))
        self.db.commit()

    # ---- ownership (continuity follows LABEL; UUID is forensic) ----
    def _owns(self, pod) -> str | bool:
        if pod["owner_uuid"] == self.uuid:
            return "uuid"
        if pod["owner_label"] == self.label and self.label != "(unlabeled)":
            return "label"
        return False

    def _maybe_reclaim(self, pod):
        """Same label, new session UUID (e.g. after /resume) -> take it over, logged."""
        if self._owns(pod) == "label" and pod["owner_uuid"] != self.uuid:
            self.db.execute("UPDATE pods SET owner_uuid=? WHERE pod_id=?",
                            (self.uuid, pod["pod_id"]))
            self.db.commit()
            self._log(pod["pod_id"], "reclaim",
                      f"label {self.label}: {pod['owner_uuid']} -> {self.uuid}")

    # ---- CRUD ----
    def register(self, pod_id, gpu_type=None, ssh_ip=None, ssh_port=None, cost_per_hr=None,
                 status="running", remote_path=None, local_path=None, ssh_key=None, notes=None):
        self.db.execute(
            "INSERT INTO pods(pod_id,owner_uuid,owner_label,gpu_type,ssh_ip,ssh_port,status,"
            "cost_per_hr,deployed_at,last_heartbeat,remote_path,local_path,ssh_key,notes) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?) "
            "ON CONFLICT(pod_id) DO UPDATE SET owner_uuid=excluded.owner_uuid,"
            "owner_label=excluded.owner_label,gpu_type=COALESCE(excluded.gpu_type,gpu_type),"
            "ssh_ip=COALESCE(excluded.ssh_ip,ssh_ip),ssh_port=COALESCE(excluded.ssh_port,ssh_port),"
            "status=excluded.status,remote_path=COALESCE(excluded.remote_path,remote_path),"
            "local_path=COALESCE(excluded.local_path,local_path),"
            "ssh_key=COALESCE(excluded.ssh_key,ssh_key)",
            (pod_id, self.uuid, self.label, gpu_type, ssh_ip, ssh_port, status, cost_per_hr,
             _ts(), _ts(), remote_path, local_path, ssh_key, notes))
        self.db.commit()
        self._log(pod_id, "register", f"{self.label} gpu={gpu_type}")
        return self.get(pod_id)

    def get(self, pod_id):
        r = self.db.execute("SELECT * FROM pods WHERE pod_id=?", (pod_id,)).fetchone()
        return dict(r) if r else None

    def list_pods(self, scope="all"):
        out = [dict(r) for r in self.db.execute("SELECT * FROM pods ORDER BY deployed_at")]
        if scope == "mine":
            out = [p for p in out if self._owns(p)]
        elif scope == "others":
            out = [p for p in out if not self._owns(p)]
        return out

    def claim(self, pod_id):
        pod = self.get(pod_id)
        if not pod:
            raise SystemExit(f"{pod_id} not in registry")
        prev = f"{pod['owner_label']}/{pod['owner_uuid']}"
        self.db.execute("UPDATE pods SET owner_uuid=?,owner_label=? WHERE pod_id=?",
                        (self.uuid, self.label, pod_id))
        self.db.commit()
        self._log(pod_id, "claim", f"{prev} -> {self.label}/{self.uuid}")
        return self.get(pod_id)

    def heartbeat(self, pod_id):
        self.db.execute("UPDATE pods SET last_heartbeat=? WHERE pod_id=?", (_ts(), pod_id))
        self.db.commit()
        self._log(pod_id, "heartbeat")

    # ---- GPU health probe (definitive, via SSH) ----
    def probe(self, pod_id):
        pod = self.get(pod_id)
        if not pod:
            raise SystemExit(f"{pod_id} not in registry")
        rc, out, err = ssh_run(
            pod, "nvidia-smi --query-gpu=utilization.gpu --format=csv,noheader,nounits "
                 "2>/dev/null | head -1")
        util = None
        if rc == 0 and out.strip().isdigit():
            util = int(out.strip())
            self.db.execute("UPDATE pods SET last_gpu_util=?,gpu_verified_at=? WHERE pod_id=?",
                            (util, _ts(), pod_id))
            self.db.commit()
        return util  # None = no GPU reachable / CPU-only

    # ---- artifact mirror ----
    def sync(self, pod_id):
        pod = self.get(pod_id)
        if not (pod and pod["remote_path"] and pod["local_path"]):
            return False, "no artifact spec (remote_path/local_path) registered"
        rc, err = rsync_pull(pod, pod["remote_path"], pod["local_path"])
        self._log(pod_id, "sync", "ok" if rc == 0 else f"rsync rc={rc}")
        return rc == 0, err

    # ---- dead-man switch ----
    def arm(self, pod_id, kill_in_min, token_file=None):
        """Plant an on-pod self-destruct that fires at now+kill_in_min unless petted."""
        pod = self.get(pod_id)
        if not pod:
            raise SystemExit(f"{pod_id} not in registry")
        deadline = _now() + timedelta(minutes=kill_in_min)
        key = Path(token_file).read_text().strip() if token_file else runpod_key()
        script = DEADMAN_SH.read_text()
        # install: drop key to tmpfs, write deadline, launch detached watchdog
        rc, out, err = ssh_run(pod, (
            f"mkdir -p /dev/shm/podtrack && umask 077 && "
            f"printf '%s' '{key}' > /dev/shm/podtrack/rpk && "
            f"printf '%s' '{pod_id}' > /dev/shm/podtrack/pod_id && "
            f"printf '%s' '{_ts(deadline)}' > /dev/shm/podtrack/deadline && "
            f"cat > /dev/shm/podtrack/deadman.sh <<'PTEOF'\n{script}\nPTEOF\n"
            f"chmod +x /dev/shm/podtrack/deadman.sh && "
            f"setsid nohup /dev/shm/podtrack/deadman.sh >/dev/shm/podtrack/deadman.log 2>&1 &"
        ), timeout=40)
        if rc != 0:
            raise SystemExit(f"arm failed (rc={rc}): {err or out}")
        self.db.execute("UPDATE pods SET kill_after=?,deadman=1 WHERE pod_id=?",
                        (_ts(deadline), pod_id))
        self.db.commit()
        self._log(pod_id, "arm", f"deadman fires {_ts(deadline)}")
        return _ts(deadline)

    def pet(self, pod_id, extend_min):
        """Slide the on-pod deadline forward (watchdog kept happy by a healthy job)."""
        pod = self.get(pod_id)
        deadline = _now() + timedelta(minutes=extend_min)
        rc, out, err = ssh_run(
            pod, f"printf '%s' '{_ts(deadline)}' > /dev/shm/podtrack/deadline")
        if rc == 0:
            self.db.execute("UPDATE pods SET kill_after=? WHERE pod_id=?",
                            (_ts(deadline), pod_id))
            self.db.commit()
            self._log(pod_id, "pet", f"deadline -> {_ts(deadline)}")
        return rc == 0

    # ---- teardown: pull -> verify -> kill ----
    def teardown(self, pod_id, force=False, skip_pull=False, as_reaper=False):
        """Owner-guarded, artifact-safe teardown.
          force      — operator override: bypass BOTH ownership and pull-verify.
          as_reaper  — policy enforcement: bypass OWNERSHIP only; pull-verify still
                       applies (refuses + leaves pod UP on sync failure, so the
                       on-pod dead-man switch + network volume remain the backstop).
          skip_pull  — skip the local mirror (e.g. killing an artifact-less leak)."""
        pod = self.get(pod_id)
        if not pod:
            raise SystemExit(f"{pod_id} not in registry; run reconcile first")
        if not self._owns(pod) and not force and not as_reaper:
            raise SystemExit(
                f"REFUSED: {pod_id} owned by '{pod['owner_label']}' ({pod['owner_uuid']}), "
                f"not you ('{self.label}'). Use --force only if certain, or `claim` it first.")
        if not as_reaper:
            self._maybe_reclaim(pod)
        # ARTIFACT GUARANTEE: pull + verify before any kill (skipped only by force/skip_pull).
        if not skip_pull and not force and pod["remote_path"] and pod["local_path"]:
            ok, err = self.sync(pod_id)
            local = Path(pod["local_path"])
            nonempty = local.exists() and any(local.rglob("*"))
            if not (ok and nonempty):
                raise SystemExit(
                    f"REFUSED: artifact pull/verify failed for {pod_id} "
                    f"(rsync_ok={ok}, local_nonempty={nonempty}). Pod left UP. "
                    f"Fix sync or use --force. ({err.strip()[:120]})")
        terminate_remote(pod_id)
        self.db.execute("UPDATE pods SET status='terminated', terminated_at=? WHERE pod_id=?",
                        (_ts(), pod_id))
        self.db.commit()
        self._log(pod_id, "force-teardown" if force else "teardown", f"was {pod['owner_label']}")
        return True

    # ---- reconcile ----
    def reconcile(self, terminate_untracked=False):
        remote = {p["pod_id"]: p for p in fetch_remote_pods()}
        known = {p["pod_id"]: p for p in self.list_pods("all")}
        s = {"live": [], "untracked": [], "vanished": [], "idle_or_cpu": []}
        for pid, rp in remote.items():
            running = rp["desired"] == "RUNNING"
            if running and (not rp["n_gpus"] or
                            (rp["gpu_util"] == 0 and (rp["uptime_s"] or 0) > 600)):
                s["idle_or_cpu"].append(pid)
            if pid in known:
                self.db.execute(
                    "UPDATE pods SET status=?,gpu_type=COALESCE(?,gpu_type),cost_per_hr=?,"
                    "ssh_ip=COALESCE(?,ssh_ip),ssh_port=COALESCE(?,ssh_port),"
                    "last_gpu_util=?,gpu_verified_at=? WHERE pod_id=?",
                    ("running" if running else "exited", rp["gpu_type"], rp["cost_per_hr"],
                     rp["ssh_ip"], rp["ssh_port"], rp["gpu_util"],
                     _ts() if rp["n_gpus"] else None, pid))
                s["live"].append(pid)
            else:
                self.db.execute(
                    "INSERT OR IGNORE INTO pods(pod_id,owner_uuid,owner_label,gpu_type,ssh_ip,"
                    "ssh_port,status,cost_per_hr,deployed_at,notes) VALUES (?,?,?,?,?,?,?,?,?,?)",
                    (pid, "UNKNOWN", "UNTRACKED", rp["gpu_type"], rp["ssh_ip"], rp["ssh_port"],
                     "untracked", rp["cost_per_hr"], _ts(), "discovered by reconcile"))
                s["untracked"].append(pid)
                self._log(pid, "reconcile", "found untracked/leaked pod")
        for pid, kp in known.items():
            if pid not in remote and kp["status"] in LIVE_STATUSES:
                self.db.execute("UPDATE pods SET status='terminated',terminated_at=? "
                                "WHERE pod_id=?", (_ts(), pid))
                s["vanished"].append(pid)
        self.db.commit()
        if terminate_untracked:
            for pid in s["untracked"]:
                self.teardown(pid, force=True, skip_pull=True)
        return s

    # ---- job heartbeat (positive liveness signal) ----
    def job_heartbeat(self, pod_id):
        """Mark the job alive NOW (for local jobs; remote jobs use JOB_HB_FILE)."""
        self.db.execute("UPDATE pods SET last_job_heartbeat=? WHERE pod_id=?", (_ts(), pod_id))
        self.db.commit()

    def _read_remote_heartbeat(self, pod):
        """Best-effort: pull the job's heartbeat file (ISO-UTC ts) from the pod via SSH.
        Jobs keep it fresh with e.g. `while :; do date -u +%FT%TZ > JOB_HB_FILE; sleep 60; done`."""
        rc, out, _ = ssh_run(pod, f"cat {JOB_HB_FILE} 2>/dev/null", timeout=20)
        ts = out.strip()
        if rc == 0 and ts:
            try:
                _parse(ts)                      # validate ISO-8601
                self.db.execute("UPDATE pods SET last_job_heartbeat=? WHERE pod_id=?",
                                (ts, pod["pod_id"]))
                self.db.commit()
            except Exception:
                pass

    # ---- autonomous reaper (run by the systemd timer) ----
    def reap(self, mirror=True, pet_min=30, startup_grace_min=15,
             idle_strikes_needed=3, hb_grace_min=20):
        """Autonomous: reconcile -> mirror -> pet healthy -> safe-teardown TTL/idle pods.

        v0.2.1 safety (a momentary 0%-GPU snapshot no longer kills an active pod):
          - startup grace: never idle-kill a pod younger than `startup_grace_min`.
          - SUSTAINED idle: idle-kill needs `idle_strikes_needed` CONSECUTIVE idle
            reconciles (busy GPU resets the counter), not a single snapshot.
          - heartbeat override: a fresh job heartbeat (< `hb_grace_min`) blocks idle-kill.
        A hard `kill_after` TTL still fires regardless (the owner set it deliberately)."""
        recon = self.reconcile()
        report = {"reaped": [], "petted": [], "mirrored": [], "kept": [], **recon}
        now = _now()
        for pod in self.list_pods("all"):
            if pod["status"] != "running":
                continue
            pid = pod["pod_id"]
            if mirror and pod["remote_path"]:
                ok, _ = self.sync(pid)
                if ok:
                    report["mirrored"].append(pid)
            self._read_remote_heartbeat(pod)
            pod = self.get(pid)                                  # reload after hb/strike updates
            # sustained-idle accounting
            gpu_idle_now = pid in recon["idle_or_cpu"]
            strikes = (pod["idle_strikes"] or 0) + 1 if gpu_idle_now else 0
            self.db.execute("UPDATE pods SET idle_strikes=? WHERE pod_id=?", (strikes, pid))
            self.db.commit()
            # decision
            dep = _parse(pod["deployed_at"])
            young = dep is not None and (now - dep) < timedelta(minutes=startup_grace_min)
            hb = _parse(pod["last_job_heartbeat"])
            fresh_hb = hb is not None and (now - hb) < timedelta(minutes=hb_grace_min)
            ttl_expired = pod["kill_after"] and now > _parse(pod["kill_after"])
            idle_kill = (strikes >= idle_strikes_needed) and not young and not fresh_hb
            if ttl_expired or idle_kill:
                try:
                    self.teardown(pid, as_reaper=True)          # bypass ownership; keep pull-verify
                    report["reaped"].append((pid, "ttl" if ttl_expired else f"idle x{strikes}"))
                except SystemExit as e:
                    report["kept"].append((pid, f"refused: {str(e)[:60]}"))
            elif pod["deadman"]:
                self.pet(pid, pet_min)
                report["petted"].append(pid)
            else:
                report["kept"].append(
                    (pid, f"strikes={strikes} young={young} fresh_hb={bool(fresh_hb)}"))
        return report


# ------------------------------------------------------------------------- CLI
def _print_pods(pods):
    if not pods:
        print("  (none)"); return
    print(f"  {'pod_id':<16}{'status':<12}{'gpu':<20}{'owner':<16}{'util':<6}{'kill_after':<22}ssh")
    for p in pods:
        ssh = f"{p['ssh_ip']}:{p['ssh_port']}" if p["ssh_ip"] else "-"
        print(f"  {p['pod_id']:<16}{(p['status'] or '?'):<12}{(p['gpu_type'] or '?'):<20}"
              f"{(p['owner_label'] or '?'):<16}{str(p['last_gpu_util'] if p['last_gpu_util'] is not None else '-'):<6}"
              f"{(p['kill_after'] or '-'):<22}{ssh}")


def main():
    ap = argparse.ArgumentParser(prog="podtrack", description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("whoami"); sub.add_parser("adopt-key")
    r = sub.add_parser("register"); r.add_argument("pod_id")
    for f in ("--label", "--gpu", "--ssh-ip", "--remote-path", "--local-path", "--ssh-key"):
        r.add_argument(f)
    r.add_argument("--ssh-port", type=int); r.add_argument("--cost", type=float)
    jh = sub.add_parser("job-heartbeat"); jh.add_argument("pod_id")
    li = sub.add_parser("list"); gg = li.add_mutually_exclusive_group()
    for fl in ("all", "mine", "others"):
        gg.add_argument(f"--{fl}", action="store_const", const=fl, dest="scope")
    c = sub.add_parser("claim"); c.add_argument("pod_id"); c.add_argument("--label")
    pr = sub.add_parser("probe"); pr.add_argument("pod_id")
    sy = sub.add_parser("sync"); sy.add_argument("pod_id")
    hb = sub.add_parser("heartbeat"); hb.add_argument("pod_id")
    am = sub.add_parser("arm"); am.add_argument("pod_id")
    am.add_argument("--kill-in", type=int, required=True, help="minutes until self-destruct")
    am.add_argument("--token-file", help="restricted RunPod token for the on-pod switch")
    pt = sub.add_parser("pet"); pt.add_argument("pod_id"); pt.add_argument("--min", type=int, default=30)
    td = sub.add_parser("teardown"); td.add_argument("pod_id")
    td.add_argument("--force", action="store_true"); td.add_argument("--skip-pull", action="store_true")
    rc = sub.add_parser("reconcile"); rc.add_argument("--terminate-untracked", action="store_true")
    rp = sub.add_parser("reap"); rp.add_argument("--no-mirror", action="store_true")
    a = ap.parse_args()

    if a.cmd == "adopt-key":
        print(adopt_key()); return
    if a.cmd == "whoami":
        u, l = whoami(); print(f"owner_uuid: {u}\nowner_label: {l}"); return
    reg = Registry(owner_label=getattr(a, "label", None))
    if a.cmd == "register":
        p = reg.register(a.pod_id, gpu_type=a.gpu, ssh_ip=a.ssh_ip, ssh_port=a.ssh_port,
                         cost_per_hr=a.cost, remote_path=a.remote_path, local_path=a.local_path,
                         ssh_key=a.ssh_key)
        print(f"registered {p['pod_id']} -> '{p['owner_label']}'")
    elif a.cmd == "job-heartbeat":
        reg.job_heartbeat(a.pod_id); print(f"job-heartbeat {a.pod_id}")
    elif a.cmd == "list":
        _print_pods(reg.list_pods(a.scope or "all"))
    elif a.cmd == "claim":
        p = reg.claim(a.pod_id); print(f"claimed {a.pod_id} -> '{p['owner_label']}'")
    elif a.cmd == "probe":
        u = reg.probe(a.pod_id); print(f"{a.pod_id} GPU util: {u if u is not None else 'NO GPU / unreachable'}%")
    elif a.cmd == "sync":
        ok, err = reg.sync(a.pod_id); print("synced" if ok else f"sync failed: {err}")
    elif a.cmd == "heartbeat":
        reg.heartbeat(a.pod_id); print(f"heartbeat {a.pod_id}")
    elif a.cmd == "arm":
        d = reg.arm(a.pod_id, a.kill_in, token_file=a.token_file)
        print(f"armed {a.pod_id}: dead-man fires {d}")
    elif a.cmd == "pet":
        print("petted" if reg.pet(a.pod_id, a.min) else "pet failed")
    elif a.cmd == "teardown":
        reg.teardown(a.pod_id, force=a.force, skip_pull=a.skip_pull); print(f"torn down {a.pod_id}")
    elif a.cmd == "reconcile":
        s = reg.reconcile(terminate_untracked=a.terminate_untracked)
        print(f"live={len(s['live'])} untracked={s['untracked']} vanished={s['vanished']} "
              f"idle_or_cpu={s['idle_or_cpu']}")
    elif a.cmd == "reap":
        rep = reg.reap(mirror=not a.no_mirror)
        print(f"reaped={rep['reaped']} petted={rep['petted']} mirrored={len(rep['mirrored'])} "
              f"untracked={rep['untracked']} kept={len(rep['kept'])}")


if __name__ == "__main__":
    main()
