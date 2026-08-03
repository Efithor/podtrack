#!/usr/bin/env python3
"""podtrack — a shared, ownership-aware registry + autonomous reaper for RunPod pods.

Why this exists
---------------
Several independent workers share one RunPod account. Without ownership they
clobber each other's pods (lost/orphaned -> silent billing), tear down each
other's live pods, and leave idle/CPU pods running for hours. podtrack replaces
a single unlocked JSON state file with a concurrency-safe SQLite registry that
records WHO owns each pod, refuses cross-owner teardowns, and autonomously reaps
idle/expired pods WITHOUT losing artifacts.

Capabilities
------------
1. Registry        — one row per pod, shared across workers (SQLite, WAL).
2. Ownership       — owner = (uuid, friendly-label). Continuity follows the LABEL
                     (the uuid is ephemeral across restarts); teardown is guarded.
3. Reconcile       — diff registry vs live RunPod; catch leaks, mark vanished,
                     flag GPU-vs-CPU / idle.
4. Artifact safety — every teardown is pull -> verify -> kill; a periodic mirror
                     bounds loss on ungraceful death. Artifacts live on the RunPod
                     network volume so any kill is data-safe.
5. Autonomous reap — `reap` (run by a systemd timer) reconciles, mirrors, pets
                     healthy pods' dead-man switch, and safe-tears-down
                     TTL-expired / idle pods.
6. Dead-man switch — `arm` plants an on-pod self-destruct (default-to-death) so a
                     pod self-terminates at its deadline even if the host machine
                     is asleep. The reaper "pets" it (slides the deadline) while a
                     job is healthy.
7. Multi-account   — every pod row carries the RunPod ACCOUNT it lives on; keys
                     are held per account and reconcile/reap sweep all configured
                     accounts, so pods on a second account are never invisible.

Credential mandate: podtrack is custodian of every RunPod key
(`adopt-key [--account NAME --from PATH]`).
"""
from __future__ import annotations

import argparse
import json
import os
import re
import socket
import sqlite3
import subprocess
import sys
import time
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
# Restricted RunPod token planted in the on-pod dead-man switch (failure mode #4).
# Prefer a token scoped as narrowly as the account allows (ideally podTerminate-
# only) so a compromised/leaked pod can self-destruct but not touch the account.
DEADMAN_TOKEN_PATH = CRED_DIR / "deadman.token"
LEGACY_KEY = Path.home() / ".keys/runpod"
RUNPOD_API = "https://api.runpod.io/graphql"
SSH_KEYS = [Path.home() / ".runpod/ssh/RunPod-Key-Go",   # RunPod pods
            Path.home() / ".ssh/id_ed25519"]             # generic fallback
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
DEADMAN_SUP = Path(__file__).with_name("deadman_supervisor.sh")   # respawns the watchdog (#3)
JOB_HB_FILE = "/root/.podtrack_job_alive"      # job writes ISO-UTC ts here; reaper reads it


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _ts(dt: datetime | None = None) -> str:
    return (dt or _now()).strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse(ts: str | None) -> datetime | None:
    if not ts:
        return None
    return datetime.strptime(ts, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)


def _cfg(cli_val, env_name, default) -> int:
    """Resolve a reaper knob: CLI arg > env var > built-in default."""
    if cli_val is not None:
        return cli_val
    env = os.environ.get(env_name)
    return int(env) if env else default


# ----------------------------------------------------------------------- identity
def whoami(uuid: str | None = None, label: str | None = None) -> tuple[str, str]:
    """Resolve (owner_uuid, owner_label). The uuid is ephemeral (changes across
    restarts); LABEL is the stable task identity that ownership continuity follows.
    CLAUDE_SESSION_ID is read as an optional fallback for agent-runner setups."""
    u = uuid or os.environ.get("PODTRACK_OWNER_UUID") or os.environ.get("CLAUDE_SESSION_ID")
    if not u:
        marker = DATA_DIR / "owner_id"
        if marker.exists():
            u = marker.read_text().strip()
        else:
            u = f"host-{socket.gethostname()}-{_uuid.uuid4().hex[:8]}"
            DATA_DIR.mkdir(parents=True, exist_ok=True)
            marker.write_text(u)
        print(f"# podtrack: no owner uuid in env; using fallback '{u}'. "
              f"Set PODTRACK_OWNER_UUID + PODTRACK_LABEL to distinguish workers.",
              file=sys.stderr)
    return u, (label or os.environ.get("PODTRACK_LABEL") or "(unlabeled)")


# --------------------------------------------------------------- key custodian
# Accounts are named credential slots. "main" is the original single account
# (custody file `runpod.key`); any other account NAME keeps its key at
# `runpod.<NAME>.key`. A pod row records which account it lives on, so every
# API call about that pod authenticates against the right key.
ACCOUNT_RE = re.compile(r"^[a-z0-9][a-z0-9_-]*$")


def _cred_path(account: str = "main") -> Path:
    if not ACCOUNT_RE.match(account):
        raise SystemExit(f"bad account name '{account}' (want lowercase [a-z0-9_-])")
    return CRED_PATH if account == "main" else CRED_DIR / f"runpod.{account}.key"


def configured_accounts() -> list[str]:
    """Accounts with a key in custody, 'main' first. This is what reconcile/reap
    sweep — a pod on an account without a key here is INVISIBLE to podtrack."""
    accts = ["main"] if CRED_PATH.exists() else []
    for p in sorted(CRED_DIR.glob("runpod.*.key")):
        name = p.name[len("runpod."):-len(".key")]
        if name and ACCOUNT_RE.match(name):
            accts.append(name)
    return accts


def adopt_key(account: str = "main", source: str | None = None) -> str:
    cred = _cred_path(account)
    CRED_DIR.mkdir(parents=True, exist_ok=True)
    if cred.exists():
        return f"already adopted: {cred}"
    src = (Path(source).expanduser() if source
           else LEGACY_KEY if account == "main"
           else Path.home() / f".keys/runpod-{account}")
    if not src.exists():
        raise SystemExit(f"no key at {src} to adopt (and none at {cred})")
    text = src.read_text()
    if "MOVED" in text:
        raise SystemExit(f"{src} is already a moved-notice stub; nothing to adopt")
    cred.write_text(text)
    cred.chmod(0o600)
    src.write_text(
        f"# MOVED. The RunPod API key (account '{account}') is now managed by podtrack.\n"
        "# Do NOT read this file or call the RunPod API directly.\n"
        f"# Use: podtrack <cmd>.  Key custodian: {cred}\n")
    src.chmod(0o600)
    return f"adopted [{account}]: {src} -> {cred} (source path now a notice)"


def runpod_key(account: str = "main") -> str:
    cred = _cred_path(account)
    if cred.exists():
        return cred.read_text().strip()
    if (account == "main" and LEGACY_KEY.exists()
            and "MOVED" not in LEGACY_KEY.read_text(errors="replace")):
        raise SystemExit(f"RunPod key still at {LEGACY_KEY}; run `podtrack adopt-key`.")
    raise SystemExit(f"no RunPod key for account '{account}' at {cred}; "
                     f"run `podtrack adopt-key --account {account} [--from PATH]`.")


def deadman_token(token_file: str | None = None, account: str = "main") -> tuple[str, str]:
    """Resolve the credential to plant in the on-pod dead-man switch (failure mode #4).

    Order: explicit --token-file  >  restricted token for the pod's ACCOUNT
    (`deadman.token` for main, `deadman.<account>.token` otherwise)  >  (loud
    fallback) that account's full key. The full key sitting plaintext in a
    pod's /dev/shm is the failure we're closing: a restricted token means a
    leaked pod can only terminate ITSELF, not the whole account. The credential
    MUST belong to the pod's own account — a cross-account podTerminate is
    silently unauthorized and the switch would never confirm the kill. Returns
    (secret, human-readable-source); callers WARN when the source is the full key."""
    if token_file:
        return Path(token_file).read_text().strip(), f"token-file {token_file}"
    tok = (DEADMAN_TOKEN_PATH if account == "main"
           else CRED_DIR / f"deadman.{account}.token")
    if tok.exists():
        return tok.read_text().strip(), f"restricted token {tok}"
    return runpod_key(account), f"FULL ACCOUNT KEY '{account}' (no restricted token configured)"


# ------------------------------------------------------------------ RunPod API
def gql(query: str, variables: dict | None = None, account: str = "main") -> dict:
    body = json.dumps({"query": query, "variables": variables or {}}).encode()
    # #15: send the key in an Authorization header, NOT the URL query string, so it
    # can't leak into access logs / proxies / crash traces. RunPod accepts
    # `Authorization: Bearer <key>` (confirmed 2026-07-05).
    req = urllib.request.Request(
        RUNPOD_API, data=body,
        headers={"Content-Type": "application/json", "User-Agent": "Mozilla/5.0 (podtrack)",
                 "Authorization": f"Bearer {runpod_key(account)}"})
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            out = json.loads(r.read().decode())
    except urllib.error.HTTPError as e:
        raise SystemExit(f"RunPod HTTP {e.code}: {e.read().decode(errors='replace')[:300]}")
    if out.get("errors"):
        raise SystemExit(f"RunPod GraphQL error: {out['errors']}")
    return out.get("data", {})


def fetch_remote_pods(account: str = "main") -> list[dict]:
    q = """query { myself { pods {
        id name desiredStatus costPerHr
        machine { gpuDisplayName }
        runtime { uptimeInSeconds gpus { id gpuUtilPercent }
                  ports { ip publicPort privatePort type } } } } }"""
    pods = (gql(q, account=account).get("myself") or {}).get("pods") or []
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
            "ssh_ip": ssh_ip, "ssh_port": ssh_port, "account": account})
    return norm


def terminate_remote(pod_id: str, account: str = "main"):
    gql("mutation($i:PodTerminateInput!){podTerminate(input:$i)}", {"i": {"podId": pod_id}},
        account=account)


# ---- ephemeral network-volume lifecycle (failure mode #10 / policy P-2) ----
# Schema confirmed empirically against api.runpod.io 2026-07-05 (introspection is
# disabled server-side; verified via query-validation probes):
#   createNetworkVolume(input:{name,size,dataCenterId!}) -> NetworkVolume{id name size dataCenterId}
#   deleteNetworkVolume(input:{id!})
#   myself.networkVolumes{id name size dataCenterId}
#   dataCenters{id storageSupport gpuAvailability{gpuTypeId available stockStatus}}
EPH_VOLUME_PREFIX = "pt-eph-"      # names ephemeral, sweepable volumes (P-1 leak sweep)
_STOCK_RANK = {"High": 3, "Medium": 2, "Low": 1, None: 0}


def create_network_volume(name: str, size_gb: int, datacenter_id: str,
                          account: str = "main") -> dict:
    q = ("mutation($i:CreateNetworkVolumeInput!){createNetworkVolume(input:$i)"
         "{id name size dataCenterId}}")
    return gql(q, {"i": {"name": name, "size": size_gb, "dataCenterId": datacenter_id}},
               account=account)["createNetworkVolume"]


def delete_network_volume(volume_id: str, account: str = "main") -> None:
    gql("mutation($i:DeleteNetworkVolumeInput!){deleteNetworkVolume(input:$i)}",
        {"i": {"id": volume_id}}, account=account)


def list_network_volumes(account: str = "main") -> list[dict]:
    return ((gql("query{myself{networkVolumes{id name size dataCenterId}}}",
                 account=account).get("myself") or {})
            .get("networkVolumes") or [])


def datacenters_for_gpu(gpu_type_id: str, account: str = "main") -> list[dict]:
    """DCs that BOTH support network volumes AND currently have `gpu_type_id`
    available, best-stock first. Resolves the datacenter oddity: the ephemeral
    volume is created in a DC we then pin the pod to, so it can never silently
    unmount. Returns [{'id','stock'}] ranked High>Medium>Low."""
    q = ("query{dataCenters{id storageSupport "
         "gpuAvailability{gpuTypeId available stockStatus}}}")
    out = []
    for dc in (gql(q, account=account).get("dataCenters") or []):
        if not dc.get("storageSupport"):
            continue
        for ga in (dc.get("gpuAvailability") or []):
            if ga.get("gpuTypeId") == gpu_type_id and ga.get("available"):
                out.append({"id": dc["id"], "stock": ga.get("stockStatus"),
                            "_rank": _STOCK_RANK.get(ga.get("stockStatus"), 0)})
                break
    out.sort(key=lambda d: -d["_rank"])
    return out


def pod_desired_status(pod_id: str, account: str = "main") -> str | None:
    """Live desiredStatus for a pod, or None if it's gone (confirmation signal)."""
    q = "query($id:String!){pod(input:{podId:$id}){desiredStatus}}"
    pod = gql(q, {"id": pod_id}, account=account).get("pod")
    return (pod or {}).get("desiredStatus") if pod else None


# ----------------------------------------------------------------- ssh helpers
def _ssh_base(pod):
    return ["ssh", "-i", _ssh_key_for(pod), "-p", str(pod["ssh_port"]),
            "-o", "StrictHostKeyChecking=no", "-o", "ConnectTimeout=30",
            f"{SSH_USER}@{pod['ssh_ip']}"]


def ssh_run(pod, remote_cmd, timeout=60, retries=2, warn=True):
    """Run a command over SSH, treating transport failure as first-class (#14):
    retry transient errors with backoff and surface a WARN. rc 255 == SSH
    transport failure (unreachable); any other rc means the command actually ran."""
    if not pod.get("ssh_ip"):
        return 255, "", "no ssh endpoint in registry (run reconcile)"
    last = (255, "", "ssh transport error")
    for attempt in range(1, retries + 1):
        try:
            p = subprocess.run(_ssh_base(pod) + [remote_cmd],
                               capture_output=True, text=True, timeout=timeout)
            if p.returncode != 255:          # command ran (even if it exited nonzero)
                return p.returncode, p.stdout, p.stderr
            last = (255, p.stdout, (p.stderr or "ssh transport error").strip())
        except subprocess.TimeoutExpired:
            last = (255, "", "ssh timeout")
        if attempt < retries:
            time.sleep(min(2 * attempt, 8))
    if warn:
        print(f"# WARN: ssh to {pod.get('pod_id')} @ {pod.get('ssh_ip')}:{pod.get('ssh_port')} "
              f"failed after {retries} attempts: {last[2][:120]}", file=sys.stderr)
    return last


def rsync_pull(pod, remote_path, local_path, timeout=1800):
    Path(local_path).mkdir(parents=True, exist_ok=True)
    ssh = (f'ssh -i {_ssh_key_for(pod)} -p {pod["ssh_port"]} '
           f'-o StrictHostKeyChecking=no -o ConnectTimeout=30')
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
    "volume_id": "TEXT",                   # ephemeral network volume attached to this pod (P-2)
    "volume_deleted_at": "TEXT",           # set when the ephemeral volume is deleted at teardown
    "deadman_verified_at": "TEXT",         # last time the on-pod watchdog was confirmed alive (#1)
    "ssh_fail_streak": "INTEGER DEFAULT 0",  # consecutive reaps SSH was unreachable (#8/#14)
    "vanish_strikes": "INTEGER DEFAULT 0",   # consecutive fetches a live pod was absent (#11 guard)
    "kill_after_absolute": "TEXT",         # hard TTL cap that ignores heartbeats (#6)
    "no_artifacts": "INTEGER DEFAULT 0",   # opt-out: short job with intentionally no artifacts (#9)
    "account": "TEXT DEFAULT 'main'",      # which RunPod account the pod lives on (v0.4)
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

    @staticmethod
    def _acct(pod) -> str:
        """Account a pod row lives on ('main' for pre-v0.4 rows)."""
        return pod.get("account") or "main"

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
                 status="running", remote_path=None, local_path=None, ssh_key=None,
                 volume_id=None, no_artifacts=False, notes=None, account=None):
        account = account or os.environ.get("PODTRACK_ACCOUNT") or "main"
        # A pod filed under an account with no key in custody would be invisible
        # to reconcile/reap forever — the exact leak podtrack exists to prevent.
        if account not in configured_accounts():
            raise SystemExit(
                f"account '{account}' has no key in custody (configured: "
                f"{configured_accounts() or 'NONE'}). Run `podtrack adopt-key "
                f"--account {account} --from PATH` first.")
        self.db.execute(
            "INSERT INTO pods(pod_id,owner_uuid,owner_label,gpu_type,ssh_ip,ssh_port,status,"
            "cost_per_hr,deployed_at,last_heartbeat,remote_path,local_path,ssh_key,volume_id,"
            "no_artifacts,notes,account) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) "
            "ON CONFLICT(pod_id) DO UPDATE SET owner_uuid=excluded.owner_uuid,"
            "owner_label=excluded.owner_label,gpu_type=COALESCE(excluded.gpu_type,gpu_type),"
            "ssh_ip=COALESCE(excluded.ssh_ip,ssh_ip),ssh_port=COALESCE(excluded.ssh_port,ssh_port),"
            "status=excluded.status,remote_path=COALESCE(excluded.remote_path,remote_path),"
            "local_path=COALESCE(excluded.local_path,local_path),"
            "ssh_key=COALESCE(excluded.ssh_key,ssh_key),"
            "volume_id=COALESCE(excluded.volume_id,volume_id),"
            "no_artifacts=excluded.no_artifacts,account=excluded.account",
            (pod_id, self.uuid, self.label, gpu_type, ssh_ip, ssh_port, status, cost_per_hr,
             _ts(), _ts(), remote_path, local_path, ssh_key, volume_id,
             1 if no_artifacts else 0, notes, account))
        self.db.commit()
        self._log(pod_id, "register", f"{self.label} gpu={gpu_type} acct={account}"
                  + (f" vol={volume_id}" if volume_id else ""))
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
    DEADMAN_DIR = "/dev/shm/podtrack"
    # The SUPERVISOR is what guarantees a watchdog is present (it respawns
    # deadman.sh if it dies), so it is the process we verify liveness against (#3).
    DEADMAN_PROC = "/dev/shm/podtrack/deadman_supervisor.sh"

    def _deadman_alive(self, pod) -> bool:
        """Definitive check that the on-pod dead-man SUPERVISOR is running
        (failure modes #1/#3: `arm` is fire-and-forget; a 'protected' pod may have no
        watchdog after a silent launch failure, pod restart, or OOM). The supervisor
        keeps deadman.sh respawned, so its presence == a live watchdog.
        ⚠ The pattern MUST be bracketed: a plain `pgrep -f <path>` over SSH matches
        the remote `bash -c` wrapper's own cmdline (which contains the pattern) and
        reports ALIVE unconditionally — silently defeating the verification."""
        rc, out, _ = ssh_run(
            pod, "pgrep -f '[d]eadman_supervisor[.]sh' >/dev/null && echo ALIVE || echo DEAD",
            timeout=20)
        return rc == 0 and "ALIVE" in out

    def _launch_deadman(self, pod):
        return ssh_run(
            pod, f"setsid nohup bash {self.DEADMAN_PROC} "
                 f"</dev/null >{self.DEADMAN_DIR}/supervisor.log 2>&1 &", timeout=20)

    def arm(self, pod_id, kill_in_min, token_file=None, kill_absolute_min=None):
        """Plant an on-pod self-destruct that fires at now+kill_in_min unless petted,
        then VERIFY the watchdog is actually alive before claiming the pod is
        protected (failure mode #1). Uses a restricted token by default (#4).
        `kill_absolute_min` sets a HARD cap (`kill_after_absolute`) the reaper honors
        even if the job is heartbeating (#6) — the soft deadline can be petted, the
        absolute one cannot."""
        pod = self.get(pod_id)
        if not pod:
            raise SystemExit(f"{pod_id} not in registry")
        deadline = _now() + timedelta(minutes=kill_in_min)
        abs_deadline = (_now() + timedelta(minutes=kill_absolute_min)
                        if kill_absolute_min else None)
        key, key_src = deadman_token(token_file, account=self._acct(pod))
        if "FULL ACCOUNT KEY" in key_src:
            print(f"# WARNING: arming {pod_id} with the FULL RunPod account key in the pod's "
                  f"/dev/shm. Prefer a restricted token — drop one at {DEADMAN_TOKEN_PATH} "
                  f"(scoped to podTerminate). See README (dead-man credential).", file=sys.stderr)
        else:
            print(f"# arm: dead-man credential = {key_src}", file=sys.stderr)
        script = DEADMAN_SH.read_text()
        supervisor = DEADMAN_SUP.read_text()
        # install: drop key to tmpfs, write deadline + both scripts, launch the
        # SUPERVISOR detached (it keeps deadman.sh respawned — #3).
        rc, out, err = ssh_run(pod, (
            f"mkdir -p /dev/shm/podtrack && umask 077 && "
            f"printf '%s' '{key}' > /dev/shm/podtrack/rpk && "
            f"printf '%s' '{pod_id}' > /dev/shm/podtrack/pod_id && "
            f"printf '%s' '{_ts(deadline)}' > /dev/shm/podtrack/deadline && "
            f"cat > /dev/shm/podtrack/deadman.sh <<'PTEOF'\n{script}\nPTEOF\n"
            f"cat > /dev/shm/podtrack/deadman_supervisor.sh <<'PTSUP'\n{supervisor}\nPTSUP\n"
            f"chmod +x /dev/shm/podtrack/deadman.sh /dev/shm/podtrack/deadman_supervisor.sh && "
            f"setsid nohup bash /dev/shm/podtrack/deadman_supervisor.sh "   # bash: /dev/shm is noexec on RunPod images
            f"</dev/null >/dev/shm/podtrack/supervisor.log 2>&1 &"
        ), timeout=40)
        if rc != 0:
            raise SystemExit(f"arm failed (rc={rc}): {err or out}")
        # VERIFY (#1): the watchdog must actually be running, else this pod is
        # unprotected despite deadman=1. One relaunch attempt, then hard-fail.
        alive = self._deadman_alive(pod)
        if not alive:
            self._launch_deadman(pod)
            alive = self._deadman_alive(pod)
        if not alive:
            raise SystemExit(
                f"arm: dead-man watchdog failed to start on {pod_id} (pgrep found "
                f"no {self.DEADMAN_PROC}). Pod is NOT protected — investigate the pod.")
        # Preserve any existing absolute cap on re-arm (reap calls arm() without it).
        if abs_deadline is not None:
            self.db.execute("UPDATE pods SET kill_after=?,kill_after_absolute=?,deadman=1,"
                            "deadman_verified_at=? WHERE pod_id=?",
                            (_ts(deadline), _ts(abs_deadline), _ts(), pod_id))
        else:
            self.db.execute("UPDATE pods SET kill_after=?,deadman=1,deadman_verified_at=? "
                            "WHERE pod_id=?", (_ts(deadline), _ts(), pod_id))
        self.db.commit()
        self._log(pod_id, "arm", f"deadman fires {_ts(deadline)} (verified alive)"
                  + (f" abs-cap {_ts(abs_deadline)}" if abs_deadline else ""))
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
        # #9: a long-lived pod with NO artifact spec is almost certainly a mistake
        # (P-2 gives every >1 h job a remote_path). Warn loudly; short jobs silence
        # it with `register --no-artifacts`. Skipped for --force/--skip-pull kills.
        if not skip_pull and not force and not pod["remote_path"] and not pod["no_artifacts"]:
            dep = _parse(pod["deployed_at"])
            if dep and (_now() - dep) > timedelta(hours=1):
                hrs = (_now() - dep).total_seconds() / 3600
                print(f"# WARN: tearing down {pod_id} (up {hrs:.1f}h) with NO artifact spec "
                      f"— any outputs are NOT being pulled and will be lost. Set "
                      f"--remote-path/--local-path, or `register --no-artifacts` to silence.",
                      file=sys.stderr)
                self._log(pod_id, "artifactless-teardown", f"up {hrs:.1f}h, no remote_path")
        # ARTIFACT GUARANTEE: pull + verify before any kill (skipped only by force/skip_pull).
        pull_verified = False
        if not skip_pull and not force and pod["remote_path"] and pod["local_path"]:
            ok, err = self.sync(pod_id)
            local = Path(pod["local_path"])
            nonempty = local.exists() and any(local.rglob("*"))
            if not (ok and nonempty):
                raise SystemExit(
                    f"REFUSED: artifact pull/verify failed for {pod_id} "
                    f"(rsync_ok={ok}, local_nonempty={nonempty}). Pod left UP. "
                    f"Fix sync or use --force. ({err.strip()[:120]})")
            pull_verified = True
        terminate_remote(pod_id, account=self._acct(pod))
        self.db.execute("UPDATE pods SET status='terminated', terminated_at=? WHERE pod_id=?",
                        (_ts(), pod_id))
        self.db.commit()
        self._log(pod_id, "force-teardown" if force else "teardown", f"was {pod['owner_label']}")
        # P-2: the ephemeral volume dies with the pod — nothing persists on RunPod.
        # (Unless it holds unpulled artifacts — see the data guard in _delete_volume.)
        self._delete_volume(pod, pull_verified=pull_verified)
        return True

    def _delete_volume(self, pod, pull_verified=False):
        """Delete a pod's ephemeral network volume after teardown (P-1/P-2). Best-
        effort but LOUD on failure — a leaked volume bills storage indefinitely.
        DATA GUARD (2026-07-05): if the pod declared artifacts (remote_path) and
        this teardown did NOT verify a pull, the volume is the ONLY copy of the
        data — keep it (it bills; sweep manually after rescuing the artifacts)."""
        vid = pod.get("volume_id")
        if not vid or pod.get("volume_deleted_at"):
            return
        if pod.get("remote_path") and not pull_verified and not pod.get("no_artifacts"):
            print(f"# WARNING: keeping volume {vid} of {pod['pod_id']} — artifacts were "
                  f"NOT pull-verified this teardown. Rescue the data, then "
                  f"`podtrack sweep-volumes --force`.", file=sys.stderr)
            self._log(pod["pod_id"], "volume-kept-unpulled", vid)
            return
        try:
            delete_network_volume(vid, account=self._acct(pod))
            self.db.execute("UPDATE pods SET volume_deleted_at=? WHERE pod_id=?",
                            (_ts(), pod["pod_id"]))
            self.db.commit()
            self._log(pod["pod_id"], "volume-delete", vid)
        except SystemExit as e:
            print(f"# WARNING: failed to delete ephemeral volume {vid} for {pod['pod_id']}: "
                  f"{str(e)[:160]}. It will bill storage until swept "
                  f"(`podtrack sweep-volumes`).", file=sys.stderr)
            self._log(pod["pod_id"], "volume-delete-failed", f"{vid}: {str(e)[:120]}")

    def sweep_volumes(self, force=False):
        """Leak sweep (P-1 enforcement): delete ephemeral `pt-eph-*` volumes not
        referenced by any live pod in the registry, across ALL configured
        accounts. Volumes unknown to the registry are only deleted with
        force=True (a fresh volume mid-deploy is briefly unknown — avoid racing
        it). Returns (deleted, skipped)."""
        live_vids = {p["volume_id"] for p in self.list_pods("all")
                     if p.get("volume_id") and p["status"] in LIVE_STATUSES}
        known_vids = {p["volume_id"] for p in self.list_pods("all") if p.get("volume_id")}
        deleted, skipped = [], []
        for acct in configured_accounts():
            try:
                vols = list_network_volumes(acct)
            except SystemExit as e:
                skipped.append((f"[{acct}]", f"volume list failed: {str(e)[:80]}"))
                continue
            for v in vols:
                if not (v.get("name") or "").startswith(EPH_VOLUME_PREFIX):
                    continue
                vid = v["id"]
                if vid in live_vids:
                    skipped.append((vid, "attached to live pod"))
                    continue
                if vid not in known_vids and not force:
                    skipped.append((vid, "unknown to registry (use --force)"))
                    continue
                try:
                    delete_network_volume(vid, account=acct)
                    deleted.append(vid)
                    self._log(vid, "volume-sweep", f"[{acct}] {v.get('name', '')}")
                except SystemExit as e:
                    skipped.append((vid, f"delete failed: {str(e)[:80]}"))
        return deleted, skipped

    # ---- reconcile ----
    def reconcile(self, terminate_untracked=False):
        """Diff registry vs live RunPod across ALL configured accounts. Each
        account is fetched with its own key; a failed fetch skips that account's
        vanish sweep entirely (an auth/network blip must not mark its pods
        terminated). Pods on an account with no key in custody are warned about
        — podtrack cannot see or reap them."""
        accounts = configured_accounts()
        known = {p["pod_id"]: p for p in self.list_pods("all")}
        keyless = sorted({self._acct(p) for p in known.values()
                          if p["status"] in LIVE_STATUSES and self._acct(p) not in accounts})
        if keyless:
            print(f"# WARN: registry has live pods on account(s) {keyless} with no key in "
                  f"custody — they CANNOT be reconciled or reaped. `podtrack adopt-key "
                  f"--account <name>` to restore visibility.", file=sys.stderr)
        remote_by_acct: dict = {}                # acct -> {pod_id: pod} | None (fetch failed)
        for acct in accounts:
            try:
                remote_by_acct[acct] = {p["pod_id"]: p for p in fetch_remote_pods(acct)}
            except SystemExit as e:
                print(f"# WARN: pod fetch failed for account '{acct}': {str(e)[:140]} — "
                      f"skipping its vanish sweep this cycle.", file=sys.stderr)
                remote_by_acct[acct] = None
        seen_live = {pid for r in remote_by_acct.values() if r for pid in r}
        s = {"live": [], "untracked": [], "vanished": [], "idle_or_cpu": []}
        for acct, remote in remote_by_acct.items():
            if remote is None:
                continue
            for pid, rp in remote.items():
                running = rp["desired"] == "RUNNING"
                if running and (not rp["n_gpus"] or
                                (rp["gpu_util"] == 0 and (rp["uptime_s"] or 0) > 600)):
                    s["idle_or_cpu"].append(pid)
                if pid in known:
                    # account=? self-heals a row filed under the wrong account —
                    # the fetch that actually returned the pod is the truth.
                    self.db.execute(
                        "UPDATE pods SET status=?,gpu_type=COALESCE(?,gpu_type),cost_per_hr=?,"
                        "ssh_ip=COALESCE(?,ssh_ip),ssh_port=COALESCE(?,ssh_port),"
                        "last_gpu_util=?,gpu_verified_at=?,vanish_strikes=0,account=? "
                        "WHERE pod_id=?",
                        ("running" if running else "exited", rp["gpu_type"], rp["cost_per_hr"],
                         rp["ssh_ip"], rp["ssh_port"], rp["gpu_util"],
                         _ts() if rp["n_gpus"] else None, acct, pid))
                    s["live"].append(pid)
                else:
                    self.db.execute(
                        "INSERT OR IGNORE INTO pods(pod_id,owner_uuid,owner_label,gpu_type,"
                        "ssh_ip,ssh_port,status,cost_per_hr,deployed_at,notes,account) "
                        "VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                        (pid, "UNKNOWN", "UNTRACKED", rp["gpu_type"], rp["ssh_ip"],
                         rp["ssh_port"], "untracked", rp["cost_per_hr"], _ts(),
                         "discovered by reconcile", acct))
                    s["untracked"].append(pid)
                    self._log(pid, "reconcile", f"found untracked/leaked pod [{acct}]")
            # #11 vanished-guard, PER ACCOUNT: a truncated/partial fetch must not mark
            # live pods gone (that hides a still-billing pod as an un-reapable
            # "terminated" leak). 0 pods returned while the registry believes this
            # account has live ones -> almost certainly a failed/truncated fetch.
            acct_known = {pid: kp for pid, kp in known.items() if self._acct(kp) == acct}
            acct_live = [k for k in acct_known.values() if k["status"] in LIVE_STATUSES]
            if not remote and acct_live:
                print(f"# WARN: account '{acct}' returned 0 pods but registry has "
                      f"{len(acct_live)} live there; skipping its vanish sweep this cycle "
                      f"(likely a partial/failed fetch).", file=sys.stderr)
                continue
            for pid, kp in acct_known.items():
                # seen_live (any account) covers rows whose account column was stale.
                if pid in seen_live or kp["status"] not in LIVE_STATUSES:
                    continue
                vs = (kp["vanish_strikes"] or 0) + 1
                if vs >= 2:                     # confirmed absent across 2 consecutive fetches
                    self.db.execute("UPDATE pods SET status='terminated',terminated_at=?,"
                                    "vanish_strikes=0 WHERE pod_id=?", (_ts(), pid))
                    s["vanished"].append(pid)
                else:
                    self.db.execute("UPDATE pods SET vanish_strikes=? WHERE pod_id=?", (vs, pid))
                    self._log(pid, "vanish-strike", f"{vs}/2 (absent this fetch)")
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

    def _read_remote_heartbeat(self, pod) -> bool:
        """Best-effort: pull the job's heartbeat file (ISO-UTC ts) from the pod via SSH.
        Jobs keep it fresh with e.g. `while :; do date -u +%FT%TZ > JOB_HB_FILE; sleep 60; done`.
        Returns whether the pod was SSH-REACHABLE (rc 255 == transport failure), so the
        reaper can track an unreachable streak (#8) independent of whether a hb file exists."""
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
        return rc != 255                        # reachable if SSH transport worked at all

    # ---- autonomous reaper (run by the systemd timer) ----
    def reap(self, mirror=True, pet_min=30, startup_grace_min=15,
             idle_strikes_needed=3, hb_grace_min=20,
             unreachable_reaps_needed=3, untracked_grace_min=30):
        """Autonomous: reconcile -> mirror -> pet healthy -> safe-teardown TTL/idle pods.

        v0.2.1 safety (a momentary 0%-GPU snapshot no longer kills an active pod):
          - startup grace: never idle-kill a pod younger than `startup_grace_min`.
          - SUSTAINED idle: idle-kill needs `idle_strikes_needed` CONSECUTIVE idle
            reconciles (busy GPU resets the counter), not a single snapshot.
        v0.3 (Phase 1):
          - #7 heartbeat-PRIMARY: a fresh job heartbeat (< `hb_grace_min`) means
            "working" — it resets idle strikes and blocks idle-kill outright.
          - #6 TTL grace: a soft `kill_after` that hits while a heartbeat is fresh is
            EXTENDED once (pet) with a warning, not insta-killed. Only `kill_after_
            absolute` (or a stale heartbeat) forces the kill.
          - #8 unreachable-escalation: an idle pod SSH-unreachable for
            `unreachable_reaps_needed` reaps past startup grace is force-terminated
            (a leak that can never otherwise be reaped; mirror already ran while
            reachable and the ephemeral volume shrinks the blast radius).
          - #12 untracked auto-reap: pods on the account but in no tracked session are
            terminated once older than `untracked_grace_min`.
          - #14 SSH failures are first-class (retry+WARN); a busy pod we can't pet is
            surfaced loudly rather than left to drift into a dead-man kill."""
        recon = self.reconcile()
        report = {"reaped": [], "petted": [], "rearmed": [], "mirrored": [], "kept": [], **recon}
        now = _now()

        # #12: auto-terminate untracked pods (owner UNKNOWN) older than the grace.
        for pod in self.list_pods("all"):
            if pod["owner_uuid"] != "UNKNOWN" or pod["status"] not in LIVE_STATUSES + ("untracked",):
                continue
            pid = pod["pod_id"]
            dep = _parse(pod["deployed_at"])
            if dep is None or (now - dep) < timedelta(minutes=untracked_grace_min):
                report["kept"].append((pid, "untracked (within grace)"))
                continue
            try:
                self.teardown(pid, force=True, skip_pull=True)   # no artifact spec to pull
                report["reaped"].append((pid, "untracked"))
            except SystemExit as e:
                report["kept"].append((pid, f"untracked-refused: {str(e)[:50]}"))

        for pod in self.list_pods("all"):
            if pod["status"] != "running" or pod["owner_uuid"] == "UNKNOWN":
                continue                          # untracked handled above
            pid = pod["pod_id"]
            if mirror and pod["remote_path"]:
                ok, _ = self.sync(pid)
                if ok:
                    report["mirrored"].append(pid)
            reachable = self._read_remote_heartbeat(pod)
            sfs = 0 if reachable else (pod["ssh_fail_streak"] or 0) + 1
            self.db.execute("UPDATE pods SET ssh_fail_streak=? WHERE pod_id=?", (sfs, pid))
            self.db.commit()
            pod = self.get(pid)                                  # reload after hb/streak updates

            gpu_idle_now = pid in recon["idle_or_cpu"]
            dep = _parse(pod["deployed_at"])
            young = dep is not None and (now - dep) < timedelta(minutes=startup_grace_min)
            hb = _parse(pod["last_job_heartbeat"])
            fresh_hb = hb is not None and (now - hb) < timedelta(minutes=hb_grace_min)
            # #7 heartbeat-primary: a working job zeroes idle strikes regardless of a
            # transient GPU-util dip; only a pod WITHOUT a fresh heartbeat accrues them.
            strikes = 0 if fresh_hb else ((pod["idle_strikes"] or 0) + 1 if gpu_idle_now else 0)
            self.db.execute("UPDATE pods SET idle_strikes=? WHERE pod_id=?", (strikes, pid))
            self.db.commit()

            # #8 unreachable-escalation: idle + SSH-dead for N reaps past grace = leak.
            if sfs >= unreachable_reaps_needed and gpu_idle_now and not young and not fresh_hb:
                print(f"# ALERT: {pid} idle AND SSH-unreachable for {sfs} reaps — "
                      f"force-terminating (un-reapable leak; last mirror was while reachable).",
                      file=sys.stderr)
                try:
                    self.teardown(pid, force=True, skip_pull=True)
                    report["reaped"].append((pid, f"unreachable x{sfs}+idle"))
                except SystemExit as e:
                    report["kept"].append((pid, f"unreachable-refused: {str(e)[:50]}"))
                continue

            abs_expired = pod["kill_after_absolute"] and now > _parse(pod["kill_after_absolute"])
            ttl_expired = pod["kill_after"] and now > _parse(pod["kill_after"])
            idle_kill = (strikes >= idle_strikes_needed) and not young

            # #6 TTL grace: soft TTL + fresh heartbeat => extend once, don't kill.
            if ttl_expired and not abs_expired and fresh_hb:
                if self.pet(pid, pet_min):
                    print(f"# WARN: {pid} hit soft TTL but a heartbeat is fresh; extended "
                          f"+{pet_min}m instead of killing. Use kill_after_absolute for a "
                          f"hard cap.", file=sys.stderr)
                    self._log(pid, "ttl-grace", f"soft TTL + fresh hb -> +{pet_min}m")
                    report["petted"].append(pid)
                else:                              # #14: busy but we can't pet — alarm, don't drift
                    print(f"# ALERT: {pid} busy (fresh hb) hit soft TTL but pet FAILED (ssh "
                          f"down). The on-pod dead-man may kill it at {pod['kill_after']}. "
                          f"Investigate.", file=sys.stderr)
                    report["kept"].append((pid, "ttl-grace pet FAILED (busy, unreachable)"))
                continue

            hard_kill = abs_expired or ttl_expired or idle_kill
            reason = ("abs-ttl" if abs_expired else "ttl" if ttl_expired else f"idle x{strikes}")
            if hard_kill:
                try:
                    self.teardown(pid, as_reaper=True)          # bypass ownership; keep pull-verify
                    report["reaped"].append((pid, reason))
                except SystemExit as e:
                    report["kept"].append((pid, f"refused: {str(e)[:60]}"))
            elif pod["deadman"]:
                # #1/#3: a watchdog is RAM-only — a pod restart/OOM wipes it, leaving
                # deadman=1 but no process. Verify each reap; re-arm to the remaining
                # TTL if it vanished, otherwise pet it forward.
                if self._deadman_alive(pod):
                    self.db.execute("UPDATE pods SET deadman_verified_at=? WHERE pod_id=?",
                                    (_ts(), pid))
                    self.db.commit()
                    if not self.pet(pid, pet_min) and fresh_hb:
                        print(f"# ALERT: {pid} busy but pet FAILED (ssh down); its dead-man "
                              f"deadline is no longer sliding. Investigate.", file=sys.stderr)
                    report["petted"].append(pid)
                else:
                    ka = _parse(pod["kill_after"])
                    remaining = max(1, int((ka - now).total_seconds() // 60)) if ka else pet_min
                    try:
                        self.arm(pid, remaining)          # re-plant + re-verify watchdog
                        report["rearmed"].append(pid)
                    except SystemExit as e:
                        report["kept"].append((pid, f"rearm-failed: {str(e)[:60]}"))
            else:
                report["kept"].append(
                    (pid, f"strikes={strikes} young={young} fresh_hb={bool(fresh_hb)} ssh_fail={sfs}"))
        return report


# ------------------------------------------------------------------------- CLI
def _print_pods(pods):
    if not pods:
        print("  (none)"); return
    print(f"  {'pod_id':<16}{'acct':<8}{'status':<12}{'gpu':<20}{'owner':<16}{'util':<6}"
          f"{'kill_after':<22}ssh")
    for p in pods:
        ssh = f"{p['ssh_ip']}:{p['ssh_port']}" if p["ssh_ip"] else "-"
        print(f"  {p['pod_id']:<16}{(p.get('account') or 'main'):<8}"
              f"{(p['status'] or '?'):<12}{(p['gpu_type'] or '?'):<20}"
              f"{(p['owner_label'] or '?'):<16}{str(p['last_gpu_util'] if p['last_gpu_util'] is not None else '-'):<6}"
              f"{(p['kill_after'] or '-'):<22}{ssh}")


def main():
    ap = argparse.ArgumentParser(prog="podtrack", description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("whoami"); sub.add_parser("accounts")
    ak = sub.add_parser("adopt-key")
    ak.add_argument("--account", default="main", help="credential slot name (default: main)")
    ak.add_argument("--from", dest="source",
                    help="key file to adopt (defaults: ~/.keys/runpod for main, "
                         "~/.keys/runpod-<account> otherwise)")
    r = sub.add_parser("register"); r.add_argument("pod_id")
    for f in ("--label", "--gpu", "--ssh-ip", "--remote-path", "--local-path", "--ssh-key"):
        r.add_argument(f)
    r.add_argument("--account", help="RunPod account the pod lives on "
                                     "(env PODTRACK_ACCOUNT; default: main)")
    r.add_argument("--ssh-port", type=int); r.add_argument("--cost", type=float)
    r.add_argument("--volume-id", help="ephemeral network volume attached to this pod (P-2)")
    r.add_argument("--no-artifacts", action="store_true",
                   help="declare this pod produces no artifacts (silences the #9 teardown warn)")
    jh = sub.add_parser("job-heartbeat"); jh.add_argument("pod_id")
    li = sub.add_parser("list"); gg = li.add_mutually_exclusive_group()
    for fl in ("all", "mine", "others"):
        gg.add_argument(f"--{fl}", action="store_const", const=fl, dest="scope")
    c = sub.add_parser("claim"); c.add_argument("pod_id"); c.add_argument("--label")
    pr = sub.add_parser("probe"); pr.add_argument("pod_id")
    sy = sub.add_parser("sync"); sy.add_argument("pod_id")
    hb = sub.add_parser("heartbeat"); hb.add_argument("pod_id")
    am = sub.add_parser("arm"); am.add_argument("pod_id")
    am.add_argument("--kill-in", type=int, required=True, help="minutes until self-destruct (soft, pettable)")
    am.add_argument("--kill-absolute-min", type=int,
                    help="minutes until a HARD cap the reaper honors even while heartbeating (#6)")
    am.add_argument("--token-file", help="restricted RunPod token for the on-pod switch")
    pt = sub.add_parser("pet"); pt.add_argument("pod_id"); pt.add_argument("--min", type=int, default=30)
    td = sub.add_parser("teardown"); td.add_argument("pod_id")
    td.add_argument("--force", action="store_true"); td.add_argument("--skip-pull", action="store_true")
    rc = sub.add_parser("reconcile"); rc.add_argument("--terminate-untracked", action="store_true")
    sv = sub.add_parser("sweep-volumes")
    sv.add_argument("--force", action="store_true",
                    help="also delete pt-eph-* volumes unknown to the registry")
    rp = sub.add_parser("reap"); rp.add_argument("--no-mirror", action="store_true")
    rp.add_argument("--startup-grace", type=int, help="min; env PODTRACK_STARTUP_GRACE_MIN (15)")
    rp.add_argument("--idle-strikes", type=int, help="consecutive idle reconciles; env PODTRACK_IDLE_STRIKES (3)")
    rp.add_argument("--hb-grace", type=int, help="min; env PODTRACK_HB_GRACE_MIN (20)")
    rp.add_argument("--pet-min", type=int, help="min; env PODTRACK_PET_MIN (30)")
    rp.add_argument("--unreachable-reaps", type=int,
                    help="idle+SSH-dead reaps before force-kill; env PODTRACK_UNREACHABLE_REAPS (3)")
    rp.add_argument("--untracked-grace", type=int,
                    help="min before an untracked pod is auto-reaped; env PODTRACK_UNTRACKED_GRACE_MIN (30)")
    a = ap.parse_args()

    if a.cmd == "adopt-key":
        print(adopt_key(a.account, a.source)); return
    if a.cmd == "accounts":
        accts = configured_accounts()
        if not accts:
            print("(no accounts configured; run `podtrack adopt-key`)")
        for acct in accts:
            print(f"{acct:<10} {_cred_path(acct)}")
        return
    if a.cmd == "whoami":
        u, l = whoami(); print(f"owner_uuid: {u}\nowner_label: {l}"); return
    reg = Registry(owner_label=getattr(a, "label", None))
    if a.cmd == "register":
        p = reg.register(a.pod_id, gpu_type=a.gpu, ssh_ip=a.ssh_ip, ssh_port=a.ssh_port,
                         cost_per_hr=a.cost, remote_path=a.remote_path, local_path=a.local_path,
                         ssh_key=a.ssh_key, volume_id=a.volume_id, no_artifacts=a.no_artifacts,
                         account=a.account)
        print(f"registered {p['pod_id']} -> '{p['owner_label']}' [{p['account'] or 'main'}]")
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
        d = reg.arm(a.pod_id, a.kill_in, token_file=a.token_file,
                    kill_absolute_min=a.kill_absolute_min)
        print(f"armed {a.pod_id}: dead-man fires {d}")
    elif a.cmd == "pet":
        print("petted" if reg.pet(a.pod_id, a.min) else "pet failed")
    elif a.cmd == "teardown":
        reg.teardown(a.pod_id, force=a.force, skip_pull=a.skip_pull); print(f"torn down {a.pod_id}")
    elif a.cmd == "reconcile":
        s = reg.reconcile(terminate_untracked=a.terminate_untracked)
        print(f"live={len(s['live'])} untracked={s['untracked']} vanished={s['vanished']} "
              f"idle_or_cpu={s['idle_or_cpu']}")
    elif a.cmd == "sweep-volumes":
        deleted, skipped = reg.sweep_volumes(force=a.force)
        print(f"deleted={deleted} skipped={skipped}")
    elif a.cmd == "reap":
        rep = reg.reap(
            mirror=not a.no_mirror,
            startup_grace_min=_cfg(a.startup_grace, "PODTRACK_STARTUP_GRACE_MIN", 15),
            idle_strikes_needed=_cfg(a.idle_strikes, "PODTRACK_IDLE_STRIKES", 3),
            hb_grace_min=_cfg(a.hb_grace, "PODTRACK_HB_GRACE_MIN", 20),
            pet_min=_cfg(a.pet_min, "PODTRACK_PET_MIN", 30),
            unreachable_reaps_needed=_cfg(a.unreachable_reaps, "PODTRACK_UNREACHABLE_REAPS", 3),
            untracked_grace_min=_cfg(a.untracked_grace, "PODTRACK_UNTRACKED_GRACE_MIN", 30))
        print(f"reaped={rep['reaped']} petted={rep['petted']} rearmed={rep['rearmed']} "
              f"mirrored={len(rep['mirrored'])} untracked={rep['untracked']} "
              f"kept={len(rep['kept'])}")


if __name__ == "__main__":
    main()
