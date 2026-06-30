#!/usr/bin/env python3
"""podtrack — a shared, ownership-aware registry for RunPod pods.

Why this exists
---------------
Multiple agent streams (Claude Code sessions) share one RunPod account. With a
single mutable `pod_state.json` and no ownership, streams clobber each other:
they lose track of pods (one deploy overwrites another's state -> orphaned pod
billing), tear down *other* streams' live pods, and leave CPU/idle pods running
for hours. This module replaces that file with a concurrency-safe SQLite
registry that records WHO owns each pod, and refuses cross-owner teardowns.

Three jobs (v1):
  1. Registry      — one row per pod, shared across streams (SQLite, WAL mode).
  2. Ownership     — every pod is owned by (session-UUID, friendly-label);
                     teardown is owner-guarded.
  3. Reconcile     — diff the registry against the live RunPod account: catch
                     leaked/untracked pods, mark vanished ones terminated, and
                     flag GPU-vs-CPU / idle pods.

Credential mandate
------------------
podtrack is the *custodian* of the RunPod API key. `adopt-key` moves
~/.keys/runpod into podtrack's private store; thereafter the only sanctioned way
to reach RunPod is through this module, so a pod cannot be created or destroyed
without being registered. Direct readers of the old path get a notice telling
them to use podtrack.

CLI:
    podtrack whoami
    podtrack adopt-key
    podtrack register <pod_id> --label melt-run [--gpu H100 --ssh-ip .. --ssh-port ..]
    podtrack list [--all | --mine | --others]
    podtrack heartbeat <pod_id>
    podtrack teardown <pod_id> [--force]
    podtrack reconcile [--terminate-untracked]

Library:
    from podtrack import Registry, whoami
    reg = Registry()
    reg.register(pod_id, label="melt-run", gpu_type="H100")
    reg.reconcile()
"""
from __future__ import annotations

import argparse
import json
import os
import socket
import sqlite3
import sys
import urllib.error
import urllib.request
import uuid as _uuid
from datetime import datetime, timezone
from pathlib import Path

# ---------------------------------------------------------------- paths / config
DATA_DIR = Path(os.environ.get("PODTRACK_HOME", Path.home() / ".local/share/podtrack"))
DB_PATH = DATA_DIR / "pods.db"
CRED_DIR = Path.home() / ".config/podtrack"
CRED_PATH = CRED_DIR / "runpod.key"
LEGACY_KEY = Path.home() / ".keys/runpod"
RUNPOD_API = "https://api.runpod.io/graphql"

LIVE_STATUSES = ("provisioning", "running", "exited")  # not yet torn down


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# ----------------------------------------------------------------------- identity
def whoami(uuid: str | None = None, label: str | None = None) -> tuple[str, str]:
    """Resolve (owner_uuid, owner_label) for this agent stream.

    UUID precedence:   arg -> $PODTRACK_OWNER_UUID -> $CLAUDE_SESSION_ID
                       -> persisted per-install id (last resort, warns).
    Label precedence:  arg -> $PODTRACK_LABEL -> '(unlabeled)'.
    """
    u = uuid or os.environ.get("PODTRACK_OWNER_UUID") or os.environ.get("CLAUDE_SESSION_ID")
    if not u:
        marker = DATA_DIR / "owner_id"
        if marker.exists():
            u = marker.read_text().strip()
        else:
            u = f"host-{socket.gethostname()}-{_uuid.uuid4().hex[:8]}"
            DATA_DIR.mkdir(parents=True, exist_ok=True)
            marker.write_text(u)
        print(f"# podtrack: no session UUID in env; using fallback owner '{u}'.\n"
              f"#   set PODTRACK_OWNER_UUID + PODTRACK_LABEL to distinguish streams.",
              file=sys.stderr)
    label = label or os.environ.get("PODTRACK_LABEL") or "(unlabeled)"
    return u, label


# --------------------------------------------------------------- key custodian
def adopt_key() -> str:
    """Move the RunPod key into podtrack's private store and leave a notice.
    Idempotent: if already adopted, just reports."""
    CRED_DIR.mkdir(parents=True, exist_ok=True)
    if CRED_PATH.exists():
        return f"already adopted: {CRED_PATH}"
    if not LEGACY_KEY.exists():
        raise SystemExit(f"no key at {LEGACY_KEY} to adopt (and none at {CRED_PATH})")
    key = LEGACY_KEY.read_text()
    CRED_PATH.write_text(key)
    CRED_PATH.chmod(0o600)
    # Replace the legacy path with a 0600 notice so direct readers fail loudly.
    LEGACY_KEY.write_text(
        "# MOVED. The RunPod API key is now managed by podtrack.\n"
        "# Do NOT read this file or call the RunPod API directly.\n"
        "# Use: podtrack <cmd>   (library: from podtrack import Registry)\n"
        f"# Key custodian: {CRED_PATH}\n")
    LEGACY_KEY.chmod(0o600)
    return f"adopted: {LEGACY_KEY} -> {CRED_PATH} (legacy path now a notice)"


def runpod_key() -> str:
    """The ONLY sanctioned way to obtain the RunPod key."""
    if CRED_PATH.exists():
        return CRED_PATH.read_text().strip()
    if LEGACY_KEY.exists() and "MOVED" not in LEGACY_KEY.read_text(errors="replace"):
        raise SystemExit(
            f"RunPod key still at {LEGACY_KEY}, not yet under podtrack.\n"
            f"Run `podtrack adopt-key` so all RunPod access is tracked.")
    raise SystemExit(f"no RunPod key found at {CRED_PATH}; run `podtrack adopt-key`.")


# ------------------------------------------------------------------ RunPod API
def gql(query: str, variables: dict | None = None) -> dict:
    """RunPod GraphQL call. Cloudflare 403s the default urllib UA — spoof it."""
    body = json.dumps({"query": query, "variables": variables or {}}).encode()
    req = urllib.request.Request(
        f"{RUNPOD_API}?api_key={runpod_key()}", data=body,
        headers={"Content-Type": "application/json",
                 "User-Agent": "Mozilla/5.0 (podtrack)"})
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            out = json.loads(r.read().decode())
    except urllib.error.HTTPError as e:
        raise SystemExit(f"RunPod HTTP {e.code}: {e.read().decode(errors='replace')[:300]}")
    if out.get("errors"):
        raise SystemExit(f"RunPod GraphQL error: {out['errors']}")
    return out.get("data", {})


def fetch_remote_pods() -> list[dict]:
    """Live pods on the RunPod account, normalized."""
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
        ssh_ip, ssh_port = None, None
        for port in (rt.get("ports") or []):
            if port.get("privatePort") == 22:
                ssh_ip, ssh_port = port.get("ip"), port.get("publicPort")
        norm.append({
            "pod_id": p.get("id"), "name": p.get("name"),
            "desired": p.get("desiredStatus"), "cost_per_hr": p.get("costPerHr"),
            "gpu_type": (p.get("machine") or {}).get("gpuDisplayName"),
            "n_gpus": len(gpus), "uptime_s": rt.get("uptimeInSeconds"),
            "gpu_util": max((g.get("gpuUtilPercent") or 0) for g in gpus) if gpus else None,
            "ssh_ip": ssh_ip, "ssh_port": ssh_port, "has_runtime": bool(rt),
        })
    return norm


# -------------------------------------------------------------------- registry
SCHEMA = """
CREATE TABLE IF NOT EXISTS pods (
    pod_id          TEXT PRIMARY KEY,
    owner_uuid      TEXT,
    owner_label     TEXT,
    gpu_type        TEXT,
    ssh_ip          TEXT,
    ssh_port        INTEGER,
    status          TEXT,           -- provisioning|running|exited|terminated|untracked
    cost_per_hr     REAL,
    gpu_verified_at TEXT,
    deployed_at     TEXT,
    last_heartbeat  TEXT,
    terminated_at   TEXT,
    notes           TEXT
);
CREATE TABLE IF NOT EXISTS events (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    ts          TEXT,
    pod_id      TEXT,
    actor_uuid  TEXT,
    actor_label TEXT,
    action      TEXT,
    detail      TEXT
);
"""


class Registry:
    def __init__(self, owner_uuid: str | None = None, owner_label: str | None = None):
        self.uuid, self.label = whoami(owner_uuid, owner_label)
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(DB_PATH, timeout=30)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")      # safe concurrent readers/writers
        self.db.execute("PRAGMA busy_timeout=30000")
        self.db.executescript(SCHEMA)
        self.db.commit()

    def _log(self, pod_id, action, detail=""):
        self.db.execute(
            "INSERT INTO events(ts,pod_id,actor_uuid,actor_label,action,detail) "
            "VALUES (?,?,?,?,?,?)",
            (_now(), pod_id, self.uuid, self.label, action, detail))
        self.db.commit()

    # ---- ownership-aware operations ----
    def register(self, pod_id, gpu_type=None, ssh_ip=None, ssh_port=None,
                 cost_per_hr=None, status="running", notes=None):
        self.db.execute(
            "INSERT INTO pods(pod_id,owner_uuid,owner_label,gpu_type,ssh_ip,ssh_port,"
            "status,cost_per_hr,deployed_at,last_heartbeat,notes) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?) "
            "ON CONFLICT(pod_id) DO UPDATE SET owner_uuid=excluded.owner_uuid,"
            "owner_label=excluded.owner_label,gpu_type=excluded.gpu_type,"
            "ssh_ip=excluded.ssh_ip,ssh_port=excluded.ssh_port,status=excluded.status,"
            "cost_per_hr=excluded.cost_per_hr,last_heartbeat=excluded.last_heartbeat",
            (pod_id, self.uuid, self.label, gpu_type, ssh_ip, ssh_port, status,
             cost_per_hr, _now(), _now(), notes))
        self.db.commit()
        self._log(pod_id, "register", f"{self.label} gpu={gpu_type}")
        return self.get(pod_id)

    def get(self, pod_id):
        r = self.db.execute("SELECT * FROM pods WHERE pod_id=?", (pod_id,)).fetchone()
        return dict(r) if r else None

    def list_pods(self, scope="all"):
        rows = self.db.execute("SELECT * FROM pods ORDER BY deployed_at").fetchall()
        out = [dict(r) for r in rows]
        if scope == "mine":
            out = [p for p in out if p["owner_uuid"] == self.uuid]
        elif scope == "others":
            out = [p for p in out if p["owner_uuid"] != self.uuid]
        return out

    def heartbeat(self, pod_id):
        self.db.execute("UPDATE pods SET last_heartbeat=? WHERE pod_id=?", (_now(), pod_id))
        self.db.commit()
        self._log(pod_id, "heartbeat")

    def teardown(self, pod_id, force=False, _remote=True):
        """Owner-guarded teardown. Refuses to kill another stream's pod unless --force."""
        pod = self.get(pod_id)
        if not pod:
            raise SystemExit(f"{pod_id} not in registry; run `reconcile` first")
        if pod["owner_uuid"] != self.uuid and not force:
            raise SystemExit(
                f"REFUSED: {pod_id} is owned by '{pod['owner_label']}' "
                f"({pod['owner_uuid']}), not you ('{self.label}'). "
                f"Use --force only if you are certain it is safe.")
        if _remote:
            gql("mutation($i:PodTerminateInput!){podTerminate(input:$i)}",
                {"i": {"podId": pod_id}})
        self.db.execute("UPDATE pods SET status='terminated', terminated_at=? WHERE pod_id=?",
                        (_now(), pod_id))
        self.db.commit()
        self._log(pod_id, "force-teardown" if force else "teardown",
                  f"was {pod['owner_label']}")
        return True

    def reconcile(self, terminate_untracked=False):
        """Diff registry vs live RunPod account. Returns a summary."""
        remote = {p["pod_id"]: p for p in fetch_remote_pods()}
        known = {p["pod_id"]: p for p in self.list_pods("all")}
        summary = {"live": [], "untracked": [], "vanished": [], "idle_or_cpu": []}

        for pid, rp in remote.items():
            running = rp["desired"] == "RUNNING"
            # GPU/idle health flag
            if running and (not rp["n_gpus"] or (rp["gpu_util"] == 0 and rp["uptime_s"]
                                                 and rp["uptime_s"] > 600)):
                summary["idle_or_cpu"].append(pid)
            if pid in known:
                self.db.execute(
                    "UPDATE pods SET status=?,gpu_type=COALESCE(?,gpu_type),"
                    "cost_per_hr=?,ssh_ip=COALESCE(?,ssh_ip),ssh_port=COALESCE(?,ssh_port),"
                    "gpu_verified_at=? WHERE pod_id=?",
                    ("running" if running else "exited", rp["gpu_type"], rp["cost_per_hr"],
                     rp["ssh_ip"], rp["ssh_port"],
                     _now() if rp["n_gpus"] else None, pid))
                summary["live"].append(pid)
            else:
                # On RunPod but nobody registered it -> a leak.
                self.db.execute(
                    "INSERT OR IGNORE INTO pods(pod_id,owner_uuid,owner_label,gpu_type,"
                    "ssh_ip,ssh_port,status,cost_per_hr,deployed_at,notes) "
                    "VALUES (?,?,?,?,?,?,?,?,?,?)",
                    (pid, "UNKNOWN", "UNTRACKED", rp["gpu_type"], rp["ssh_ip"],
                     rp["ssh_port"], "untracked", rp["cost_per_hr"], _now(),
                     "discovered by reconcile — owner unknown"))
                summary["untracked"].append(pid)
                self._log(pid, "reconcile", "found untracked/leaked pod")

        # Registry rows that are gone from RunPod -> mark terminated.
        for pid, kp in known.items():
            if pid not in remote and kp["status"] in LIVE_STATUSES:
                self.db.execute(
                    "UPDATE pods SET status='terminated', terminated_at=? WHERE pod_id=?",
                    (_now(), pid))
                summary["vanished"].append(pid)
        self.db.commit()

        if terminate_untracked:
            for pid in summary["untracked"]:
                self.teardown(pid, force=True)
        return summary


# ------------------------------------------------------------------------- CLI
def _print_pods(pods):
    if not pods:
        print("  (none)"); return
    print(f"  {'pod_id':<16} {'status':<12} {'gpu':<22} {'owner':<18} ssh")
    for p in pods:
        ssh = f"{p['ssh_ip']}:{p['ssh_port']}" if p["ssh_ip"] else "-"
        print(f"  {p['pod_id']:<16} {(p['status'] or '?'):<12} "
              f"{(p['gpu_type'] or '?'):<22} {(p['owner_label'] or '?'):<18} {ssh}")


def main():
    ap = argparse.ArgumentParser(prog="podtrack", description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("whoami")
    sub.add_parser("adopt-key")
    r = sub.add_parser("register"); r.add_argument("pod_id")
    r.add_argument("--label"); r.add_argument("--gpu"); r.add_argument("--ssh-ip")
    r.add_argument("--ssh-port", type=int); r.add_argument("--cost", type=float)
    li = sub.add_parser("list")
    g = li.add_mutually_exclusive_group()
    g.add_argument("--all", action="store_const", const="all", dest="scope")
    g.add_argument("--mine", action="store_const", const="mine", dest="scope")
    g.add_argument("--others", action="store_const", const="others", dest="scope")
    hb = sub.add_parser("heartbeat"); hb.add_argument("pod_id")
    td = sub.add_parser("teardown"); td.add_argument("pod_id")
    td.add_argument("--force", action="store_true")
    rc = sub.add_parser("reconcile")
    rc.add_argument("--terminate-untracked", action="store_true")
    a = ap.parse_args()

    if a.cmd == "adopt-key":
        print(adopt_key()); return
    if a.cmd == "whoami":
        u, l = whoami(); print(f"owner_uuid: {u}\nowner_label: {l}"); return

    reg = Registry(owner_label=getattr(a, "label", None))
    if a.cmd == "register":
        p = reg.register(a.pod_id, gpu_type=a.gpu, ssh_ip=a.ssh_ip,
                         ssh_port=a.ssh_port, cost_per_hr=a.cost)
        print(f"registered {p['pod_id']} -> owner '{p['owner_label']}'")
    elif a.cmd == "list":
        _print_pods(reg.list_pods(a.scope or "all"))
    elif a.cmd == "heartbeat":
        reg.heartbeat(a.pod_id); print(f"heartbeat {a.pod_id}")
    elif a.cmd == "teardown":
        reg.teardown(a.pod_id, force=a.force); print(f"torn down {a.pod_id}")
    elif a.cmd == "reconcile":
        s = reg.reconcile(terminate_untracked=a.terminate_untracked)
        print(f"live={len(s['live'])}  untracked={s['untracked']}  "
              f"vanished={s['vanished']}  idle_or_cpu={s['idle_or_cpu']}")
        if s["untracked"]:
            print("  ⚠ untracked pods are billing with no owner — investigate or "
                  "`reconcile --terminate-untracked`")


if __name__ == "__main__":
    main()
