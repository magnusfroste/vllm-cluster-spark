"""vLLM Panel (vllmapp) — set up, run and monitor vLLM on one DGX Spark or a cluster of them.

Configuration comes from the environment (Easypanel's env tab, or ~/vllmapp/.env when
install.sh runs the app). The nodes are reached
over SSH with a dedicated key that may only run vllmapp-agent (see agent/).
"""
import base64
import difflib
import hashlib
import hmac
import json
import os
import re
import secrets
import shlex
import socket
import sqlite3
import subprocess
import threading
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, PlainTextResponse, RedirectResponse


def env(name, default=""):
    return os.environ.get(name, default).strip()


def envbool(name, default=False):
    return env(name, "true" if default else "false").lower() in ("1", "true", "yes", "ja")


APP_VERSION = "1.21.0"  # bump on every release that changes the app; shown in the menu

GITHUB_REPO = os.environ.get("GITHUB_REPO", "magnusfroste/vllm-cluster-spark").strip()  # owner/name, for links and the update check
UPDATE_HINT = os.environ.get("UPDATE_HINT", "").strip()  # how to update this install; install.sh sets it
_m = re.match(r"bash (\S+)/install\.sh", UPDATE_HINT)
# Where env values such as HF_TOKEN are changed, in words for the page.
ENV_WHERE = (f"{_m.group(1)}/.env, then run: bash {_m.group(1)}/install.sh" if _m
             else "the app's Environment tab in Easypanel, then Deploy")

# ---------- configuration ----------
# The nodes from env are the starting point; a list set on the Nodes page replaces them (apply_nodes).
ENV_HEAD = env("HEAD_HOST")
ENV_WORKERS = [h.strip() for h in env("WORKER_HOSTS").split(",") if h.strip()]
HEAD_HOST, WORKER_HOSTS = ENV_HEAD, list(ENV_WORKERS)
SSH_USER = env("SSH_USER", "root")
SSH_PORT = env("SSH_PORT", "22")
CLUSTER_DIR = env("CLUSTER_DIR", "~/vllm-cluster")
CONFIG_MODE = env("CONFIG_MODE", "managed")  # managed | existing
VLLM_PORT = env("VLLM_PORT", "8000")

TP_SIZE = env("TP_SIZE")
API_KEY = env("API_KEY")
HF_TOKEN = env("HF_TOKEN")  # set in env it wins; otherwise the token saved under Settings is used
HF_CACHE_DIR = env("HF_CACHE_DIR", "${HOME}/.cache/huggingface")
EXTRA_MOUNTS = [m.strip() for m in env("EXTRA_MOUNTS").split(",") if m.strip()]
IF_NAMES = env("IF_NAMES")  # optional override, ;-separated in node order
IB_HCAS = env("IB_HCAS")
PATCHES = env("PATCHES", "auto")  # auto (by model) | none | name of a folder in patchsets/
HF_OFFLINE = envbool("HF_OFFLINE", True)  # vLLM never downloads; the app's download does

ADMIN_USER = env("ADMIN_USER", "admin")
ADMIN_PASSWORD = env("ADMIN_PASSWORD")
PORT = int(env("PORT", "8080"))
# Operational settings, changed in the app. An env value wins and shows as locked.
OPS = {  # key: (env, type, default, min, max)
    "auto_recover": ("AUTO_RECOVER", bool, False),
    "allow_reboot": ("ALLOW_REBOOT", bool, False),
    "auto_reboot": ("AUTO_REBOOT", bool, False),
    "stall_timeout_min": ("STALL_TIMEOUT_MIN", float, 5, 1, 60),  # startup with no new log line
    "hang_timeout_min": ("HANG_TIMEOUT_MIN", float, 40, 10, 240),  # startup not ready after
    "unhealthy_grace_min": ("UNHEALTHY_GRACE_MIN", float, 3, 1, 60),  # was ready, stopped answering
    "max_auto_restarts": ("MAX_AUTO_RESTARTS", int, 1, 0, 5),
    "mem_warn_gib": ("MEM_WARN_GIB", float, 4, 0, 64),
    "extra_watts": ("EXTRA_WATTS", float, 0, 0, 1000),  # per node, on top of the GPU's own reading
}
POLL_SECONDS = float(env("POLL_SECONDS", "15"))

DATA = env("DATA_DIR", "/data")
KEY = os.path.join(DATA, "id_ed25519")
STATE_FILE = os.path.join(DATA, "state.json")
SETTINGS_FILE = os.path.join(DATA, "settings.json")  # the model chosen in the app
STATS_DB = os.path.join(DATA, "stats.db")  # token and energy use per hour
EVENTS_FILE = os.path.join(DATA, "events.log")

NODES = []  # filled by apply_nodes(); changed in place, so every reader sees the current list

PATCHSET_DIR = os.path.join(os.path.dirname(__file__), "patchsets")


def load_patchsets():
    sets = {}
    for name in sorted(os.listdir(PATCHSET_DIR)) if os.path.isdir(PATCHSET_DIR) else []:
        try:
            sets[name] = json.load(open(os.path.join(PATCHSET_DIR, name, "set.json")))
        except (OSError, ValueError):
            pass
    return sets


PATCHSETS = load_patchsets()


def load_catalog():
    """Ready-made models in models/*.json, then the admin's own templates in /data/models/*.json
    (they survive updates of the panel), in their display order."""
    cat = []
    for d, own in ((os.path.join(os.path.dirname(__file__), "models"), False), (OWN_DIR, True)):
        for name in sorted(os.listdir(d)) if os.path.isdir(d) else []:
            try:
                m = json.load(open(os.path.join(d, name)))
            except (OSError, ValueError):
                continue
            if own and not str(m.get("id", "")).startswith("my-"):
                continue  # an own template can't replace a ready-made one
            cat.append({**m, "own": own})
    return {m["id"]: m for m in sorted(cat, key=lambda m: m.get("order", 99))}


OWN_DIR = os.path.join(env("DATA_DIR", "/data"), "models")
CATALOG = load_catalog()


def reload_catalog():
    CATALOG.clear()
    CATALOG.update(load_catalog())
# Model settings the app owns. If one is set in the env it wins, and the page shows it as locked.
MODEL_KEYS = {"model": "MODEL", "served_model_name": "SERVED_MODEL_NAME", "image": "VLLM_IMAGE",
              "gpu_mem_util": "GPU_MEM_UTIL", "max_model_len": "MAX_MODEL_LEN",
              "vllm_args": "VLLM_EXTRA_ARGS", "revision": "MODEL_REVISION"}


def load_settings():
    try:
        return json.load(open(SETTINGS_FILE))
    except (OSError, ValueError):
        return {}


def apply_nodes():
    """The nodes in effect: the list set on the Nodes page (settings.json), else env."""
    global HEAD_HOST, WORKER_HOSTS
    st = load_settings().get("nodes") or {}
    HEAD_HOST = st.get("head") or ENV_HEAD
    WORKER_HOSTS = list(st["workers"]) if "workers" in st else list(ENV_WORKERS)
    NODES[:] = ([{"role": "head", "host": HEAD_HOST}] if HEAD_HOST else []) + \
        [{"role": "worker", "host": h} for h in WORKER_HOSTS]


def save_settings(update):
    """Merge into settings.json; a None value removes the key."""
    st = load_settings()
    st.update(update)
    st = {k: v for k, v in st.items() if v is not None}
    tmp = SETTINGS_FILE + ".tmp"
    json.dump(st, open(tmp, "w"), indent=2)
    os.replace(tmp, SETTINGS_FILE)
    return st


apply_nodes()


def conv(t, v):
    return (str(v).lower() in ("1", "true", "yes", "on")) if t is bool else t(v)


def ops():
    """Operational settings in effect: env > app > default."""
    saved = load_settings().get("ops", {})
    o = {"locked": []}
    for k, (e, t, d, *_) in OPS.items():
        if env(e):
            o[k] = conv(t, env(e))
            o["locked"].append(k)
        elif k in saved:
            o[k] = conv(t, saved[k])
        else:
            o[k] = d
    return o


def cfg():
    """The model config in effect: env > what was chosen in the app > the catalog entry."""
    st = load_settings()
    env_model = env("MODEL")
    entry = (next((m for m in CATALOG.values() if m["model"] == env_model), None) if env_model
             else CATALOG.get(st.get("id")) or (st.get("custom") and {"id": "custom", **st["custom"]})
             or None)
    entry = entry or {}
    c = {"id": entry.get("id"), "entry": entry, "locked": []}
    for k, e in MODEL_KEYS.items():
        v = env(e)
        if v:
            c["locked"].append(k)
        elif k in ("gpu_mem_util", "max_model_len") and st.get(k) and st.get("id") == entry.get("id"):
            v = str(st[k])
        else:
            v = str(entry.get(k) or "")
        c[k] = v
    model = c["model"]
    c["patchset"] = (None if entry.get("patchset") == "none" else entry.get("patchset")
                     or next((n for n, ps in PATCHSETS.items() if model in ps.get("models", [])), None)
                     ) if PATCHES == "auto" else None if PATCHES in ("", "none") else PATCHES
    # How the nodes are joined: "ray" (the image has Ray) or "mp" (vLLM's own multi-node mode,
    # for images without Ray). And extra environment variables the model needs in the container.
    c["executor"] = entry.get("executor", "ray")
    c["env"] = {k: str(v) for k, v in (entry.get("env") or {}).items() if re.match(r"^[A-Z_][A-Z0-9_]*$", k)}
    if not c["revision"]:
        c["revision"] = (PATCHSETS.get(c["patchset"]) or {}).get("revision", "")
    if c["model"] and not c["image"]:  # a custom model runs on the image of the verified entries
        c["image"] = next((m["image"] for m in CATALOG.values() if m.get("status") == "verified"), "")
    return c

# ---------- credentials set in the app (Settings), kept in /data/credentials.json (0600) ----------
CREDS_FILE = os.path.join(DATA, "credentials.json")


def load_creds():
    try:
        return json.load(open(CREDS_FILE))
    except (OSError, ValueError):
        return {}


def save_creds(update):
    c = {**load_creds(), **update}
    c = {k: v for k, v in c.items() if v}
    tmp = CREDS_FILE + ".tmp"
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as f:
        json.dump(c, f)
    os.replace(tmp, CREDS_FILE)


def hf_token():
    return HF_TOKEN or load_creds().get("hf_token", "")


def hash_password(pw, salt=None, rounds=310000):
    salt = salt or secrets.token_hex(16)
    dk = hashlib.pbkdf2_hmac("sha256", pw.encode(), bytes.fromhex(salt), rounds).hex()
    return f"pbkdf2_sha256${rounds}${salt}${dk}"


def password_hash():
    """The admin password set under Settings. It replaces ADMIN_PASSWORD from env; deleting
    /data/credentials.json makes the env password work again (the way back from a lost one)."""
    return load_creds().get("admin_password", "")


def password_ok(pw):
    h = password_hash()
    if h:
        try:
            _, rounds, salt, dk = h.split("$")
            return secrets.compare_digest(hash_password(pw, salt, int(rounds)).split("$")[3], dk)
        except ValueError:
            return False
    return bool(ADMIN_PASSWORD) and secrets.compare_digest(pw.encode(), ADMIN_PASSWORD.encode())


def secret_values():
    return [s for s in (API_KEY, hf_token(), ADMIN_PASSWORD) if len(s) >= 6]


def redact(text):
    if not text:
        return text
    for s in secret_values():
        text = text.replace(s, "***")
    text = re.sub(r"('api_key': \[)'[^']*'", r"\1'***'", text)
    text = re.sub(r"(--api-key[ =])(?![\"']?\$)\S+", r"\1***", text)
    text = re.sub(r"(?m)^((?:VLLM_API_KEY|API_KEY|HF_TOKEN)=).+$", r"\1***", text)
    text = re.sub(r"hf_[A-Za-z0-9]{20,}", "hf_***", text)
    return text


def now():
    return time.time()


def dur(sec):
    sec = int(sec)
    return f"{sec // 3600} h {sec % 3600 // 60} min" if sec >= 3600 else \
        f"{sec // 60} min" if sec >= 60 else f"{sec} s"


def iso(ts):
    return datetime.fromtimestamp(ts, timezone.utc).isoformat()


def parse_ts(s):
    if not s or s.startswith("0001"):
        return None
    s = re.sub(r"\.(\d{6})\d*", r".\1", s.replace("Z", "+00:00"))
    try:
        return datetime.fromisoformat(s).timestamp()
    except ValueError:
        return None


# ---------- event log and state ----------
_lock = threading.Lock()


def event(msg, kind="info"):
    line = json.dumps({"ts": iso(now()), "kind": kind, "msg": redact(msg)}, ensure_ascii=False)
    with _lock:
        with open(EVENTS_FILE, "a") as f:
            f.write(line + "\n")
    print(line, flush=True)
    if kind == "error" or (kind == "warn" and "automatically" in msg):
        alert(msg, kind)


# ---------- alerts: a webhook (ntfy, Slack, Discord, or plain JSON) when the cluster needs a look ----------
_alerts = {}


def alert_payload(url, text, kind):
    """The request for this kind of webhook: ntfy takes plain text, Slack and Discord their JSON."""
    host = urllib.parse.urlparse(url).hostname or ""
    title = "vLLM Panel" + (f" · {HEAD_HOST}" if HEAD_HOST else "")
    if "ntfy" in host:
        return text.encode(), {"Title": title, "Priority": "high" if kind == "error" else "default",
                               "Tags": {"error": "rotating_light", "ok": "white_check_mark"}.get(kind, "warning")}
    if host.endswith("slack.com"):
        return json.dumps({"text": f"*{title}*: {text}"}).encode(), {"Content-Type": "application/json"}
    if "discord" in host:
        return json.dumps({"content": f"**{title}**: {text}"}).encode(), {"Content-Type": "application/json"}
    return json.dumps({"title": title, "text": text, "kind": kind, "ts": iso(now()),
                       "panel": load_settings().get("public_url") or None}).encode(), {"Content-Type": "application/json"}


def send_alert(url, text, kind):
    data, headers = alert_payload(url, text, kind)
    req = urllib.request.Request(url, data=data, headers={"User-Agent": f"vllm-panel/{APP_VERSION}", **headers})
    with urllib.request.urlopen(req, timeout=10) as r:
        return r.status


def alert(text, kind="warn"):
    """Send in the background; the same text at most once every 10 minutes."""
    url = load_settings().get("alert_url")
    if not url or now() - _alerts.get(text, 0) < 600:
        return
    _alerts[text] = now()

    def go():
        try:
            send_alert(url, redact(text), kind)
        except Exception as e:  # noqa: BLE001 — an alert must never break the panel
            print(json.dumps({"ts": iso(now()), "kind": "warn", "msg": f"alert not sent: {e}"}), flush=True)
    threading.Thread(target=go, daemon=True).start()


def events(n=100):
    try:
        with open(EVENTS_FILE) as f:
            lines = f.readlines()[-n:]
    except FileNotFoundError:
        return []
    return [json.loads(l) for l in reversed(lines) if l.strip()]


def load_state():
    try:
        return json.load(open(STATE_FILE))
    except (FileNotFoundError, ValueError):
        return {"attempts": 0, "last_action": 0, "last_reboot": 0}


def save_state(s):
    tmp = STATE_FILE + ".tmp"
    json.dump(s, open(tmp, "w"))
    os.replace(tmp, STATE_FILE)


# ---------- SSH ----------
def ensure_key():
    os.makedirs(DATA, exist_ok=True)
    if not os.path.exists(KEY):
        subprocess.run(["ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-C", "vllmapp",
                        "-f", KEY], check=True)
        event("created SSH key for vllmapp")


def pubkey():
    return open(KEY + ".pub").read().strip()


_masters = {}  # host -> lock: one SSH connection per node, shared by every command


def ssh_opts(host):
    return ["ssh", "-i", KEY, "-p", SSH_PORT,
            "-o", "BatchMode=yes", "-o", "ConnectTimeout=5",
            "-o", "StrictHostKeyChecking=accept-new",
            "-o", f"UserKnownHostsFile={DATA}/known_hosts",
            "-o", "ServerAliveInterval=15", "-o", "ServerAliveCountMax=3",
            "-o", f"ControlPath=/tmp/vllmapp-ssh-{host}-{SSH_PORT}"]


def ensure_master(host):
    """Start the shared connection in the background if it isn't up. It is started on its own,
    with stdio on /dev/null: a master forked from a command with captured output would hold the
    pipe open and make that command wait until the master exits. -N runs no command, so the
    node's forced command (the agent) only runs for real requests."""
    lock = _masters.setdefault(host, threading.Lock())
    with lock:
        base = ssh_opts(host) + [f"{SSH_USER}@{host}"]
        if subprocess.run(base[:1] + ["-O", "check"] + base[1:], stdin=subprocess.DEVNULL,
                          stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=10).returncode == 0:
            return
        try:
            subprocess.run(base[:1] + ["-M", "-N", "-f", "-o", "ControlPersist=600"] + base[1:],
                           stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                           stderr=subprocess.DEVNULL, timeout=15)
        except subprocess.TimeoutExpired:
            pass  # the command below then connects on its own


def ssh(host, cmd, timeout=60, input=None):
    """Run an agent command on the node. Returns (rc, parsed json | None, raw text)."""
    try:
        ensure_master(host)
    except (OSError, subprocess.TimeoutExpired):
        pass
    # ControlMaster=no: use the shared connection if it is up, otherwise connect directly
    argv = ssh_opts(host) + ["-o", "ControlMaster=no", f"{SSH_USER}@{host}", cmd]
    try:
        p = subprocess.run(argv, capture_output=True, text=True, timeout=timeout, input=input)
    except subprocess.TimeoutExpired:
        return 124, None, "timeout"
    raw = (p.stdout or "").strip()
    try:
        return p.returncode, json.loads(raw.splitlines()[-1]) if raw else None, p.stderr
    except ValueError:
        return p.returncode, None, raw + p.stderr


# ---------- status ----------
PHASES = [  # (pattern, phase) — the last match in the log wins
    (re.compile(r"Väntar på \d+ noder|Waiting for"), "waiting for nodes"),
    (re.compile(r"Alla noder uppe|All nodes up|Starting vLLM with"), "starting vLLM"),
    (re.compile(r"Loading safetensors checkpoint shards:\s+(\d+)%"), "loading weights"),
    (re.compile(r"Available KV cache memory"), "profiling KV cache"),
    (re.compile(r"Running FlashInfer autotune"), "autotune"),
    (re.compile(r"Capturing CUDA graph"), "capturing CUDA graphs"),
    (re.compile(r"Application startup complete"), "ready"),
]
ERRORS = re.compile(r"Traceback|RuntimeError|ValueError|Timed out|out of memory|No available memory"
                    r"|ActorDied|ActorHandleNotFound|RayWorkerError")
IGNORE_ERRORS = re.compile(r"RuntimeError: cancelled")

STATUS = {"updated": 0, "nodes": [], "cluster": {}}
UNHEALTHY_SINCE = None
_action_lock = threading.Lock()
CURRENT_ACTION = {"name": None, "started": 0}


def http_get(url, headers=None, timeout=5):
    req = urllib.request.Request(url, headers=headers or {})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, r.read().decode("utf-8", "replace")
    except urllib.error.HTTPError as e:
        return e.code, ""
    except Exception as e:  # noqa: BLE001 — a network error is a state, not an exception
        return 0, str(e)


def node_status(n):
    t0 = now()
    c = cfg()
    args = (f" {n['host']} {HEAD_HOST} {shlex.quote(HF_CACHE_DIR)} {c['model'] or '-'} {c['revision'] or '-'}"
            f" {shlex.quote(c['image']) if c['image'] else '-'}")
    rc, st, err = ssh(n["host"], "status" + args.rstrip(), timeout=30)
    res = {**n, "reachable": st is not None and "error" not in (st or {}),
           "latency_ms": int((now() - t0) * 1000), "error": None if st else redact(err)[-300:]}
    if st:
        res.update(st)
    return res


def scan_head(since):
    rc, sc, _ = ssh(HEAD_HOST, f"scan {since}" if since else "scan", timeout=90)
    return sc or {"lines": [], "last": None}


def analyse(lines):
    phase, pct, kv_tokens, concurrency, errs = None, None, None, None, []
    for l in lines:
        for rx, name in PHASES:
            m = rx.search(l)
            if m:
                phase = name
                pct = int(m.group(1)) if m.groups() else None
        m = re.search(r"GPU KV cache size: ([\d,]+) tokens", l)
        if m:
            kv_tokens = int(m.group(1).replace(",", ""))
        m = re.search(r"Maximum concurrency for ([\d,]+) tokens per request: ([\d.]+)x", l)
        if m:
            concurrency = {"tokens": int(m.group(1).replace(",", "")), "x": float(m.group(2))}
        if ERRORS.search(l) and not IGNORE_ERRORS.search(l):
            errs.append(l)
    return phase, pct, kv_tokens, concurrency, errs


APP_STARTED = time.time()
_hung_polls = {"n": 0}


def healthy_since_start(st, hc):
    return bool(hc.get("started_at")) and st.get("ready_for") == hc["started_at"]


def poll_once():
    with ThreadPoolExecutor(max_workers=max(1, len(NODES))) as ex:
        nodes = list(ex.map(node_status, NODES))
    head = nodes[0] if nodes else {}
    hc = (head.get("container") or {})
    started = parse_ts(hc.get("started_at"))
    scan = scan_head(hc.get("started_at")) if hc.get("running") else {"lines": [], "last": None}
    phase, pct, kv, conc, errs = analyse(scan["lines"])

    code, _ = http_get(f"http://{HEAD_HOST}:{VLLM_PORT}/health")
    healthy = code == 200
    models, vllm_version = [], None
    if healthy:
        c3, vbody = http_get(f"http://{HEAD_HOST}:{VLLM_PORT}/version")  # no API key needed
        try:
            vllm_version = json.loads(vbody)["version"] if c3 == 200 else None
        except (ValueError, KeyError):
            pass
        c2, body = http_get(f"http://{HEAD_HOST}:{VLLM_PORT}/v1/models",
                            {"Authorization": f"Bearer {API_KEY}"})
        try:
            models = [m["id"] for m in json.loads(body)["data"]] if c2 == 200 else []
        except (ValueError, KeyError):
            pass

    image_ids = {n.get("container", {}).get("image_id") for n in nodes
                 if n.get("container")}
    uptime = now() - started if started else None
    last_ts = parse_ts((scan.get("last") or "").split(" ", 1)[0])
    stall = now() - last_ts if last_ts else None
    roce_bad = [{"role": n["role"], "host": n["host"], "hca": r["hca"], "iface": r["iface"]}
                for n in nodes for r in (n.get("roce") or []) if not r["ok"]] if len(NODES) > 1 else []

    if healthy:
        state = "ready"
    elif not head.get("reachable"):
        state = "head unreachable"
    elif not hc:
        state = "no container"
    elif not hc.get("running"):
        state = "stopped"
    else:
        # A cluster that has answered once since this container started is never "starting"
        # again, only "not responding" (with its grace period). Remembered in the state file,
        # so an app restart while the cluster is busy can't mistake hours of uptime for a hang.
        st = load_state()
        if healthy_since_start(st, hc) or scan.get("ready_seen") or phase == "ready":
            state = "not responding"
        else:
            state = "starting"
    if healthy and hc.get("started_at"):
        st = load_state()
        if st.get("ready_for") != hc["started_at"]:
            st["ready_for"] = hc["started_at"]
            save_state(st)

    o = ops()
    global UNHEALTHY_SINCE
    UNHEALTHY_SINCE = None if state != "not responding" else (UNHEALTHY_SINCE or now())
    hung = None
    if state == "starting" and uptime is not None:
        if uptime > o["hang_timeout_min"] * 60:
            hung = f"startup has run for {dur(uptime)} without responding"
        elif errs and uptime > 180:
            hung = f"startup shows {len(errs)} error line(s)"
        elif stall and stall > o["stall_timeout_min"] * 60 and uptime > o["stall_timeout_min"] * 60:
            hung = f"no new log line for {dur(stall)} (phase: {phase or 'unknown'})"
    elif state == "not responding" and now() - UNHEALTHY_SINCE > o["unhealthy_grace_min"] * 60:
        hung = f"was ready, has not answered /health for {dur(now() - UNHEALTHY_SINCE)}"
    _hung_polls["n"] = _hung_polls["n"] + 1 if hung else 0

    STATUS.update({
        "updated": now(),
        "nodes": nodes,
        "cluster": {
            "state": state,
            "phase": "ready" if healthy else phase,
            "phase_pct": pct,
            "healthy": healthy,
            "models": models,
            "vllm_version": vllm_version,
            "image_ref": (load_state().get("applied") or {}).get("image"),
            "kv_tokens": kv,
            "concurrency": conc,
            "errors": [redact(e) for e in errs[-15:]],
            "last_log": redact(scan.get("last")),
            "uptime_s": uptime,
            "log_silent_s": stall,
            "hung": bool(hung),
            "hung_reason": hung,
            "roce_bad": roce_bad,
            "mem_low": [n["host"] for n in nodes
                        if (n.get("mem") or {}).get("available", 1 << 62) < o["mem_warn_gib"] * 2**30],
            "image_mismatch": len(image_ids) > 1,
            "workers_down": [n["host"] for n in nodes[1:]
                             if not (n.get("container") or {}).get("running")],
        },
        "setup": setup_summary(nodes, models),
        "action": dict(CURRENT_ACTION),
    })
    try:
        record_usage(nodes, models)
    except Exception as e:  # noqa: BLE001 — statistics must never stop the polling
        event(f"usage statistics: {e}", "warn")
    return STATUS


_model_size = {}
_hf = {"ts": 0, "info": None}


def hf_status():
    """Whether a Hugging Face token is set and which account it belongs to (checked once an hour)."""
    tok = hf_token()
    base = {"where": ENV_WHERE, "locked": bool(HF_TOKEN)}
    if not tok:
        return {"set": False, **base}
    if now() - _hf["ts"] > 3600 or _hf["info"] is None or _hf.get("tok") != tok:
        _hf["ts"], _hf["tok"] = now(), tok
        code, body = http_get("https://huggingface.co/api/whoami-v2",
                              {"Authorization": f"Bearer {tok}"}, timeout=10)
        try:
            _hf["info"] = {"valid": True, "user": json.loads(body).get("name")} if code == 200 \
                else {"valid": False if code == 401 else None}
        except ValueError:
            _hf["info"] = {"valid": None}
    return {"set": True, **base, **_hf["info"]}



def model_size():
    """Total size of the model repo from the HF API (cached), for download progress and disk check."""
    c = cfg()
    if not c["model"]:
        return None
    key = (c["model"], c["revision"])
    size, checked = _model_size.get(key), _model_size.get(("ts",) + key, 0)
    if size is None and now() - checked > 600:  # a failed lookup is retried every 10 min
        _model_size[("ts",) + key] = now()
        hdr = {"Authorization": f"Bearer {hf_token()}"} if hf_token() else {}
        code, body = http_get(f"https://huggingface.co/api/models/{c['model']}/revision/"
                              f"{c['revision'] or 'main'}?blobs=true", hdr, timeout=15)
        try:
            if code == 200:
                size = _model_size[key] = sum(f.get("size") or 0 for f in json.loads(body)["siblings"])
        except (ValueError, KeyError):
            pass
    return size


AGENT_VERSION = re.search(r'^VERSION = "(\d+)"', open(os.path.join(os.path.dirname(__file__),
                          "vllmapp-agent")).read(), re.M).group(1)


def setup_summary(nodes, running=()):
    """What still needs doing before the cluster can start: root fixes per node, env, model."""
    size = model_size()
    c = cfg()
    missing = [k for k, v in (("HEAD_HOST", HEAD_HOST), ("API_KEY", API_KEY)) if not v]
    per = []
    for n in nodes:
        m = n.get("model") or {}
        dl = m.get("download") or {}
        checks = dict(n.get("checks") or {})
        if len(NODES) < 2:  # one node opens no cluster ports
            checks.pop("cluster_ports", None)
        if "disk_free" in checks and size and not m.get("present"):
            need = size - m.get("bytes", 0)
            checks["disk_free"] = {**checks["disk_free"], "ok": checks["disk_free"]["bytes"] > need * 1.05,
                                   "need": need}
        per.append({"role": n["role"], "host": n["host"], "reachable": n.get("reachable"),
                    "agent": n.get("agent"), "agent_old": n.get("reachable") and n.get("agent") != AGENT_VERSION,
                    "checks": checks, "model": m or None,
                    "lan_ips": n.get("lan_ips") or [], "models": n.get("models") or [],
                    "error": n.get("error") if not n.get("reachable") else None,
                    "downloading": bool(dl.get("running")),
                    "download_failed": bool(dl) and not dl.get("running") and dl.get("exit_code") != 0})
    applied = load_state().get("applied")
    return {"missing_env": missing, "env_where": ENV_WHERE, "hf": hf_status(), "model": c["model"], "revision": c["revision"], "size": size,
            "no_model": CONFIG_MODE == "managed" and not (c["model"] and c["image"]),
            "patchset": c["patchset"],
            "patchset_missing": bool(c["patchset"]) and c["patchset"] not in PATCHSETS,
            "agent_version": AGENT_VERSION, "nodes": per,
            "pending": CONFIG_MODE == "managed" and (
                applied is not None and any(applied.get(k, d) != v for k, v in applied_key(c).items()
                                            for d in [{"executor": "ray", "env": {}, "nodes": v}.get(k)])
                or bool(running) and bool(c["served_model_name"]) and c["served_model_name"] not in running),
            "model_ready": bool(c["model"]) and all((p["model"] or {}).get("present") for p in per)}


def applied_key(c):
    """What decides the rendered config, to tell whether a restart is needed to apply it."""
    return {**{k: c[k] for k in (*MODEL_KEYS, "patchset", "executor", "env")},
            "nodes": [n["host"] for n in NODES]}


# ---------- usage statistics ----------
_stats = {"counters": None, "ts": None}
_stats_lock = threading.Lock()


def stats_db():
    db = sqlite3.connect(STATS_DB, timeout=10)
    db.execute("""CREATE TABLE IF NOT EXISTS hourly (
        hour INTEGER, model TEXT, prompt INTEGER DEFAULT 0, cached INTEGER DEFAULT 0,
        gen INTEGER DEFAULT 0, requests INTEGER DEFAULT 0, gpu_wh REAL DEFAULT 0,
        extra_wh REAL DEFAULT 0, PRIMARY KEY (hour, model))""")
    return db


def vllm_counters():
    """Token and request counters from vLLM's /metrics, summed per model."""
    code, body = http_get(f"http://{HEAD_HOST}:{VLLM_PORT}/metrics",
                          {"Authorization": f"Bearer {API_KEY}"})
    if code != 200:
        return None
    want = {"vllm:prompt_tokens_total": "prompt", "vllm:prompt_tokens_cached_total": "cached",
            "vllm:generation_tokens_total": "gen", "vllm:request_success_total": "requests"}
    gauges = {"vllm:num_requests_running": "running", "vllm:num_requests_waiting": "waiting",
              "vllm:kv_cache_usage_perc": "kv"}
    res, load = {}, {"running": 0, "waiting": 0, "kv": 0.0}
    for line in body.splitlines():
        m = re.match(r'^([a-z_:]+)\{([^}]*)\} ([0-9.e+]+)$', line)
        if not m:
            continue
        if m.group(1) in gauges:
            load[gauges[m.group(1)]] += float(m.group(3))
        if m.group(1) not in want:
            continue
        model = (re.search(r'model_name="([^"]*)"', m.group(2)) or [None, ""])[1]
        c = res.setdefault(model, {"prompt": 0, "cached": 0, "gen": 0, "requests": 0})
        c[want[m.group(1)]] += float(m.group(3))
    _live["gauges"] = load
    return res


_live = {"gauges": None}


def record_usage(nodes, running):
    """Add this poll's tokens (counter deltas) and energy (power × time) to the hour's row."""
    t = now()
    counters = vllm_counters() if running else None
    watts = sum((n.get("gpu") or {}).get("power") or 0 for n in nodes)
    extra = ops()["extra_watts"] * sum(1 for n in nodes if n.get("reachable"))
    with _stats_lock:
        last, last_ts = _stats["counters"], _stats["ts"]
        _stats.update(counters=counters, ts=t)
        if last_ts is None:
            return  # first sample after start: only a baseline
        # live load for the Overview: the gauges, and tokens per second since the last poll
        tot = lambda cs, k: sum(c[k] for c in (cs or {}).values())
        span = t - last_ts
        if counters and last and span > 0 and _live["gauges"] is not None:
            rate = lambda k: max(0.0, tot(counters, k) - tot(last, k)) / span
            STATUS["load"] = {**_live["gauges"], "prompt_tps": rate("prompt"), "gen_tps": rate("gen"), "ts": t}
        else:
            STATUS["load"] = None
        dt = min(t - last_ts, 5 * POLL_SECONDS)  # a gap (app down) is not counted as energy
        rows = {}
        for model, c in (counters or {}).items():
            prev = (last or {}).get(model)
            # a counter that went down means vLLM restarted: everything since then is new
            rows[model] = {k: v - prev[k] if prev and v >= prev[k] else (v if prev else 0)
                           for k, v in c.items()}
        model = running[0] if running else ""
        r = rows.setdefault(model, {"prompt": 0, "cached": 0, "gen": 0, "requests": 0})
        r["gpu_wh"] = watts * dt / 3600
        r["extra_wh"] = extra * dt / 3600
        hour = int(t // 3600 * 3600)
        db = stats_db()
        with db:
            for m, r in rows.items():
                db.execute("""INSERT INTO hourly (hour, model, prompt, cached, gen, requests, gpu_wh, extra_wh)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT (hour, model) DO UPDATE SET
                    prompt = prompt + excluded.prompt, cached = cached + excluded.cached,
                    gen = gen + excluded.gen, requests = requests + excluded.requests,
                    gpu_wh = gpu_wh + excluded.gpu_wh, extra_wh = extra_wh + excluded.extra_wh""",
                           (hour, m, int(r["prompt"]), int(r["cached"]), int(r["gen"]),
                            int(r["requests"]), r.get("gpu_wh", 0), r.get("extra_wh", 0)))
        db.close()


def auto_recover():
    c = STATUS["cluster"]
    s = load_state()
    if c.get("healthy"):
        if s["attempts"]:
            event(f"cluster responding again after {s['attempts']} automatic action(s)", "ok")
            s["attempts"] = 0
            save_state(s)
        return
    o = ops()
    if not o["auto_recover"] or not c.get("hung") or CURRENT_ACTION["name"]:
        return
    # never act on one observation, and not while the app itself is just starting
    if _hung_polls["n"] < 2 or time.time() - APP_STARTED < 120:
        return
    if now() - s["last_action"] < max(o["stall_timeout_min"], 5) * 60:
        return
    if c.get("roce_bad"):
        if s.get("gave_up", 0) <= s["last_action"]:
            event(f"cluster is hung and RoCE GID 3 is missing ({roce_text(c['roce_bad'])}) "
                  "— a restart won't help, the link needs resetting (sudo)", "error")
            s["gave_up"] = now()
            save_state(s)
        return
    if s["attempts"] < o["max_auto_restarts"]:
        event(f"hang detected: {c.get('hung_reason')} — restarting the cluster automatically", "warn")
        start_action("restart", auto=True)
    elif o["allow_reboot"] and o["auto_reboot"] and now() - s["last_reboot"] > 6 * 3600:
        event("restart did not help — rebooting the nodes automatically", "warn")
        start_action("reboot", auto=True)
    elif s.get("gave_up", 0) <= s["last_action"]:
        event("cluster is hung and automation has given up — needs a human", "error")
        s["gave_up"] = now()
        save_state(s)


_watch = {"healthy": None, "bad_polls": 0, "down_sent": 0}


def watch_health():
    """Alert when a cluster that answered stops answering (two polls in a row, not during an action
    the panel runs), and when it answers again."""
    c = STATUS.get("cluster") or {}
    healthy = bool(c.get("healthy"))
    bad = not healthy and c.get("state") not in ("stopped", "starting", None) and not CURRENT_ACTION["name"]
    _watch["bad_polls"] = _watch["bad_polls"] + 1 if bad else 0
    model = ", ".join(c.get("models") or []) or cfg()["served_model_name"] or "the model"
    if _watch["bad_polls"] == 2 and _watch["healthy"] is not False:
        _watch["down_sent"] = now()
        alert(f"{model} stopped answering: {c.get('state')}" + (f" ({c.get('hung_reason')})" if c.get("hung") else "")
              + (". Auto-recover is on." if ops()["auto_recover"] else ". Auto-recover is off."), "error")
        _watch["healthy"] = False
    elif healthy:
        if _watch["healthy"] is False and _watch["down_sent"]:
            alert(f"{model} is answering again after {dur(now() - _watch['down_sent'])}", "ok")
        _watch["healthy"], _watch["down_sent"] = True, 0


def poller():
    while True:
        try:
            poll_once()
            auto_recover()
            watch_health()
        except Exception as e:  # noqa: BLE001
            event(f"polling error: {e}", "error")
        time.sleep(POLL_SECONDS)


# ---------- actions ----------
def on_nodes(nodes, cmd, timeout=600):
    ok = True
    for n in nodes:
        rc, res, err = ssh(n["host"], cmd, timeout=timeout)
        good = rc == 0 and res and res.get("rc", 0) == 0 and "error" not in res
        ok &= bool(good)
        detail = (res or {}).get("out") or (res or {}).get("error") or err
        event(f"{n['role']} {n['host']}: {cmd} → {'ok' if good else 'FAILED'}"
              + ("" if good else f" ({redact(str(detail))[-300:]})"),
              "info" if good else "error")
    return ok


def roce_text(bad):
    return ", ".join(f"{b['role']} {b['host']} {b['hca']}" for b in bad)


def roce_fix(bad):
    """Commands that bring the GID back: reconnecting the device re-adds it."""
    out = []
    for host in dict.fromkeys(b["host"] for b in bad):
        ifs = " ".join(b["iface"] for b in bad if b["host"] == host and b["iface"])
        out.append(f"# on {host}, over a different network than the link itself\n"
                   f"for i in {ifs}; do sudo nmcli dev disconnect $i; sudo nmcli dev connect $i; done")
    return "\n".join(out)


def preflight():
    """RoCE GID 3 must exist on every node, otherwise NCCL init fails with 'unhandled system error'.
    One node uses no link at all, so there is nothing to check."""
    if len(NODES) < 2:
        return
    with ThreadPoolExecutor(max_workers=max(1, len(NODES))) as ex:
        res = list(ex.map(lambda n: (n, ssh(n["host"], "roce", timeout=20)[1]), NODES))
    bad = [{"role": n["role"], "host": n["host"], "hca": r["hca"], "iface": r["iface"]}
           for n, r in res for r in ((r or {}).get("roce") or []) if not r["ok"]]
    if bad:
        raise RuntimeError(f"RoCE GID 3 missing on {roce_text(bad)} — NCCL would fail. Fix:\n"
                           + roce_fix(bad))
    event("preflight: RoCE GID 3 present on all nodes", "ok")


def model_check():
    """Managed mode: the model must be downloaded on every node (vLLM runs offline)."""
    c = cfg()
    if CONFIG_MODE != "managed":
        return
    if not (c["model"] and c["image"]):
        raise RuntimeError("no model chosen — pick one under Model")
    s = poll_once().get("setup") or {}  # fresh, in case the model was just changed
    bad = [p["host"] for p in s.get("nodes", []) if p.get("model") and not p["model"].get("present")]
    if bad:
        raise RuntimeError(f"{c['model']} is not fully downloaded on {', '.join(bad)} — use Download model first")


def drop_caches():
    """Free the page cache on every node before vLLM measures free memory. On DGX Spark the
    GPU shares system memory, and a warm cache (a big download, say) leaves less for the KV
    cache. Needs a sudoers line; without it the start goes on as before."""
    with ThreadPoolExecutor(max_workers=max(1, len(NODES))) as ex:
        res = list(ex.map(lambda n: (n, ssh(n["host"], "drop-caches", timeout=120)), NODES))
    for n, (rc, r, err) in res:
        r = r or {}
        if r.get("rc") == 0:
            event(f"{n['role']} {n['host']}: page cache freed ({r.get('freed_gib', '?')} GiB)")
        elif r.get("no_sudo"):
            pass  # shown as an optional item under Setup
        else:
            event(f"{n['role']} {n['host']}: could not free the page cache ({redact(str(r.get('out') or err))[-150:]})", "warn")


def do_start():
    model_check()
    preflight()
    write_config()
    drop_caches()
    on_nodes(NODES[1:], "up")
    on_nodes(NODES[:1], "up")


def do_stop():
    on_nodes(NODES[:1], "stop")
    on_nodes(NODES[1:], "stop")


def do_restart():
    model_check()
    preflight()
    write_config()
    # stop + up rather than compose restart, which would keep the old .env and compose.yaml
    on_nodes(NODES[:1], "stop")
    on_nodes(NODES[1:], "stop")
    drop_caches()
    on_nodes(NODES[1:], "up")
    on_nodes(NODES[:1], "up")


def do_reboot():
    if not ops()["allow_reboot"]:
        raise RuntimeError("the reboot button is off under Settings")
    s = load_state()
    s["last_reboot"] = now()
    save_state(s)
    on_nodes(NODES[1:], "reboot", timeout=30)
    time.sleep(5)
    on_nodes(NODES[:1], "reboot", timeout=30)  # the app goes down with the head if it runs there


def do_pull():
    """Pull the chosen model's image on every node, so a switch doesn't wait for it (and the
    image Easypanel's daily cleanup removed from the head comes back)."""
    image = cfg()["image"]
    if not image:
        raise RuntimeError("no model chosen")
    with ThreadPoolExecutor(max_workers=max(1, len(NODES))) as ex:
        ok = all(ex.map(lambda n: on_nodes([n], f"pull-image {shlex.quote(image)}", timeout=3600), NODES))
    if not ok:
        raise RuntimeError("the image could not be pulled on every node — see Events")


def write_config():
    """Managed mode: write the config to every node before the cluster starts.
    An app deploy never gets here, so new env only takes effect on Start/Restart."""
    if CONFIG_MODE != "managed":
        return
    failed = False
    c = cfg()
    for i, n in enumerate(NODES):
        for name, content in render(i, n, c).items():
            rc, res, err = ssh(n["host"], f"put {shlex.quote(name)}", input=content, timeout=30)
            good = rc == 0 and res and res.get("rc") == 0
            event(f"{n['role']} {n['host']}: wrote {name}"
                  + (" (unchanged)" if good and not res.get("changed") else "")
                  + ("" if good else f" — FAILED {redact(str(res or err))[-200:]}"),
                  "info" if good else "error")
            failed |= not good
    if failed:
        raise RuntimeError("could not write the config to every node — cluster not started")
    s = load_state()
    s["applied"] = applied_key(c)
    save_state(s)


def check_download_size(c):
    """`hf download` fetches the whole repo. Refuse repos that hold far more than the model
    (GGUF repos with every quantization run to terabytes) or that don't fit on a node's disk."""
    size = model_size()
    if not size:
        event("could not read the repo size from Hugging Face — downloading without the size check", "warn")
        return
    gb = size / 1e9
    expected = c["entry"].get("size_gb")
    if expected and gb > max(expected * 1.5, expected + 20):
        raise RuntimeError(f"the repo holds {gb:.0f} GB but the model is {expected} GB — it contains other "
                           "files too (several quantizations or GGUF variants); pick a repo with one variant")
    for n in STATUS["nodes"]:
        m = n.get("model") or {}
        free = ((n.get("checks") or {}).get("disk_free") or {}).get("bytes")
        need = size - m.get("bytes", 0)
        if free is not None and not m.get("present") and need * 1.05 > free:
            raise RuntimeError(f"{n['role']} {n['host']}: the download needs {need / 1e9:.0f} GB "
                               f"but only {free / 1e9:.0f} GB is free")


def do_download():
    c = cfg()
    if not (c["model"] and c["image"]):
        raise RuntimeError("no model chosen — pick one under Model")
    check_download_size(c)
    cmd = f"download {shlex.quote(c['image'])} {c['model']} {shlex.quote(HF_CACHE_DIR)} {c['revision']}".rstrip()

    def one(n):
        rc, res, err = ssh(n["host"], cmd, timeout=3600, input=hf_token())
        good = rc == 0 and res and res.get("rc") == 0
        event(f"{n['role']} {n['host']}: download "
              + ("started" if good else f"FAILED ({redact(str((res or {}).get('out') or err))[-300:]})"),
              "info" if good else "error")
        return good
    with ThreadPoolExecutor(max_workers=max(1, len(NODES))) as ex:
        if not all(ex.map(one, NODES)):
            raise RuntimeError("the download did not start on every node")


def do_download_stop():
    on_nodes(NODES, "download-stop", timeout=90)


ACTIONS = {"start": do_start, "stop": do_stop, "restart": do_restart,
           "reboot": do_reboot, "pull": do_pull,
           "download": do_download, "download-stop": do_download_stop}


def start_action(name, auto=False):
    if not _action_lock.acquire(blocking=False):
        raise HTTPException(409, f"{CURRENT_ACTION['name']} is already running")
    CURRENT_ACTION.update(name=name, started=now())
    s = load_state()
    s["last_action"] = now()
    if auto:
        s["attempts"] += 1
    save_state(s)

    def run():
        try:
            event(f"{'auto: ' if auto else ''}{name} started")
            ACTIONS[name]()
            event(f"{name} done", "ok")
        except Exception as e:  # noqa: BLE001
            event(f"{name} failed: {e}", "error")
        finally:
            CURRENT_ACTION.update(name=None, started=0)
            _action_lock.release()

    threading.Thread(target=run, daemon=True).start()


# ---------- managed mode: render config ----------
COMPOSE = """# Generated by vLLM Panel — change it in the panel, not here
services:
  vllm:
    image: ${VLLM_IMAGE}
    container_name: vllm-${ROLE}
    restart: unless-stopped
    logging:  # json-file has no limit by default; a node would fill up over months
      driver: json-file
      options:
        max-size: "50m"
        max-file: "3"
    entrypoint: ["/bin/bash", "/opt/entrypoint.sh"]
    network_mode: host
    ipc: host
    shm_size: 16gb
    ulimits:
      memlock: -1
      stack: 67108864
    devices:
      - /dev/infiniband:/dev/infiniband
    deploy:
      resources:
        reservations:
          devices:
            - driver: nvidia
              count: all
              capabilities: [gpu]
    volumes:
      - ./entrypoint.sh:/opt/entrypoint.sh:ro
      - {hf_cache}:/root/.cache/huggingface
{mounts}    env_file: .env
    environment:
      VLLM_HOST_IP: ${HOST_IP}
      NCCL_SOCKET_IFNAME: ${IF_NAME}
      GLOO_SOCKET_IFNAME: ${IF_NAME}
      TP_SOCKET_IFNAME: ${IF_NAME}
      UCX_NET_DEVICES: ${IF_NAME}
      OMPI_MCA_btl_tcp_if_include: ${IF_NAME}
      NCCL_IB_HCA: ${IB_HCA}
      NCCL_IB_GID_INDEX: "3"
      RAY_memory_monitor_refresh_ms: "0"
      MASTER_ADDR: ${HEAD_IP}
      VLLM_USE_V2_MODEL_RUNNER: "0"
      CUTE_DSL_ARCH: sm_121a
      HF_HUB_OFFLINE: "{offline}"
"""

ENTRYPOINT = r"""#!/bin/bash
# Generated by vLLM Panel. EXECUTOR=ray: ROLE=head runs the Ray head and vLLM (:8000), a worker
# joins Ray and blocks. EXECUTOR=mp: vLLM's own multi-node mode, no Ray; every node runs
# vllm serve with its NODE_RANK, the head serves the API and the workers run --headless.
set -euo pipefail

# Run the FlashInfer autotune from scratch on every start. With a saved cache the
# ranks hit it differently and fall out of step (gloo waiting on one, NCCL on the
# other) until the 30 min timeout. Without a cache the tuning takes ~1 min.
rm -rf /root/.cache/vllm/flashinfer_autotune_cache

# One node: a plain vllm serve, with neither Ray nor the multi-node flags.
if [ "${NUM_NODES:-1}" -le 1 ]; then
  echo "Starting vLLM with ${MODEL} (one node)"
  exec vllm serve "${MODEL}" --tensor-parallel-size "${TP_SIZE:-1}" \
    --host 0.0.0.0 --port 8000 --api-key "${VLLM_API_KEY}" --disable-uvicorn-access-log ${VLLM_EXTRA_ARGS:-}
fi

if [ "${EXECUTOR:-ray}" = "mp" ]; then
  MP_ARGS=(--tensor-parallel-size "${TP_SIZE:-$NUM_NODES}" --distributed-executor-backend mp
           --nnodes "${NUM_NODES}" --node-rank "${NODE_RANK}"
           --master-addr "${HEAD_IP}" --master-port "${MASTER_PORT:-29501}")
  if [ "$ROLE" = "worker" ]; then
    echo "Starting vLLM with ${MODEL} (multiprocessing executor, node rank ${NODE_RANK}, headless)"
    exec vllm serve "${MODEL}" "${MP_ARGS[@]}" --headless ${VLLM_EXTRA_ARGS:-}
  fi
  echo "Starting vLLM with ${MODEL} (multiprocessing executor, node rank 0)"
  exec vllm serve "${MODEL}" "${MP_ARGS[@]}" \
    --host 0.0.0.0 --port 8000 --api-key "${VLLM_API_KEY}" --disable-uvicorn-access-log ${VLLM_EXTRA_ARGS:-}
fi

if [ "$ROLE" = "worker" ]; then
  until ray start --block --address="${HEAD_IP}:6379" --node-ip-address="${HOST_IP}"; do
    echo "Waiting for head ${HEAD_IP}..."; sleep 5
  done
  exit 0
fi

# No Ray dashboard: nothing uses it, and its nine sub-processes cost ~400 MB on the head,
# which also runs Easypanel and the app and is the node that runs short of memory.
ray start --head --node-ip-address="${HOST_IP}" --port=6379 --include-dashboard=false
echo "Waiting for ${NUM_NODES} nodes in the Ray cluster..."
until [ "$(python3 -c 'import ray; ray.init(address="auto", logging_level="ERROR"); print(sum(n["Alive"] for n in ray.nodes()))' 2>/dev/null)" -ge "${NUM_NODES}" ]; do
  sleep 5
done
echo "All nodes up – starting vLLM with ${MODEL}"

exec vllm serve "${MODEL}" \
  --tensor-parallel-size "${TP_SIZE:-$NUM_NODES}" \
  --distributed-executor-backend ray \
  --host 0.0.0.0 --port 8000 \
  --api-key "${VLLM_API_KEY}" \
  --disable-uvicorn-access-log \
  ${VLLM_EXTRA_ARGS:-}
"""

_net_cache = {}


def net_for(i, n):
    if IF_NAMES and IB_HCAS:
        return IF_NAMES.split(";")[i].strip(), IB_HCAS.split(";")[i].strip()
    if n["host"] not in _net_cache:
        rc, res, err = ssh(n["host"], f"netdetect {n['host']}", timeout=20)
        _net_cache[n["host"]] = ((res or {}).get("iface") or "",
                                 ",".join((res or {}).get("hcas") or []))
    return _net_cache[n["host"]]


def vllm_args(c):
    a = []
    if c["served_model_name"]:
        a += ["--served-model-name", c["served_model_name"]]
    if c["max_model_len"]:
        a += ["--max-model-len", c["max_model_len"]]
    if c["gpu_mem_util"]:
        a += ["--gpu-memory-utilization", c["gpu_mem_util"]]
    if c["revision"] and "--revision" not in c["vllm_args"]:
        a += ["--revision", c["revision"]]
    return " ".join(a + shlex.split(c["vllm_args"]))


def render(i, n, c=None):
    c = c or cfg()
    iface, hca = net_for(i, n)
    envfile = "\n".join([
        "# Generated by vLLM Panel — change it in the panel, not here",
        f"ROLE={n['role']}",
        f"VLLM_IMAGE={c['image']}",
        f"HOST_IP={n['host']}",
        f"HEAD_IP={HEAD_HOST}",
        f"IF_NAME={iface}",
        f"IB_HCA={hca}",
        f"NUM_NODES={len(NODES)}",
        f"NODE_RANK={i}",
        f"EXECUTOR={c['executor']}",
        "MASTER_PORT=29501",
        f"TP_SIZE={TP_SIZE or len(NODES)}",
        f"MODEL={c['model']}",
        f"VLLM_EXTRA_ARGS={vllm_args(c)}",
        f"VLLM_API_KEY={API_KEY}",
        f"HF_TOKEN={hf_token()}",
    ] + [f"{k}={v}" for k, v in c["env"].items()]) + "\n"
    mounts = [m if m.count(":") >= 2 else m + ":ro" for m in EXTRA_MOUNTS]
    files = {".env": envfile}
    ps = PATCHSETS.get(c["patchset"])
    if ps:
        for name, dst in ps["mounts"].items():
            files[f"patches/{name}"] = open(os.path.join(PATCHSET_DIR, c["patchset"], name)).read()
            mounts.append(f"./patches/{name}:{dst.replace('{revision}', ps.get('revision', ''))}:ro")
    compose = COMPOSE.replace("{hf_cache}", HF_CACHE_DIR) \
        .replace("{mounts}", "".join(f"      - {m}\n" for m in mounts)) \
        .replace("{offline}", "1" if HF_OFFLINE else "0")
    return {**files, "compose.yaml": compose, "entrypoint.sh": ENTRYPOINT}


def preview():
    out = []
    for i, n in enumerate(NODES):
        files = []
        for name, new in render(i, n).items():
            rc, res, err = ssh(n["host"], f"read {shlex.quote(name)}", timeout=20)
            cur = (res or {}).get("content", "")
            diff = "".join(difflib.unified_diff(
                redact(cur).splitlines(True), redact(new).splitlines(True),
                f"{n['host']}:{name} (current)", f"{n['host']}:{name} (new)"))
            files.append({"name": name, "exists": (res or {}).get("exists", False),
                          "diff": diff, "rendered": redact(new)})
        out.append({**n, "files": files})
    return out


# ---------- web ----------
app = FastAPI(title="vllmapp")
SESSION_COOKIE = "vllmapp_session"
SESSION_DAYS = 30


def session_secret():
    """Signing key for the login cookie. It lives in /data so a login survives redeploys,
    and the password is mixed in so changing it logs everyone out."""
    path = os.path.join(DATA, "session.key")
    if not os.path.exists(path):
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, "w") as f:
            f.write(secrets.token_hex(32))
    return hashlib.sha256((open(path).read().strip() + ADMIN_USER + (password_hash() or ADMIN_PASSWORD)).encode()).digest()


def make_session(user):
    exp = str(int(now() + SESSION_DAYS * 86400))
    msg = f"{user}|{exp}"
    return msg + "|" + hmac.new(session_secret(), msg.encode(), hashlib.sha256).hexdigest()


def session_user(cookie):
    try:
        user, exp, sig = (cookie or "").rsplit("|", 2)
        good = hmac.new(session_secret(), f"{user}|{exp}".encode(), hashlib.sha256).hexdigest()
        return user if hmac.compare_digest(sig, good) and int(exp) > now() else None
    except ValueError:
        return None


def check_password(user, password):
    return secrets.compare_digest(user.encode(), ADMIN_USER.encode()) and password_ok(password)


def auth(request: Request):
    """A login cookie, or Basic auth for scripts (decoded as UTF-8, since FastAPI's HTTPBasic
    decodes ASCII and non-ASCII passwords always failed). Pages without either go to /login;
    API calls get a plain 401, so the browser shows no password dialog."""
    if not (ADMIN_PASSWORD or password_hash()):
        raise HTTPException(503, "ADMIN_PASSWORD is not set — set it in the app's env (Easypanel's env tab or ~/vllmapp/.env)")
    user = session_user(request.cookies.get(SESSION_COOKIE))
    if user:
        return user
    password = None
    scheme, _, param = request.headers.get("authorization", "").partition(" ")
    if scheme.lower() == "basic":
        raw = base64.b64decode(param + "===")
        for enc in ("utf-8", "latin-1"):
            try:
                user, _, password = raw.decode(enc).partition(":")
                break
            except UnicodeDecodeError:
                continue
    if user is not None and check_password(user, password):
        return user
    if request.url.path.startswith("/api/"):
        raise HTTPException(401, "not logged in")
    up = "../" * (request.url.path.count("/") - 1)  # relative, so it also works behind a path prefix
    raise HTTPException(303, headers={"Location": f"{up}login?next=" + urllib.parse.quote(request.url.path)})


LOGIN_PAGE = """<!doctype html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1"><title>vLLM Panel</title>
<link rel="icon" href="favicon.svg" type="image/svg+xml">
<script>try { const t = localStorage.getItem("vllmapp-theme"); if (t === "light" || t === "dark") document.documentElement.dataset.theme = t; } catch (e) {}</script>
<style>
:root { --bg: #f5f6f8; --card: #fff; --fg: #111827; --muted: #6b7280; --line: #e7e9ee; --accent: #5b5bd6; --err: #dc2626; }
@media (prefers-color-scheme: dark) {
  :root:not([data-theme="light"]) { --bg: #0b0d12; --card: #12151c; --fg: #e5e7eb; --muted: #8b93a3;
  --line: #232835; --accent: #8b8cf8; --err: #f87171; }
}
:root[data-theme="dark"] { --bg: #0b0d12; --card: #12151c; --fg: #e5e7eb; --muted: #8b93a3;
  --line: #232835; --accent: #8b8cf8; --err: #f87171; }
:root { color-scheme: light dark; } :root[data-theme="light"] { color-scheme: light; } :root[data-theme="dark"] { color-scheme: dark; }
body { margin: 0; background: var(--bg); color: var(--fg); font: 14px/1.5 Inter, "SF Pro Text", system-ui, -apple-system, "Segoe UI", sans-serif; -webkit-font-smoothing: antialiased;
       display: grid; place-items: center; min-height: 100vh; }
form { background: var(--card); border: 1px solid var(--line); border-radius: 16px; padding: 28px; width: min(340px, calc(100vw - 32px));
       box-shadow: 0 10px 30px rgba(16,24,40,.08), 0 2px 6px rgba(16,24,40,.05); }
h1 { font-size: 20px; letter-spacing: -.02em; margin: 0 0 16px; }
label { display: block; font-size: 12px; color: var(--muted); margin-top: 10px; }
input { width: 100%; box-sizing: border-box; font: inherit; padding: 9px 11px; border-radius: 9px; border: 1px solid var(--line);
        background: var(--bg); color: var(--fg); margin-top: 3px; }
button { margin-top: 16px; width: 100%; font: inherit; padding: 9px; border-radius: 9px; font-weight: 600; border: 1px solid var(--accent);
         background: var(--accent); color: #fff; cursor: pointer; }
input:focus { outline: none; border-color: var(--accent); box-shadow: 0 0 0 3px color-mix(in srgb, var(--accent) 22%, transparent); }
.err { color: var(--err); margin-top: 10px; }
</style></head><body>
<form method="post" action="login"><h1>vLLM Panel</h1>
<label>User<input name="user" value="{user}" autocomplete="username"></label>
<label>Password<input name="password" type="password" autocomplete="current-password" autofocus></label>
<input type="hidden" name="next" value="{next}">{error}
<button>Log in</button></form></body></html>"""


def safe_next(n):
    return n if re.match(r"^/[A-Za-z0-9/_-]*$", n or "") and not n.startswith("//") else "/"


def login_page(user="", nxt="/", error=""):
    esc = lambda v: v.replace("&", "&amp;").replace("<", "&lt;").replace('"', "&quot;")
    return LOGIN_PAGE.replace("{user}", esc(user)).replace("{next}", esc(safe_next(nxt))) \
        .replace("{error}", f'<div class="err">{esc(error)}</div>' if error else "")


@app.get("/login")
def login_form(next: str = "/"):
    return HTMLResponse(login_page(ADMIN_USER, next))


_login_fails = []


@app.post("/login")
async def login(request: Request):
    f = urllib.parse.parse_qs((await request.body()).decode("utf-8", "replace"))
    user, password = f.get("user", [""])[0], f.get("password", [""])[0]
    nxt = safe_next(f.get("next", ["/"])[0])
    _login_fails[:] = [t for t in _login_fails if now() - t < 300]
    if len(_login_fails) >= 10:  # slow down guessing: at most 10 wrong passwords per 5 minutes
        return HTMLResponse(login_page(user, nxt, "Too many attempts. Wait a few minutes."), 429)
    if not check_password(user, password):
        _login_fails.append(now())
        return HTMLResponse(login_page(user, nxt, "Wrong user or password."), 401)
    r = RedirectResponse("." + nxt, 303)
    https = request.headers.get("x-forwarded-proto", request.url.scheme) == "https"
    r.set_cookie(SESSION_COOKIE, make_session(user), max_age=SESSION_DAYS * 86400,
                 httponly=True, samesite="lax", secure=https)
    return r


IPV4 = re.compile(r"^(25[0-5]|2[0-4]\d|1?\d?\d)(\.(25[0-5]|2[0-4]\d|1?\d?\d)){3}$")


def cluster_running():
    return any((n.get("container") or {}).get("running") for n in STATUS.get("nodes") or [])


@app.post("/api/nodes", dependencies=[Depends(auth)])
async def api_nodes(request: Request):
    """Add a Spark, remove one, or change the head's address. The list replaces HEAD_HOST and
    WORKER_HOSTS from env. Adding works while the cluster runs (it applies at the next Restart);
    removing a node or moving the head needs a stopped cluster, so no container is left behind."""
    b = await request.json()
    action, host = b.get("action"), str(b.get("host") or "").strip()
    if action not in ("add", "remove", "head") or not IPV4.match(host):
        raise HTTPException(400, "give an IPv4 address, for example 192.168.100.2")
    head, workers = HEAD_HOST, list(WORKER_HOSTS)
    if action in ("remove", "head") and cluster_running():
        raise HTTPException(409, "stop the cluster first, so no container is left running on a node the app no longer manages")
    if action == "add":
        if host == head or host in workers:
            raise HTTPException(400, f"{host} is already in the cluster")
        if not b.get("force"):
            try:
                socket.create_connection((host, int(SSH_PORT)), timeout=3).close()
            except OSError:
                raise HTTPException(422, f"nothing answers on {host}:{SSH_PORT}. Check the cable and that the new Spark has "
                                         "this address on the cluster link, or add it anyway")
        workers.append(host)
    elif action == "remove":
        if host not in workers:
            raise HTTPException(400, f"{host} is not a worker here")
        workers.remove(host)
    else:
        if host in workers:
            raise HTTPException(400, f"{host} is a worker; remove it first")
        head = host
    save_settings({"nodes": {"head": head, "workers": workers}})
    apply_nodes()
    event({"add": f"node added: worker {host}. Install the agent on it, download the model, then Restart",
           "remove": f"node removed: worker {host}",
           "head": f"head address changed to {host}"}[action])
    return {"ok": True, "head": HEAD_HOST, "workers": WORKER_HOSTS, "nodes": len(NODES)}


_garage = {"ts": 0, "res": None}


@app.get("/api/garage", dependencies=[Depends(auth)])
def api_garage(fresh: int = 0):
    """GarageAI on the head node, as the agent reads it without root (cached for a minute)."""
    if fresh or now() - _garage["ts"] > 60 or _garage["res"] is None:
        rc, res, err = ssh(HEAD_HOST, f"garage {VLLM_PORT}", timeout=40)
        if res and "error" in res and "unknown command" in str(res["error"]):
            res = {"agent_old": True}
        elif not res:
            res = {"unreachable": True, "error": redact(str(err))[-200:]}
        _garage.update(ts=now(), res=res)
    c = cfg()
    return {**_garage["res"], "checked": _garage["ts"], "port": VLLM_PORT,
            "serving": (STATUS.get("cluster") or {}).get("models") or [],
            "chosen": c["served_model_name"], "agent_version": AGENT_VERSION}


# Flags the panel sets itself, from its own fields; an own template must not set them in its arguments.
PANEL_FLAGS = {"--host", "--port", "--api-key", "--served-model-name", "--gpu-memory-utilization",
               "--max-model-len", "--tensor-parallel-size", "-tp", "--distributed-executor-backend",
               "--nnodes", "--node-rank", "--master-addr", "--master-port", "--headless", "--model"}
PANEL_ENV = {"HF_TOKEN", "HUGGING_FACE_HUB_TOKEN", "VLLM_API_KEY", "HF_HUB_OFFLINE"}


def own_template(b, old_id=None):
    """Check an own template from the form and return the catalog entry."""
    def text(k, n):
        return str(b.get(k) or "").strip()[:n]
    name, model = text("name", 80), text("model", 200)
    if not name:
        raise HTTPException(400, "give the model a name")
    if not re.match(r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$", model):
        raise HTTPException(400, "the Hugging Face repo looks like org/model")
    served = text("served_model_name", 64) or model.split("/")[-1].lower()
    if not re.match(r"^[A-Za-z0-9_.-]+$", served):
        raise HTTPException(400, "the name for clients may only use letters, digits, '.', '_' and '-'")
    image = text("image", 300) or next((m["image"] for m in CATALOG.values()
                                         if m.get("status") == "verified" and m.get("executor") == "mp"), "")
    if not re.match(r"^[A-Za-z0-9_./:@-]+$", image):
        raise HTTPException(400, "the image looks like repo:tag or repo@sha256:...")
    executor = text("executor", 4) or "mp"
    if executor not in ("mp", "ray"):
        raise HTTPException(400, "executor is mp or ray")
    try:
        min_nodes = int(b.get("min_nodes") or 1)
        assert 1 <= min_nodes <= 8
        gpu = f"{float(b.get('gpu_mem_util') or 0.8):.2f}"
        assert 0.3 <= float(gpu) <= 0.95
        mlen = str(int(b.get("max_model_len") or 0) or "")
        assert not mlen or 1024 <= int(mlen) <= 1048576
    except (ValueError, AssertionError):
        raise HTTPException(400, "Sparks 1-8, GPU share 0.30-0.95, context 1024-1048576 tokens")
    args = text("vllm_args", 4000)
    try:
        toks = shlex.split(args)
    except ValueError as e:
        raise HTTPException(400, f"the vLLM arguments don't parse: {e}")
    taken = sorted({t.split("=")[0] for t in toks if t.split("=")[0] in PANEL_FLAGS})
    if taken:
        raise HTTPException(400, f"the panel sets these itself, from the fields above: {', '.join(taken)}")
    if any(" " in t for t in toks):
        raise HTTPException(400, "an argument contains a space; write JSON values without spaces")
    envs = {}
    for line in str(b.get("env") or "").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        k, sep, v = line.partition("=")
        k = k.strip()
        if not sep or not re.match(r"^[A-Z_][A-Z0-9_]*$", k):
            raise HTTPException(400, f"environment lines look like NAME=value: {line[:60]}")
        if k in PANEL_ENV:
            raise HTTPException(400, f"{k} is set by the panel")
        envs[k] = v.strip()
    slug = re.sub(r"[^a-z0-9.-]+", "-", served.lower()).strip("-")[:50] or "model"
    mid = old_id or f"my-{slug}"
    if not old_id and mid in CATALOG:
        raise HTTPException(409, f"there is already a model called {mid}; pick another name for clients")
    size = None
    code, body = http_get(f"https://huggingface.co/api/models/{model}?blobs=true",
                          {"Authorization": f"Bearer {hf_token()}"} if hf_token() else {}, timeout=15)
    if code == 200:
        try:
            size = round(sum(f.get("size") or 0 for f in json.loads(body)["siblings"]) / 1e9, 1)
        except (ValueError, KeyError):
            pass
    elif code in (401, 404):
        raise HTTPException(400, f"Hugging Face doesn't know {model} (or it is gated and the token has no access)")
    return {"order": 80, "id": mid, "name": name, "summary": text("summary", 200), "arch": text("arch", 80),
            "quant": text("quant", 40), "model": model, "served_model_name": served, "size_gb": size,
            "min_nodes": min_nodes, "status": "own", "gpu_mem_util": gpu, "max_model_len": mlen,
            "vllm_args": args, "notes": text("notes", 2000), "image": image, "patchset": "none",
            "executor": executor, "env": envs}


@app.post("/api/own-models", dependencies=[Depends(auth)])
async def api_own_save(request: Request):
    """Add or change an own model template. It lives in /data/models and survives updates."""
    b = await request.json()
    old = str(b.get("id") or "")
    if old and (old not in CATALOG or not CATALOG[old].get("own")):
        raise HTTPException(404, "unknown own model")
    m = own_template(b, old or None)
    os.makedirs(OWN_DIR, exist_ok=True)
    tmp = os.path.join(OWN_DIR, f".{m['id']}.tmp")
    json.dump(m, open(tmp, "w"), indent=2, ensure_ascii=False)
    os.replace(tmp, os.path.join(OWN_DIR, f"{m['id']}.json"))
    reload_catalog()
    event(f"own model {'changed' if old else 'added'}: {m['name']} ({m['model']})")
    return {"ok": True, "model": m}


@app.delete("/api/own-models/{mid}", dependencies=[Depends(auth)])
def api_own_delete(mid: str):
    if mid not in CATALOG or not CATALOG[mid].get("own"):
        raise HTTPException(404, "unknown own model")
    if load_settings().get("id") == mid:
        raise HTTPException(409, "that is the chosen model; pick another one first")
    os.remove(os.path.join(OWN_DIR, f"{mid}.json"))
    reload_catalog()
    event(f"own model removed: {mid}")
    return {"ok": True}


@app.post("/api/alert-test", dependencies=[Depends(auth)])
async def api_alert_test(request: Request):
    url = str((await request.json()).get("url") or load_settings().get("alert_url") or "").strip()
    if not re.match(r"^https?://[^\s]+$", url):
        raise HTTPException(400, "give a webhook URL that starts with http:// or https://")
    try:
        code = send_alert(url, "Test from vLLM Panel: alerts reach you here.", "ok")
    except Exception as e:  # noqa: BLE001
        raise HTTPException(502, f"the webhook did not accept it: {redact(str(e))[:200]}")
    return {"ok": True, "status": code}


def image_refs(n_image):
    """Every way an image on a node can be named, for matching against the catalog."""
    return set(n_image.get("tags") or []) | set(n_image.get("digests") or [])


@app.get("/api/images", dependencies=[Depends(auth)])
def api_images():
    """vLLM images per node: size, whether a container uses it, and which catalog models need it.
    Without Easypanel's daily cleanup nothing removes them, and each is 30+ GB."""
    c = cfg()
    by_image = {}
    for m in CATALOG.values():
        by_image.setdefault(m.get("image"), []).append(m["name"])
    repos = {(i or "").split("@")[0].rsplit(":", 1)[0] for i in by_image} | {(c["image"] or "").split("@")[0]}

    def one(n):
        rc, res, err = ssh(n["host"], "images", timeout=60)
        if not res or "images" not in res:
            return {"role": n["role"], "host": n["host"], "error": redact(str((res or {}).get("error") or err))[-200:],
                    "agent_old": bool(res and "unknown command" in str(res.get("error")))}
        rows = []
        for im in res["images"]:
            repo = im.get("repo") or ""
            if (repo not in repos and "vllm" not in repo) or "vllmapp" in repo or "vllm-cluster-spark" in repo:
                continue  # only vLLM images: not the panel's own, and not the node's other images
            refs = image_refs(im)
            needed = sorted({name for ref, names in by_image.items() if ref in refs for name in names})
            rows.append({**im, "needed_by": needed, "chosen": c["image"] in refs,
                         "removable": not im.get("used") and c["image"] not in refs})
        return {"role": n["role"], "host": n["host"], "images": rows}
    with ThreadPoolExecutor(max_workers=max(1, len(NODES))) as ex:
        return {"nodes": list(ex.map(one, NODES))}


@app.post("/api/remove-image", dependencies=[Depends(auth)])
async def api_remove_image(request: Request):
    b = await request.json()
    host, iid = str(b.get("host") or ""), str(b.get("id") or "")
    if host not in [n["host"] for n in NODES] or not re.match(r"^sha256:[0-9a-f]{64}$", iid):
        raise HTTPException(400, "unknown node or image")
    if CURRENT_ACTION["name"]:
        raise HTTPException(409, f"{CURRENT_ACTION['name']} is running")
    rc, res, _ = ssh(host, "images", timeout=60)
    target = next((im for im in (res or {}).get("images") or [] if im["id"] == iid), None)
    if target and cfg()["image"] in image_refs(target):
        raise HTTPException(409, "that is the image of the chosen model")
    rc, res, err = ssh(host, f"remove-image {iid}", timeout=320)
    if not (res and res.get("rc") == 0):
        raise HTTPException(409, redact(str((res or {}).get("out") or err))[-300:])
    event(f"{host}: removed image {iid[7:19]}")
    return {"ok": True}


@app.post("/api/credentials", dependencies=[Depends(auth)])
async def api_credentials(request: Request):
    """The Hugging Face token, set from the page. It is checked against Hugging Face before it
    is saved; an empty value removes it. HF_TOKEN in env wins and can't be changed here."""
    b = await request.json()
    tok = str(b.get("hf_token") or "").strip()
    if HF_TOKEN:
        raise HTTPException(409, f"HF_TOKEN is set in the app's env, which wins. Change it in {ENV_WHERE}.")
    if tok:
        if not re.match(r"^hf_[A-Za-z0-9]{20,}$", tok):
            raise HTTPException(400, "that does not look like a Hugging Face token (they start with hf_)")
        code, _ = http_get("https://huggingface.co/api/whoami-v2", {"Authorization": f"Bearer {tok}"}, timeout=15)
        if code == 401:
            raise HTTPException(400, "Hugging Face rejected the token. Copy it again from huggingface.co/settings/tokens")
        if code != 200:
            raise HTTPException(502, f"could not check the token with Hugging Face (HTTP {code or 'no answer'}); try again")
    save_creds({"hf_token": tok})
    _hf["info"] = None
    event("Hugging Face token " + ("saved" if tok else "removed") + " under Settings")
    return {"ok": True, "hf": hf_status()}


@app.post("/api/password")
async def api_password(request: Request, user: str = Depends(auth)):
    """Change the admin password. Everyone else is logged out; this browser stays logged in."""
    b = await request.json()
    cur, new = str(b.get("current") or ""), str(b.get("new") or "")
    if not password_ok(cur):
        _login_fails.append(now())
        raise HTTPException(403, "the current password is wrong")
    if len(new) < 10:
        raise HTTPException(400, "the new password needs at least 10 characters")
    if new == cur:
        raise HTTPException(400, "the new password is the same as the current one")
    save_creds({"admin_password": hash_password(new)})
    event("admin password changed under Settings; other sessions are logged out")
    r = JSONResponse({"ok": True})
    https = request.headers.get("x-forwarded-proto", request.url.scheme) == "https"
    r.set_cookie(SESSION_COOKIE, make_session(user), max_age=SESSION_DAYS * 86400,
                 httponly=True, samesite="lax", secure=https)
    return r


@app.get("/logout")
def logout():
    r = RedirectResponse("login", 303)
    r.delete_cookie(SESSION_COOKIE)
    return r


@app.get("/favicon.svg")
@app.get("/favicon.ico")
def favicon():
    return FileResponse(os.path.join(os.path.dirname(__file__), "static/favicon.svg"), media_type="image/svg+xml")


@app.get("/healthz")
def healthz():
    return {"ok": True}


@app.get("/", dependencies=[Depends(auth)])
def index():
    return FileResponse(os.path.join(os.path.dirname(__file__), "static/index.html"))


@app.get("/api/status")
def api_status(user: str = Depends(auth)):
    return {**STATUS, "action": dict(CURRENT_ACTION), "events": events(60),
            "config": {
                "head": HEAD_HOST, "workers": WORKER_HOSTS, "ssh_user": SSH_USER,
                "nodes_source": "page" if load_settings().get("nodes") else "env",
                "cluster_dir": CLUSTER_DIR, "mode": CONFIG_MODE,
                "served_model_name": cfg()["served_model_name"], "port": VLLM_PORT,
                **{k: v for k, v in ops().items() if k != "locked"}, "ops_locked": ops()["locked"],
                "public_url": load_settings().get("public_url", ""),
                "guide": load_settings().get("guide") or {},
                "alert_url": load_settings().get("alert_url") or "",
                "nodes": NODES, "roce_fix": roce_fix(STATUS["cluster"].get("roce_bad") or []),
                "state": load_state(),
                "model": cfg(), "patchsets": list(PATCHSETS), "hf_offline": HF_OFFLINE,
                "version": APP_VERSION, "agent_version": AGENT_VERSION, "user": user,
                "password_source": "settings" if password_hash() else "env"},
            "pubkey": pubkey()}


@app.post("/api/action/{name}", dependencies=[Depends(auth)])
def api_action(name: str):
    if name not in ACTIONS:
        raise HTTPException(404, "unknown action")
    if name == "reboot" and not ops()["allow_reboot"]:
        raise HTTPException(403, "the reboot button is off under Settings")
    start_action(name)
    return {"started": name}


@app.get("/api/models", dependencies=[Depends(auth)])
def api_models():
    return {"catalog": list(CATALOG.values()), "current": cfg(), "settings": load_settings(),
            "nodes": len(NODES)}


@app.post("/api/model", dependencies=[Depends(auth)])
async def api_model(request: Request):
    """Choose a model. It takes effect on the next Start or Restart, like any config change."""
    b = await request.json()
    st = {"id": b.get("id")}
    if st["id"] == "custom":
        cu = b.get("custom") or {}
        if not re.match(r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$", cu.get("model", "")):
            raise HTTPException(400, "the model must be a Hugging Face repo, like org/name")
        st["custom"] = {"model": cu["model"],
                        "name": cu["model"],
                        "served_model_name": re.sub(r"[^A-Za-z0-9_.-]", "", cu.get("served_model_name") or "")
                        or cu["model"].split("/")[-1].lower(),
                        "vllm_args": str(cu.get("vllm_args") or "")[:2000]}
        min_nodes = 1
    elif st["id"] in CATALOG:
        min_nodes = CATALOG[st["id"]].get("min_nodes", 1)
    else:
        raise HTTPException(400, "unknown model")
    if len(NODES) < min_nodes:
        raise HTTPException(400, f"this model needs at least {min_nodes} nodes")
    try:
        if b.get("gpu_mem_util"):
            g = float(b["gpu_mem_util"])
            assert 0.3 <= g <= 0.95
            st["gpu_mem_util"] = f"{g:.2f}"
        if b.get("max_model_len"):
            n = int(b["max_model_len"])
            assert 1024 <= n <= 1048576
            st["max_model_len"] = str(n)
    except (ValueError, AssertionError):
        raise HTTPException(400, "GPU memory must be 0.30–0.95 and context 1024–1048576 tokens")
    save_settings({"custom": None, "gpu_mem_util": None, "max_model_len": None, **st})
    c = cfg()
    event(f"model set to {c['model']}" + (f" (env overrides: {', '.join(c['locked'])})" if c["locked"] else "")
          + " — download it, then Restart to apply")
    return {"ok": True, "current": c}


@app.get("/api/usage", dependencies=[Depends(auth)])
def api_usage(days: int = 90):
    """Hourly rows; the page groups them into days and weeks in the viewer's time zone."""
    since = int(now() - max(1, min(days, 400)) * 86400)
    db = stats_db()
    rows = db.execute("""SELECT hour, model, prompt, cached, gen, requests, gpu_wh, extra_wh
        FROM hourly WHERE hour >= ? ORDER BY hour""", (since,)).fetchall()
    db.close()
    keys = ("hour", "model", "prompt", "cached", "gen", "requests", "gpu_wh", "extra_wh")
    return {"rows": [dict(zip(keys, r)) for r in rows], "extra_watts": ops()["extra_watts"],
            "nodes": len(NODES)}


_latest = {"ts": 0, "version": None}


def latest_version():
    """APP_VERSION on the repo's main branch, checked at most once an hour."""
    if not GITHUB_REPO or now() - _latest["ts"] < 3600:
        return _latest["version"]
    _latest["ts"] = now()
    code, body = http_get(f"https://raw.githubusercontent.com/{GITHUB_REPO}/main/app/main.py", timeout=10)
    m = re.search(r'^APP_VERSION = "([0-9.]+)"', body or "", re.M) if code == 200 else None
    if m:
        _latest["version"] = m.group(1)
    return _latest["version"]


def newer(a, b):
    try:
        return tuple(map(int, a.split("."))) > tuple(map(int, b.split(".")))
    except (AttributeError, ValueError):
        return False


@app.get("/api/version", dependencies=[Depends(auth)])
def api_version():
    latest = latest_version()
    return {"version": APP_VERSION, "latest": latest, "update": newer(latest, APP_VERSION),
            "repo": f"https://github.com/{GITHUB_REPO}" if GITHUB_REPO else None,
            "update_hint": UPDATE_HINT or "redeploy the app in Easypanel"}


@app.get("/api/apikey", dependencies=[Depends(auth)])
def api_apikey():
    return {"key": API_KEY}


@app.post("/api/settings", dependencies=[Depends(auth)])
async def api_settings(request: Request):
    """Operational settings and the public URL. They apply at once, no restart."""
    b = await request.json()
    upd = {}
    if "public_url" in b:
        u = str(b["public_url"] or "").strip().rstrip("/")
        if u and not re.match(r"^https?://[A-Za-z0-9.:/_-]+$", u):
            raise HTTPException(400, "the public URL must start with http:// or https://")
        upd["public_url"] = u or None
    if "alert_url" in b:
        u = str(b["alert_url"] or "").strip()
        if u and not re.match(r"^https?://[^\s]+$", u):
            raise HTTPException(400, "the alert webhook must start with http:// or https://")
        upd["alert_url"] = u or None
    if "guide" in b:  # the getting-started guide on Overview: hidden or not
        upd["guide"] = {**(load_settings().get("guide") or {}), "hidden": bool((b["guide"] or {}).get("hidden"))}
    if "ops" in b:
        saved = dict(load_settings().get("ops", {}))
        for k, v in (b["ops"] or {}).items():
            if k not in OPS:
                raise HTTPException(400, f"unknown setting {k}")
            e, t, d, *lim = OPS[k]
            try:
                v = conv(t, v)
                assert not lim or lim[0] <= v <= lim[1]
            except (ValueError, AssertionError):
                raise HTTPException(400, f"{k} must be between {lim[0]} and {lim[1]}")
            saved[k] = v
        upd["ops"] = saved
    save_settings(upd)
    event("settings changed: " + ", ".join(
        [f"{k}={v}" for k, v in (b.get("ops") or {}).items()] + (["public URL"] if "public_url" in b else [])
        + (["getting-started guide " + ("hidden" if upd["guide"]["hidden"] else "shown")] if "guide" in b else [])
        + (["alert webhook"] if "alert_url" in b else [])))
    return {"ok": True, "ops": ops()}


@app.post("/api/delete-model", dependencies=[Depends(auth)])
async def api_delete_model(request: Request):
    repo = (await request.json()).get("repo", "")
    if not re.match(r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$", repo):
        raise HTTPException(400, "invalid repo")
    c = cfg()
    if repo == c["model"]:
        raise HTTPException(400, "that is the chosen model — pick another one first")
    if CURRENT_ACTION["name"]:
        raise HTTPException(409, f"{CURRENT_ACTION['name']} is running")
    image = c["image"] or next((m["image"] for m in CATALOG.values()), "")
    cmd = f"delete-model {shlex.quote(image)} {shlex.quote(HF_CACHE_DIR)} {repo}"
    with ThreadPoolExecutor(max_workers=max(1, len(NODES))) as ex:
        res = list(ex.map(lambda n: (n, ssh(n["host"], cmd, timeout=900)), NODES))
    failed = []
    for n, (rc, r, err) in res:
        good = rc == 0 and r and r.get("rc") == 0
        gone = "not on disk" in str((r or {}).get("out"))
        event(f"{n['role']} {n['host']}: {repo} was not on disk" if good and gone
              else f"{n['role']} {n['host']}: deleted {repo}" if good
              else f"{n['role']} {n['host']}: could not delete {repo} ({redact(str((r or {}).get('out') or err))[-200:]})",
              "info" if good else "error")
        failed += [] if good else [n["host"]]
    if failed:
        raise HTTPException(502, f"could not delete on {', '.join(failed)} — see Events")
    return {"ok": True}


@app.post("/api/test", dependencies=[Depends(auth)])
async def api_test(request: Request):
    body = await request.json() if request.headers.get("content-type") == "application/json" else {}
    prompt = (body.get("prompt") or "What is 17*23? Answer with the number only.")[:2000]
    model = (STATUS["cluster"].get("models") or [cfg()["served_model_name"]])[0]
    req = urllib.request.Request(
        f"http://{HEAD_HOST}:{VLLM_PORT}/v1/chat/completions",
        data=json.dumps({"model": model, "max_tokens": 512,
                         "messages": [{"role": "user", "content": prompt}]}).encode(),
        headers={"Authorization": f"Bearer {API_KEY}", "Content-Type": "application/json"})
    t0 = now()
    try:
        with urllib.request.urlopen(req, timeout=120) as r:
            d = json.loads(r.read())
    except Exception as e:  # noqa: BLE001
        return JSONResponse({"ok": False, "error": redact(str(e))}, 502)
    msg = d["choices"][0]["message"]
    if not (load_settings().get("guide") or {}).get("tested"):
        save_settings({"guide": {**(load_settings().get("guide") or {}), "tested": True}})
    return {"ok": True, "seconds": round(now() - t0, 2), "answer": msg.get("content"),
            "reasoning": (msg.get("reasoning_content") or msg.get("reasoning") or "")[:4000],
            "usage": d.get("usage")}


@app.get("/logs/{idx}", dependencies=[Depends(auth)])
def logs_page(idx: int):
    return FileResponse(os.path.join(os.path.dirname(__file__), "static/logs.html"))


@app.get("/api/logs/{idx}", dependencies=[Depends(auth)])
def api_logs(idx: int, n: int = 500, since: str = ""):
    if not 0 <= idx < len(NODES):
        raise HTTPException(404)
    if since and not re.match(r"^[0-9T:.\-Z+]+$", since):
        raise HTTPException(400, "invalid since")
    rc, res, err = ssh(NODES[idx]["host"], f"logs {max(1, min(n, 5000))} {since}".strip(), timeout=60)
    if res is None or "error" in res:
        return JSONResponse({"error": redact(str((res or {}).get("error") or err))[-500:]}, 502)
    return {**NODES[idx], "idx": idx, "lines": [redact(l) for l in res.get("lines", [])]}


@app.get("/api/preview", dependencies=[Depends(auth)])
def api_preview():
    return {"mode": CONFIG_MODE, "nodes": preview()}


@app.get("/api/install", dependencies=[Depends(auth)])
def api_install():
    """Commands to paste on each node (installs agent + key)."""
    agent = open(os.path.join(os.path.dirname(__file__), "vllmapp-agent")).read()
    b64 = base64.b64encode(agent.encode()).decode()
    script = f"""# Run as {SSH_USER} on each node ({', '.join(n['host'] for n in NODES)})
mkdir -p ~/.local/bin ~/.config/vllmapp ~/.ssh && chmod 700 ~/.ssh
echo '{b64}' | base64 -d > ~/.local/bin/vllmapp-agent && chmod 755 ~/.local/bin/vllmapp-agent
[ -f ~/.config/vllmapp/agent.env ] || echo 'CLUSTER_DIR={CLUSTER_DIR}' > ~/.config/vllmapp/agent.env
touch ~/.ssh/authorized_keys && chmod 600 ~/.ssh/authorized_keys
sed -i '/ vllmapp$/d' ~/.ssh/authorized_keys
printf 'command="%s/.local/bin/vllmapp-agent",restrict %s\\n' "$HOME" '{pubkey()}' >> ~/.ssh/authorized_keys

# Optional: lets the app reboot the node and free the page cache before a start. Only these two commands.
printf '%s\n' "$USER ALL=(root) NOPASSWD: /usr/bin/systemctl reboot" "$USER ALL=(root) NOPASSWD: /usr/bin/tee /proc/sys/vm/drop_caches" | sudo tee /etc/sudoers.d/vllmapp >/dev/null && sudo chmod 440 /etc/sudoers.d/vllmapp && sudo visudo -cf /etc/sudoers.d/vllmapp
"""
    return PlainTextResponse(script)


@app.on_event("startup")
def startup():
    ensure_key()
    if not NODES:
        event("HEAD_HOST is not set — nothing to monitor", "error")
        return
    event(f"vLLM Panel started: head {HEAD_HOST}, workers {WORKER_HOSTS or '–'}, "
          f"mode {CONFIG_MODE}, auto-recover {'on' if ops()['auto_recover'] else 'off'}")
    threading.Thread(target=poller, daemon=True).start()
