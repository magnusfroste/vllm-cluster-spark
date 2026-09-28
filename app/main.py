"""vllmapp — status and control of a vLLM/Ray cluster on DGX Spark.

Configuration comes from the environment (Easypanel's env tab). The nodes are reached
over SSH with a dedicated key that may only run vllmapp-agent (see agent/).
"""
import base64
import difflib
import json
import os
import re
import secrets
import shlex
import subprocess
import threading
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse, PlainTextResponse


def env(name, default=""):
    return os.environ.get(name, default).strip()


def envbool(name, default=False):
    return env(name, "true" if default else "false").lower() in ("1", "true", "yes", "ja")


# ---------- configuration ----------
HEAD_HOST = env("HEAD_HOST")
WORKER_HOSTS = [h.strip() for h in env("WORKER_HOSTS").split(",") if h.strip()]
SSH_USER = env("SSH_USER", "root")
SSH_PORT = env("SSH_PORT", "22")
CLUSTER_DIR = env("CLUSTER_DIR", "~/vllm-cluster")
CONFIG_MODE = env("CONFIG_MODE", "managed")  # managed | existing
VLLM_PORT = env("VLLM_PORT", "8000")

VLLM_IMAGE = env("VLLM_IMAGE")
MODEL = env("MODEL")
SERVED_MODEL_NAME = env("SERVED_MODEL_NAME")
TP_SIZE = env("TP_SIZE")
GPU_MEM_UTIL = env("GPU_MEM_UTIL")
MAX_MODEL_LEN = env("MAX_MODEL_LEN")
VLLM_EXTRA_ARGS = env("VLLM_EXTRA_ARGS")
API_KEY = env("API_KEY")
HF_TOKEN = env("HF_TOKEN")
HF_CACHE_DIR = env("HF_CACHE_DIR", "${HOME}/.cache/huggingface")
EXTRA_MOUNTS = [m.strip() for m in env("EXTRA_MOUNTS").split(",") if m.strip()]
IF_NAMES = env("IF_NAMES")  # optional override, ;-separated in node order
IB_HCAS = env("IB_HCAS")
PATCHES = env("PATCHES", "auto")  # auto (by MODEL) | none | name of a folder in patchsets/
MODEL_REVISION = env("MODEL_REVISION")
HF_OFFLINE = envbool("HF_OFFLINE", True)  # vLLM never downloads; the app's download does

ADMIN_USER = env("ADMIN_USER", "admin")
ADMIN_PASSWORD = env("ADMIN_PASSWORD")
PORT = int(env("PORT", "8080"))
AUTO_RECOVER = envbool("AUTO_RECOVER")
HANG_TIMEOUT_MIN = float(env("HANG_TIMEOUT_MIN", "40"))
STALL_TIMEOUT_MIN = float(env("STALL_TIMEOUT_MIN", "5"))  # startup with no new log line
UNHEALTHY_GRACE_MIN = float(env("UNHEALTHY_GRACE_MIN", "3"))  # was ready, stopped answering
MEM_WARN_GIB = float(env("MEM_WARN_GIB", "4"))
MAX_AUTO_RESTARTS = int(env("MAX_AUTO_RESTARTS", "1"))
ALLOW_REBOOT = envbool("ALLOW_REBOOT")
AUTO_REBOOT = envbool("AUTO_REBOOT")
POLL_SECONDS = float(env("POLL_SECONDS", "15"))

DATA = env("DATA_DIR", "/data")
KEY = os.path.join(DATA, "id_ed25519")
STATE_FILE = os.path.join(DATA, "state.json")
EVENTS_FILE = os.path.join(DATA, "events.log")

NODES = ([{"role": "head", "host": HEAD_HOST}] if HEAD_HOST else []) + \
        [{"role": "worker", "host": h} for h in WORKER_HOSTS]

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
PATCHSET = (next((n for n, ps in PATCHSETS.items() if MODEL in ps.get("models", [])), None)
            if PATCHES == "auto" else None if PATCHES in ("", "none") else PATCHES)
REVISION = MODEL_REVISION or (PATCHSETS.get(PATCHSET) or {}).get("revision", "")

SECRETS = [s for s in (API_KEY, HF_TOKEN, ADMIN_PASSWORD) if len(s) >= 6]


def redact(text):
    if not text:
        return text
    for s in SECRETS:
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


def ssh(host, cmd, timeout=60, input=None):
    """Run an agent command on the node. Returns (rc, parsed json | None, raw text)."""
    argv = ["ssh", "-i", KEY, "-p", SSH_PORT,
            "-o", "BatchMode=yes", "-o", "ConnectTimeout=5",
            "-o", "StrictHostKeyChecking=accept-new",
            "-o", f"UserKnownHostsFile={DATA}/known_hosts",
            "-o", "ServerAliveInterval=15",
            f"{SSH_USER}@{host}", cmd]
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
    (re.compile(r"Alla noder uppe|All nodes up"), "starting vLLM"),
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
    args = f" {n['host']} {HEAD_HOST} {shlex.quote(HF_CACHE_DIR)} {MODEL or '-'} {REVISION}"
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
    models = []
    if healthy:
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
                for n in nodes for r in (n.get("roce") or []) if not r["ok"]]

    if healthy:
        state = "ready"
    elif not head.get("reachable"):
        state = "head unreachable"
    elif not hc:
        state = "no container"
    elif not hc.get("running"):
        state = "stopped"
    elif phase == "ready":
        state = "not responding"
    else:
        state = "starting"

    global UNHEALTHY_SINCE
    UNHEALTHY_SINCE = None if state != "not responding" else (UNHEALTHY_SINCE or now())
    hung = None
    if state == "starting" and uptime is not None:
        if uptime > HANG_TIMEOUT_MIN * 60:
            hung = f"startup has run for {dur(uptime)} without responding"
        elif errs and uptime > 180:
            hung = f"startup shows {len(errs)} error line(s)"
        elif stall and stall > STALL_TIMEOUT_MIN * 60 and uptime > STALL_TIMEOUT_MIN * 60:
            hung = f"no new log line for {dur(stall)} (phase: {phase or 'unknown'})"
    elif state == "not responding" and now() - UNHEALTHY_SINCE > UNHEALTHY_GRACE_MIN * 60:
        hung = f"was ready, has not answered /health for {dur(now() - UNHEALTHY_SINCE)}"

    STATUS.update({
        "updated": now(),
        "nodes": nodes,
        "cluster": {
            "state": state,
            "phase": "ready" if healthy else phase,
            "phase_pct": pct,
            "healthy": healthy,
            "models": models,
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
                        if (n.get("mem") or {}).get("available", 1 << 62) < MEM_WARN_GIB * 2**30],
            "image_mismatch": len(image_ids) > 1,
            "workers_down": [n["host"] for n in nodes[1:]
                             if not (n.get("container") or {}).get("running")],
        },
        "setup": setup_summary(nodes),
        "action": dict(CURRENT_ACTION),
    })
    return STATUS


_model_size = {}


def model_size():
    """Total size of the model repo from the HF API (cached), for download progress and disk check."""
    if not MODEL:
        return None
    size, checked = _model_size.get("v"), _model_size.get("ts", 0)
    if size is None and now() - checked > 600:  # a failed lookup is retried every 10 min
        _model_size["ts"] = now()
        hdr = {"Authorization": f"Bearer {HF_TOKEN}"} if HF_TOKEN else {}
        code, body = http_get(f"https://huggingface.co/api/models/{MODEL}/revision/"
                              f"{REVISION or 'main'}?blobs=true", hdr, timeout=15)
        try:
            if code == 200:
                size = _model_size["v"] = sum(f.get("size") or 0 for f in json.loads(body)["siblings"])
        except (ValueError, KeyError):
            pass
    return size


AGENT_VERSION = re.search(r'^VERSION = "(\d+)"', open(os.path.join(os.path.dirname(__file__),
                          "vllmapp-agent")).read(), re.M).group(1)


def setup_summary(nodes):
    """What still needs doing before the cluster can start: root fixes per node, env, model."""
    size = model_size()
    missing = [k for k, v in (("HEAD_HOST", HEAD_HOST), ("API_KEY", API_KEY),
                              ("MODEL", MODEL), ("VLLM_IMAGE", VLLM_IMAGE))
               if not v and (CONFIG_MODE == "managed" or k in ("HEAD_HOST", "API_KEY"))]
    per = []
    for n in nodes:
        m = n.get("model") or {}
        dl = m.get("download") or {}
        checks = dict(n.get("checks") or {})
        if "disk_free" in checks and size and not m.get("present"):
            need = size - m.get("bytes", 0)
            checks["disk_free"] = {**checks["disk_free"], "ok": checks["disk_free"]["bytes"] > need * 1.05,
                                   "need": need}
        per.append({"role": n["role"], "host": n["host"], "reachable": n.get("reachable"),
                    "agent": n.get("agent"), "agent_old": n.get("reachable") and n.get("agent") != AGENT_VERSION,
                    "checks": checks, "model": m or None,
                    "downloading": bool(dl.get("running")),
                    "download_failed": bool(dl) and not dl.get("running") and dl.get("exit_code") != 0})
    return {"missing_env": missing, "model": MODEL, "revision": REVISION, "size": size,
            "patchset": PATCHSET, "patchset_missing": bool(PATCHSET) and PATCHSET not in PATCHSETS,
            "agent_version": AGENT_VERSION, "nodes": per,
            "model_ready": bool(MODEL) and all((p["model"] or {}).get("present") for p in per)}


def auto_recover():
    c = STATUS["cluster"]
    s = load_state()
    if c.get("healthy"):
        if s["attempts"]:
            event(f"cluster responding again after {s['attempts']} automatic action(s)", "ok")
            s["attempts"] = 0
            save_state(s)
        return
    if not AUTO_RECOVER or not c.get("hung") or CURRENT_ACTION["name"]:
        return
    if now() - s["last_action"] < max(STALL_TIMEOUT_MIN, 5) * 60:
        return
    if c.get("roce_bad"):
        if s.get("gave_up", 0) <= s["last_action"]:
            event(f"cluster is hung and RoCE GID 3 is missing ({roce_text(c['roce_bad'])}) "
                  "— a restart won't help, the link needs resetting (sudo)", "error")
            s["gave_up"] = now()
            save_state(s)
        return
    if s["attempts"] < MAX_AUTO_RESTARTS:
        event(f"hang detected: {c.get('hung_reason')} — restarting the cluster automatically", "warn")
        start_action("restart", auto=True)
    elif ALLOW_REBOOT and AUTO_REBOOT and now() - s["last_reboot"] > 6 * 3600:
        event("restart did not help — rebooting the nodes automatically", "warn")
        start_action("reboot", auto=True)
    elif s.get("gave_up", 0) <= s["last_action"]:
        event("cluster is hung and automation has given up — needs a human", "error")
        s["gave_up"] = now()
        save_state(s)


def poller():
    while True:
        try:
            poll_once()
            auto_recover()
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
    """RoCE GID 3 must exist on every node, otherwise NCCL init fails with 'unhandled system error'."""
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
    if CONFIG_MODE != "managed" or not MODEL:
        return
    s = STATUS.get("setup") or {}
    bad = [p["host"] for p in s.get("nodes", []) if p.get("model") and not p["model"].get("present")]
    if bad:
        raise RuntimeError(f"{MODEL} is not fully downloaded on {', '.join(bad)} — use Download model first")


def do_start():
    model_check()
    preflight()
    write_config()
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
    on_nodes(NODES[1:], "up")
    on_nodes(NODES[:1], "up")


def do_reboot():
    if not ALLOW_REBOOT:
        raise RuntimeError("ALLOW_REBOOT is off")
    s = load_state()
    s["last_reboot"] = now()
    save_state(s)
    on_nodes(NODES[1:], "reboot", timeout=30)
    time.sleep(5)
    on_nodes(NODES[:1], "reboot", timeout=30)  # the app goes down with the head if it runs there


def do_pull():
    with ThreadPoolExecutor(max_workers=max(1, len(NODES))) as ex:
        list(ex.map(lambda n: on_nodes([n], "pull", timeout=3600), NODES))


def write_config():
    """Managed mode: write the config from env to every node before the cluster starts.
    An app deploy never gets here, so new env only takes effect on Start/Restart."""
    if CONFIG_MODE != "managed":
        return
    failed = False
    for i, n in enumerate(NODES):
        for name, content in render(i, n).items():
            rc, res, err = ssh(n["host"], f"put {shlex.quote(name)}", input=content, timeout=30)
            good = rc == 0 and res and res.get("rc") == 0
            event(f"{n['role']} {n['host']}: wrote {name}"
                  + (" (unchanged)" if good and not res.get("changed") else "")
                  + ("" if good else f" — FAILED {redact(str(res or err))[-200:]}"),
                  "info" if good else "error")
            failed |= not good
    if failed:
        raise RuntimeError("could not write the config to every node — cluster not started")


def do_download():
    if not (MODEL and VLLM_IMAGE):
        raise RuntimeError("MODEL and VLLM_IMAGE must be set")
    cmd = f"download {shlex.quote(VLLM_IMAGE)} {MODEL} {shlex.quote(HF_CACHE_DIR)} {REVISION}".rstrip()

    def one(n):
        rc, res, err = ssh(n["host"], cmd, timeout=3600, input=HF_TOKEN)
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
COMPOSE = """# Generated by vllmapp — change it in Easypanel's env, not here
services:
  vllm:
    image: ${VLLM_IMAGE}
    container_name: vllm-${ROLE}
    restart: unless-stopped
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
# Generated by vllmapp. ROLE=head: Ray head + vLLM (:8000). ROLE=worker: joins and blocks.
set -euo pipefail

# Run the FlashInfer autotune from scratch on every start. With a saved cache the
# ranks hit it differently and fall out of step (gloo waiting on one, NCCL on the
# other) until the 30 min timeout. Without a cache the tuning takes ~1 min.
rm -rf /root/.cache/vllm/flashinfer_autotune_cache

if [ "$ROLE" = "worker" ]; then
  until ray start --block --address="${HEAD_IP}:6379" --node-ip-address="${HOST_IP}"; do
    echo "Waiting for head ${HEAD_IP}..."; sleep 5
  done
  exit 0
fi

ray start --head --node-ip-address="${HOST_IP}" --port=6379 --dashboard-host=127.0.0.1
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


def vllm_args():
    a = []
    if SERVED_MODEL_NAME:
        a += ["--served-model-name", SERVED_MODEL_NAME]
    if MAX_MODEL_LEN:
        a += ["--max-model-len", MAX_MODEL_LEN]
    if GPU_MEM_UTIL:
        a += ["--gpu-memory-utilization", GPU_MEM_UTIL]
    if REVISION and "--revision" not in VLLM_EXTRA_ARGS:
        a += ["--revision", REVISION]
    return " ".join(a + shlex.split(VLLM_EXTRA_ARGS))


def render(i, n):
    iface, hca = net_for(i, n)
    envfile = "\n".join([
        "# Generated by vllmapp — change it in Easypanel's env, not here",
        f"ROLE={n['role']}",
        f"VLLM_IMAGE={VLLM_IMAGE}",
        f"HOST_IP={n['host']}",
        f"HEAD_IP={HEAD_HOST}",
        f"IF_NAME={iface}",
        f"IB_HCA={hca}",
        f"NUM_NODES={len(NODES)}",
        f"TP_SIZE={TP_SIZE or len(NODES)}",
        f"MODEL={MODEL}",
        f"VLLM_EXTRA_ARGS={vllm_args()}",
        f"VLLM_API_KEY={API_KEY}",
        f"HF_TOKEN={HF_TOKEN}",
    ]) + "\n"
    mounts = [m if m.count(":") >= 2 else m + ":ro" for m in EXTRA_MOUNTS]
    files = {".env": envfile}
    ps = PATCHSETS.get(PATCHSET)
    if ps:
        for name, dst in ps["mounts"].items():
            files[f"patches/{name}"] = open(os.path.join(PATCHSET_DIR, PATCHSET, name)).read()
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
                f"{n['host']}:{name} (current)", f"{n['host']}:{name} (from env)"))
            files.append({"name": name, "exists": (res or {}).get("exists", False),
                          "diff": diff, "rendered": redact(new)})
        out.append({**n, "files": files})
    return out


# ---------- web ----------
app = FastAPI(title="vllmapp")
def auth(request: Request):
    """Basic auth with UTF-8 (FastAPI's HTTPBasic decodes ASCII, so non-ASCII passwords always got 401)."""
    if not ADMIN_PASSWORD:
        raise HTTPException(503, "ADMIN_PASSWORD is not set — set it in Easypanel's env")
    user = password = None
    scheme, _, param = request.headers.get("authorization", "").partition(" ")
    if scheme.lower() == "basic":
        raw = base64.b64decode(param + "===")
        for enc in ("utf-8", "latin-1"):
            try:
                user, _, password = raw.decode(enc).partition(":")
                break
            except UnicodeDecodeError:
                continue
    ok = user is not None and \
        secrets.compare_digest(user.encode(), ADMIN_USER.encode()) and \
        secrets.compare_digest(password.encode(), ADMIN_PASSWORD.encode())
    if not ok:
        raise HTTPException(401, "invalid credentials", headers={"WWW-Authenticate": 'Basic realm="vllmapp", charset="UTF-8"'})
    return user


@app.get("/healthz")
def healthz():
    return {"ok": True}


@app.get("/", dependencies=[Depends(auth)])
def index():
    return FileResponse(os.path.join(os.path.dirname(__file__), "static/index.html"))


@app.get("/api/status", dependencies=[Depends(auth)])
def api_status():
    return {**STATUS, "action": dict(CURRENT_ACTION), "events": events(60),
            "config": {
                "head": HEAD_HOST, "workers": WORKER_HOSTS, "ssh_user": SSH_USER,
                "cluster_dir": CLUSTER_DIR, "mode": CONFIG_MODE,
                "served_model_name": SERVED_MODEL_NAME, "port": VLLM_PORT,
                "auto_recover": AUTO_RECOVER, "hang_timeout_min": HANG_TIMEOUT_MIN,
                "max_auto_restarts": MAX_AUTO_RESTARTS, "allow_reboot": ALLOW_REBOOT,
                "stall_timeout_min": STALL_TIMEOUT_MIN, "unhealthy_grace_min": UNHEALTHY_GRACE_MIN,
                "nodes": NODES, "roce_fix": roce_fix(STATUS["cluster"].get("roce_bad") or []),
                "auto_reboot": AUTO_REBOOT, "state": load_state(),
                "model": MODEL, "revision": REVISION, "image": VLLM_IMAGE,
                "patchset": PATCHSET, "patchsets": list(PATCHSETS), "hf_offline": HF_OFFLINE},
            "pubkey": pubkey()}


@app.post("/api/action/{name}", dependencies=[Depends(auth)])
def api_action(name: str):
    if name not in ACTIONS:
        raise HTTPException(404, "unknown action")
    if name == "reboot" and not ALLOW_REBOOT:
        raise HTTPException(403, "ALLOW_REBOOT is off")
    start_action(name)
    return {"started": name}


@app.post("/api/test", dependencies=[Depends(auth)])
async def api_test(request: Request):
    body = await request.json() if request.headers.get("content-type") == "application/json" else {}
    prompt = (body.get("prompt") or "What is 17*23? Answer with the number only.")[:2000]
    model = SERVED_MODEL_NAME or (STATUS["cluster"].get("models") or [""])[0]
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

# Optional, for the reboot button (passwordless reboot, nothing else):
echo "$USER ALL=(root) NOPASSWD: /usr/bin/systemctl reboot" | sudo tee /etc/sudoers.d/vllmapp-reboot >/dev/null && sudo chmod 440 /etc/sudoers.d/vllmapp-reboot
"""
    return PlainTextResponse(script)


@app.on_event("startup")
def startup():
    ensure_key()
    if not NODES:
        event("HEAD_HOST is not set — nothing to monitor", "error")
        return
    event(f"vllmapp started: head {HEAD_HOST}, workers {WORKER_HOSTS or '–'}, "
          f"mode {CONFIG_MODE}, auto-recover {'on' if AUTO_RECOVER else 'off'}")
    threading.Thread(target=poller, daemon=True).start()
