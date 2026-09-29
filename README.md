# vllmapp

Runs a vLLM model across two or more NVIDIA DGX Spark (GB10) nodes with tensor parallelism
over Ray, and gives you one web page to set it up, start it, watch it and fix it. The app runs
in [Easypanel](https://easypanel.io) on the head node and controls every node over SSH.

Once the app is deployed, everything happens from its page:

- **Model:** pick a ready-made model from the catalog, or any model on Hugging Face
- **Setup:** checks each node (Docker, NVIDIA Container Toolkit, cluster link, RoCE, disk space)
  and shows the exact command for anything that needs `sudo`
- **Download:** fetches the model on every node in parallel, with progress
- **Config:** writes `.env`, `compose.yaml`, `entrypoint.sh` and any model patches to every node.
  The network interface and RoCE HCAs are detected per node
- **Control:** start, stop, restart, pull image, reboot, test prompt
- **Status and logs:** startup phase (loading weights %, KV profiling, autotune, CUDA graphs),
  KV cache and concurrency, errors, GPU and memory per node, a live log page per node
- **Auto-recover:** restarts a cluster whose startup hangs or that stops answering, and can
  reboot the nodes as a last resort

## What you need

- Two or more DGX Spark nodes (or OEM GB10 units) with the same OS user on each
- A direct QSFP cable between them (the cluster link). With more than two nodes, a switch
- Internet on every node (wifi or the LAN port), for the image and the model
- [Easypanel](https://easypanel.io/docs) installed on the head node
- A Hugging Face token if the model is gated

## 1. Deploy the app in Easypanel

Create a project and an **App** service (not Compose) in the head node's Easypanel:

- **Source:** GitHub, this repo, branch `main`, build path `/`. The Dockerfile is in the root.
- **Environment:** start with the required values. Everything else has a working default.
  ```env
  HEAD_HOST=192.168.100.1
  WORKER_HOSTS=192.168.100.2
  SSH_USER=youruser
  ADMIN_PASSWORD=choose-one
  API_KEY=choose-a-long-random-key
  HF_TOKEN=hf_...
  ```
  The env only holds what the app can't know by itself: the nodes, passwords and keys. You pick
  the model in the app.
  `HEAD_HOST` and `WORKER_HOSTS` are the IPs the nodes will have **on the cluster link**, not
  on your LAN. Pick them now: any private /24 works, as long as nothing else on the node uses it.
- **Mounts:** a bind mount from a host directory, for example `/home/youruser/vllmapp-data`,
  to `/data`. It holds the app's SSH key and must survive redeploys. Without it the app creates
  a new key that the nodes don't know.
- **Domain / port:** Easypanel sets `PORT=80` and the app listens on it. Add a domain in
  Easypanel, or point a Cloudflare tunnel that runs inside Easypanel at
  `http://<project>_<service>:80`. The host's own `cloudflared` can't resolve Docker service names.

Deploy and open the page. Log in as `admin` with `ADMIN_PASSWORD`.

## 2. Install the agent on every node

Open **Configuration → Install the agent** on the page and paste the command on each node,
head included, as `SSH_USER`. It installs a small agent in `~/.local/bin` and adds the app's
key to `authorized_keys` with a forced command and `restrict`, so the key can only run the
agent's allowlisted commands. Run it again whenever the Setup card says an agent is outdated.

## 3. Pick a model

The **Model** card lists ready-made models with settings that suit DGX Spark: the vLLM image,
the tool and reasoning parsers, context length and GPU memory share. **Verified** means we have
run it on two Sparks and checked the answers. **Untested** means the settings come from the
model card and have not been run here yet. You can also pick any other Hugging Face repo and
give the vLLM arguments yourself. GPU memory share and max context are under **Advanced**.

Press **Use this model**. Nothing happens to a running cluster until you press Restart.

## 4. Work through the Setup card

The **Setup** card shows every node with a checklist. Each item that fails comes with the
command to fix it. These are one-time steps on the node itself, since they need root:

| Check | Typical fix |
|---|---|
| Docker usable by the SSH user | `sudo usermod -aG docker $USER`, then log in again |
| NVIDIA Container Toolkit | preinstalled on DGX OS; otherwise `apt install nvidia-container-toolkit` |
| Cluster link IP | a static IP on the QSFP port with `nmcli` |
| Head reachable over the link | cable, and link IPs in the same subnet |
| Disk space for the model | free space in the HF cache (`HF_CACHE_DIR`) |
| Passwordless reboot (optional) | a sudoers line that only allows `systemctl reboot` |

Then press **Download model**. It runs `hf download` in a container on every node at once and
shows the bytes on disk per node. xet and `hf_transfer` are turned off, since both have hung on
large downloads on DGX Spark. The download continues if you close the page, and a stopped
download resumes where it left off.

## 5. Start and use it

Press **Start**. The app checks that the model is downloaded and that RoCE is up on every node,
writes the config, starts the workers and then the head. Startup for a large model takes
10–20 minutes, and the status card follows the phases. When it says **Responding**, use
**Test the model** with a prompt whose answer you can check.

The line at the top of the page always says what to do next. When the model runs, **Use the
model** shows the API addresses (on your network, and from anywhere once you set a public URL
under Settings), the model name, the key, and ready-to-paste examples for curl, Python and
OpenCode. The API is OpenAI-compatible, so most tools work with those three values.

**Details** has the node status, the models on disk (with Delete, to free space), the settings,
the event log and a preview of the config the app writes.

## Changing the model or the config

Pick another model in the app, download it, and press **Restart**. The page says
**Restart to apply** while the running model differs from the chosen one.

Changes to the env in Easypanel need a deploy. **A deploy only restarts the app, never the
cluster.** Every change takes effect on the next **Start** or **Restart**, which writes the
config to every node first. **Configuration → Preview config** shows the diff against what the
nodes have now.

## Env reference

| Env | Default | Meaning |
|---|---|---|
| `HEAD_HOST` | – | the head's IP on the cluster link (required) |
| `WORKER_HOSTS` | – | workers' link IPs, comma-separated |
| `SSH_USER` | `root` | the OS user on the nodes |
| `SSH_PORT` | `22` | |
| `ADMIN_USER` / `ADMIN_PASSWORD` | `admin` / – | login for the page (password required) |
| `API_KEY` | – | vLLM's API key (required) |
| `HF_TOKEN` | – | for gated models and the download |
| `HF_CACHE_DIR` | `${HOME}/.cache/huggingface` | on the nodes |
| `TP_SIZE` | number of nodes | `--tensor-parallel-size` |
| `PATCHES` | `auto` | `auto` picks the patch set that lists `MODEL`, `none` turns patches off, or a folder name in `app/patchsets/` |
| `HF_OFFLINE` | `true` | vLLM runs with `HF_HUB_OFFLINE=1`, so only the app's download fetches the model |
| `EXTRA_MOUNTS` | – | comma-separated `src:dst`, relative to the cluster dir, mounted `:ro` |
| `IF_NAMES` / `IB_HCAS` | detected | `;`-separated per node, if detection gets it wrong |
| `CLUSTER_DIR` | `~/vllm-cluster` | on the nodes, set in the agent's config at install |
| `CONFIG_MODE` | `managed` | `existing` never writes to the nodes (for a hand-written setup) |
| `VLLM_PORT`, `POLL_SECONDS` | `8000`, `15` | |

Auto-recover, the reboot button, the timeouts and the public URL are set in the app under
**Details → Settings** and apply at once. They can also be set in the env (`AUTO_RECOVER`,
`ALLOW_REBOOT`, `AUTO_REBOOT`, `STALL_TIMEOUT_MIN`, `HANG_TIMEOUT_MIN`, `UNHEALTHY_GRACE_MIN`,
`MAX_AUTO_RESTARTS`, `MEM_WARN_GIB`), and then the env wins and the page shows them as locked.

The model settings are normally chosen in the app. Setting any of these in the env overrides
the app's choice, and the page shows them as locked: `MODEL`, `SERVED_MODEL_NAME`,
`VLLM_IMAGE`, `GPU_MEM_UTIL`, `MAX_MODEL_LEN`, `VLLM_EXTRA_ARGS`, `MODEL_REVISION`.

Only run one vllmapp per cluster, since each one does auto-recover on its own.

## The model catalog

Each ready-made model is a file in `app/models/`:

```json
{
  "id": "qwen3.5-122b-a10b",
  "name": "Qwen3.5-122B-A10B (Unsloth NVFP4)",
  "model": "unsloth/Qwen3.5-122B-A10B-NVFP4",
  "served_model_name": "qwen3.5-122b",
  "image": "ghcr.io/…@sha256:…",
  "vllm_args": "--reasoning-parser qwen3 --enable-auto-tool-choice --tool-call-parser qwen3_coder …",
  "gpu_mem_util": "0.80",
  "max_model_len": "262144",
  "size_gb": 79.1,
  "min_nodes": 2,
  "status": "untested",
  "notes": "Shown on the model's card in the app."
}
```

Optional: `revision` pins a commit, and `patchset` names a folder in `app/patchsets/`. Set
`status` to `verified` once the model has run on real hardware and answered a checkable prompt
correctly, and write what you saw in `notes`. Models under ~100 GB fit on a single Spark, but
the app always runs across all nodes.

## Model patch sets

Some models need changes to the vLLM code in the image. A patch set is a folder in
`app/patchsets/` with the patched files and a `set.json`:

```json
{
  "models": ["org/model"],
  "revision": "commit hash the patches were made for",
  "mounts": {"file.py": "/path/in/the/image/file.py"}
}
```

With `PATCHES=auto` the app picks the set whose `models` contains `MODEL`, writes the files to
`patches/` in the cluster dir on every node and mounts them read-only. `{revision}` in a mount
path is replaced with the set's revision. Patches are tied to one image build, so pin
`VLLM_IMAGE` to the digest the set was made for.

Included: [`glm-5.3-flash-nvfp4`](app/patchsets/glm-5.3-flash-nvfp4/README.md) for
`local-inference-lab/GLM-5.3-Flash-NVFP4-Spark`.

## How the app reaches the nodes

The app creates an SSH key pair in `/data` on first start. The agent on each node only accepts
`status`, `scan`, `logs`, `up`, `stop`, `restart`, `pull`, `reboot`, `netdetect`, `read`/`put`
of the cluster files, `download` and `download-stop`. The key is useless for anything else.

## Troubleshooting

See [docs/recovery.md](docs/recovery.md) for reinstalling a node and for the failures we have
seen: RoCE GID missing after a reboot, autotune hangs, OOM on the head, stuck downloads.

## Running without Easypanel

```bash
docker build -t vllmapp:latest .
docker run -d --name vllmapp -p 8080:8080 --env-file .env -v ~/vllmapp-data:/data vllmapp:latest
```
