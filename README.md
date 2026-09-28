# vllmapp

Runs a vLLM model across two or more NVIDIA DGX Spark (GB10) nodes with tensor parallelism
over Ray, and gives you one web page to set it up, start it, watch it and fix it. The app runs
in [Easypanel](https://easypanel.io) on the head node and controls every node over SSH.

Once the app is deployed, everything happens from its page:

- **Setup:** checks each node (Docker, NVIDIA Container Toolkit, cluster link, RoCE, disk space)
  and shows the exact command for anything that needs `sudo`
- **Model:** downloads the model on every node in parallel, with progress
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
  MODEL=local-inference-lab/GLM-5.3-Flash-NVFP4-Spark
  SERVED_MODEL_NAME=glm-5.3-flash
  VLLM_IMAGE=ghcr.io/spark-arena/dgx-vllm-eugr-nightly-tf5@sha256:1b0be62c1ccb37f738746cd8fbcec73b46f0c283082a6d46aebf9f19b5747563
  HF_TOKEN=hf_...
  ```
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

## 3. Work through the Setup card

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

## 4. Start

Press **Start**. The app checks that the model is downloaded and that RoCE is up on every node,
writes the config, starts the workers and then the head. Startup for a large model takes
10–20 minutes, and the status card follows the phases. When it says **Responding**, use
**Test the model** with a prompt whose answer you can check.

The API is OpenAI-compatible at `http://<HEAD_HOST>:8000/v1`, with `API_KEY` as the bearer token.

## Changing the config

Change the env in Easypanel and deploy. **A deploy only restarts the app, never the cluster.**
New values take effect on the next **Start** or **Restart** in the app, which writes the config
to every node first. **Configuration → Preview config** shows the diff against what the nodes have now.

## Env reference

| Env | Default | Meaning |
|---|---|---|
| `HEAD_HOST` | – | the head's IP on the cluster link (required) |
| `WORKER_HOSTS` | – | workers' link IPs, comma-separated |
| `SSH_USER` | `root` | the OS user on the nodes |
| `SSH_PORT` | `22` | |
| `ADMIN_USER` / `ADMIN_PASSWORD` | `admin` / – | login for the page (password required) |
| `API_KEY` | – | vLLM's API key (required) |
| `MODEL` | – | Hugging Face repo |
| `SERVED_MODEL_NAME` | – | the model name clients use |
| `VLLM_IMAGE` | – | vLLM image, preferably pinned to a digest |
| `HF_TOKEN` | – | for gated models and the download |
| `HF_CACHE_DIR` | `${HOME}/.cache/huggingface` | on the nodes |
| `MODEL_REVISION` | from the patch set, else `main` | pins the model to a commit |
| `GPU_MEM_UTIL` | vLLM's default | `--gpu-memory-utilization` |
| `MAX_MODEL_LEN` | vLLM's default | `--max-model-len` |
| `TP_SIZE` | number of nodes | `--tensor-parallel-size` |
| `VLLM_EXTRA_ARGS` | – | anything else for `vllm serve` |
| `PATCHES` | `auto` | `auto` picks the patch set that lists `MODEL`, `none` turns patches off, or a folder name in `app/patchsets/` |
| `HF_OFFLINE` | `true` | vLLM runs with `HF_HUB_OFFLINE=1`, so only the app's download fetches the model |
| `EXTRA_MOUNTS` | – | comma-separated `src:dst`, relative to the cluster dir, mounted `:ro` |
| `IF_NAMES` / `IB_HCAS` | detected | `;`-separated per node, if detection gets it wrong |
| `CLUSTER_DIR` | `~/vllm-cluster` | on the nodes, set in the agent's config at install |
| `CONFIG_MODE` | `managed` | `existing` never writes to the nodes (for a hand-written setup) |
| `AUTO_RECOVER` | `false` | restart a hung cluster automatically |
| `STALL_TIMEOUT_MIN` | `5` | startup counts as hung after this long without a new log line |
| `HANG_TIMEOUT_MIN` | `40` | … or when it hasn't answered after this long |
| `UNHEALTHY_GRACE_MIN` | `3` | a cluster that was ready counts as hung after failing `/health` this long |
| `MAX_AUTO_RESTARTS` | `1` | automatic restarts before giving up |
| `ALLOW_REBOOT` / `AUTO_REBOOT` | `false` / `false` | the reboot button, and rebooting as the last automatic step (at most once per 6 h) |
| `MEM_WARN_GIB` | `4` | warn when a node has less free memory than this |
| `VLLM_PORT`, `POLL_SECONDS` | `8000`, `15` | |

Only run one vllmapp per cluster, since each one does auto-recover on its own.

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
