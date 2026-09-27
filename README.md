# vllmapp

Status and control of a vLLM/Ray cluster on DGX Spark, meant to run as an app in
Easypanel on the head node. Easypanel is only needed on the head; workers need docker,
the nvidia runtime and the app's public SSH key.

- **Status:** health, `/v1/models`, startup phase (loading weights %, KV profiling, autotune,
  CUDA graphs), KV cache and concurrency, error lines since start, GPU, memory, image ID per node
- **Control:** start, stop, restart (head is stopped, workers restarted, head started),
  pull image, reboot the nodes, test prompt. Start and restart first check that RoCE GID 3
  exists on every node, since NCCL init fails without it
- **Logs:** one page per node (`/logs/0`, `/logs/1`, …) that follows the log live, with filter
- **Auto-recover:** if startup logs nothing for `STALL_TIMEOUT_MIN`, has not responded after
  `HANG_TIMEOUT_MIN`, or shows errors after 3 min, or a ready cluster fails `/health` for
  `UNHEALTHY_GRACE_MIN`, the app restarts the cluster (at most `MAX_AUTO_RESTARTS` times), and then reboots
  if `ALLOW_REBOOT` and `AUTO_REBOOT` are on (at most once per 6 h)
- **Managed mode:** the app generates `.env`, `compose.yaml` and `entrypoint.sh` for each node
  from its env and writes them over SSH. Network interface and RoCE HCAs are detected per node.
  `existing` mode (the default) never touches the files, but the preview shows the diff.

## How the app reaches the nodes

The app creates a key pair in `/data` on first start. The key goes into each node's
`authorized_keys` with `command="…/vllmapp-agent",restrict`, so it can only run the agent's
allowlisted commands (`status`, `scan`, `logs`, `up`, `stop`, `restart`, `pull`, `reboot`,
`netdetect`, `read`/`put` of the cluster files). The install command is shown on the page under
**Configuration → Install the agent**.

Reboot needs a sudoers line per node that only allows `systemctl reboot`:

```bash
echo "$USER ALL=(root) NOPASSWD: /usr/bin/systemctl reboot" | sudo tee /etc/sudoers.d/vllmapp-reboot
sudo chmod 440 /etc/sudoers.d/vllmapp-reboot
```

## Env

| Env | Required | Example / default |
|---|---|---|
| `HEAD_HOST` | yes | `192.168.100.1` (cluster link IP) |
| `WORKER_HOSTS` | yes | `192.168.100.2` (comma-separated) |
| `SSH_USER` | yes | `spark1` |
| `ADMIN_PASSWORD` | yes | password for the web page (user `ADMIN_USER`, default `admin`) |
| `API_KEY` | yes | vLLM's API key, for health, models and the test prompt |
| `SERVED_MODEL_NAME` | | `glm-5.3-flash` |
| `CLUSTER_DIR` | | `~/vllm-cluster`, set in the agent's config at install |
| `CONFIG_MODE` | | `existing` or `managed` |
| `AUTO_RECOVER` | | `false` |
| `HANG_TIMEOUT_MIN` | | `40` |
| `STALL_TIMEOUT_MIN` | | `5` — startup counts as hung when no new log line has arrived for this long |
| `UNHEALTHY_GRACE_MIN` | | `3` — a cluster that was ready counts as hung after failing `/health` this long |
| `MEM_WARN_GIB` | | `4` — warn when a node has less free memory than this |
| `MAX_AUTO_RESTARTS` | | `1` |
| `ALLOW_REBOOT` / `AUTO_REBOOT` | | `false` / `false` |
| `VLLM_PORT`, `PORT`, `POLL_SECONDS` | | `8000`, `8080`, `15` (Easypanel sets `PORT=80` itself; leave it) |

Managed mode only: `VLLM_IMAGE` (preferably pinned to a digest), `MODEL`, `TP_SIZE`
(default: number of nodes), `GPU_MEM_UTIL`, `MAX_MODEL_LEN`, `VLLM_EXTRA_ARGS`, `HF_TOKEN`,
`HF_CACHE_DIR`, `EXTRA_MOUNTS` (comma-separated `src:dst`, relative to the cluster dir,
mounted `:ro`), and the optional overrides `IF_NAMES` / `IB_HCAS` (`;`-separated in node order).

Model-specific patches don't go in the env. Put them in `compose.override.yaml` in the cluster
dir on each node, next to `patches/` (see `cluster/compose.override.yaml`). Docker Compose merges
that file into `compose.yaml` by itself, and managed mode never overwrites it. `EXTRA_MOUNTS` is
for the odd single mount.

## Deploy in Easypanel

Create an **App** service (not Compose) in the head node's Easypanel:

- **Source:** GitHub, this repo, branch `main`, build path `/`. The Dockerfile is in the root.
- **Environment:** as in the table above.
- **Mounts:** a bind mount from a host directory (for example `/home/spark1/vllmapp-data`) to
  `/data`. It holds the app's SSH key, so it must survive redeploys: without it the app creates
  a new key that the nodes' `authorized_keys` don't know.
- **Port:** Easypanel sets `PORT=80` and the app listens on it.

Only run one vllmapp instance per cluster, since each one does auto-recover on its own.

The service is reachable in the Easypanel network as `http://<project>_<service>:80`, for example
`http://vllm_vllmapp:80`. No Easypanel domain is needed if a Cloudflare tunnel running inside
Easypanel points there. The host's own `cloudflared` can't resolve Docker service names.

To build and run it outside Easypanel:

```bash
docker build -t vllmapp:latest .
docker run -d --name vllmapp -p 8080:8080 --env-file .env -v ~/vllmapp-data:/data vllmapp:latest
```
