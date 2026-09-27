# vllmapp

Status and control of a vLLM/Ray cluster on DGX Spark, meant to run as an app in
Easypanel on the head node. Easypanel is only needed on the head; workers need docker,
the nvidia runtime and the app's public SSH key.

- **Status:** health, `/v1/models`, startup phase (loading weights %, KV profiling, autotune,
  CUDA graphs), KV cache and concurrency, error lines since start, GPU, memory, image ID per node
- **Control:** start, stop, restart (head is stopped, workers restarted, head started),
  pull image, reboot the nodes, test prompt
- **Auto-recover:** if startup has not responded after `HANG_TIMEOUT_MIN`, or shows errors after
  3 min, the app restarts the cluster (at most `MAX_AUTO_RESTARTS` times), and then reboots
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
| `MAX_AUTO_RESTARTS` | | `1` |
| `ALLOW_REBOOT` / `AUTO_REBOOT` | | `false` / `false` |
| `VLLM_PORT`, `PORT`, `POLL_SECONDS` | | `8000`, `8080`, `15` |

Managed mode only: `VLLM_IMAGE` (preferably pinned to a digest), `MODEL`, `TP_SIZE`
(default: number of nodes), `GPU_MEM_UTIL`, `MAX_MODEL_LEN`, `VLLM_EXTRA_ARGS`, `HF_TOKEN`,
`HF_CACHE_DIR`, `EXTRA_MOUNTS` (comma-separated `src:dst`, relative to the cluster dir,
mounted `:ro`), and the optional overrides `IF_NAMES` / `IB_HCAS` (`;`-separated in node order).

## Build and run

```bash
docker build -t vllmapp:latest .
```

In Easypanel: an App service with the image `vllmapp:latest`, env as in the table, a volume or
bind mount on `/data` and port 8080. No domain is needed if the Cloudflare tunnel points at
`http://<project>_<service>_vllmapp:8080` (compose service).
