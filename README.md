# vLLM Cluster Spark

**Private AI on your own NVIDIA DGX Spark cluster — with a control plane you'd expect from a cloud.**

Two DGX Sparks can serve models no single box can hold, but getting there means Ray, RoCE, NCCL,
per-node configs and a lot of SSH. This app turns that into one web page, inspired by Easypanel:
set the cluster up, start it, watch it and fix it — and see what it costs to run, down to GPU
temperature, power draw and tokens per kWh on every node. Your prompts never leave your hardware.

## What it does

Runs a vLLM model across two or more NVIDIA DGX Spark (GB10) nodes with tensor parallelism
over Ray, and gives you one web page to set it up, start it, watch it and fix it. The app runs
on the head node, either straight from a one-line install script or in
[Easypanel](https://easypanel.io), and controls every node over SSH.

Once the app is deployed, everything happens from its pages, with a menu on the left:

| | Page | What it does |
|---|---|---|
| **Cluster** | **Overview** | A dashboard: status and the next step to take; load now (requests running and waiting, tokens per second, KV cache in use); the model; one line per node; today's tokens, requests and energy; the latest events. Each tile links to its page. Start, Restart, Stop, Pull image, Reboot |
| | **Nodes** | Setup checks per node (Docker, NVIDIA Container Toolkit, cluster link, RoCE, disk space) with the exact command for anything that needs `sudo`, the agent install command, and live GPU, memory and uptime per node |
| | **Logs** | The vLLM container log of each node, live, with a filter |
| | **Events** | What the app and the cluster did: starts, config writes, downloads, automatic restarts |
| **Models** | **Models** | A catalog of ready-made models, or any model on Hugging Face; download on every node at once; the models on disk, with Delete |
| | **API** | The API addresses, model name and key, examples for curl, Python and OpenCode, and a test prompt |
| | **Usage** | Input and output tokens, requests and energy per day and week, and tokens per kWh |
| **Admin** | **Settings** | Auto-recover, the reboot button, timeouts, the public URL, and a preview of the config the app writes |

The bottom of the menu shows who is logged in (`ADMIN_USER`), Log out, a theme switch, and the
app and agent versions:

- **Theme:** System follows the computer's light or dark setting; Light and Dark override it.
  The choice is kept in the browser and also applies to the log page and the login page.
- **Version:** links to the commit history on GitHub, with a link to the repo under it. Once an
  hour the app checks the version on the repo's `main` branch and shows **update available**
  when it is newer than the one running, with the command that updates it: `bash ~/vllmapp/install.sh --update`
  for the script install, or deploy again in Easypanel.

The browser tab's icon shows the cluster's state: green when it answers, yellow while it
starts, red on a problem.

Behind the pages the app writes `.env`, `compose.yaml`, `entrypoint.sh` and any model patches
to every node (the network interface and RoCE HCAs are detected per node), and auto-recover
restarts a cluster whose startup hangs or that stops answering, with a reboot as the last
resort.

## What you need

- Two or more DGX Spark nodes (or OEM GB10 units), with the same OS user on each
- A direct QSFP cable between them: the **cluster link**. With more than two nodes, a switch
- Internet on **every** node (wifi or the LAN port): each node downloads the image and the model
  itself, so a worker that only has the cluster link can't download anything
- A [Hugging Face](https://huggingface.co) account, for the model download

## 0. Prepare the nodes

These steps need root and are done once, on each node. The app can't do them for you, because
it reaches the nodes **over the cluster link** — until the link has IP addresses, the app can't
reach a worker at all.

1. **Give the cluster link a static IP on every node.** Pick a private /24 that nothing else on
   the nodes uses, for example `192.168.100.1` for the head and `192.168.100.2`, `.3`, … for the
   workers. Plug in the cable, then on each node:
   ```bash
   ip -br link        # the QSFP port shows UP once the cable is in, e.g. enp1s0f0np0
   sudo nmcli con add type ethernet ifname <port> con-name cluster ipv4.method manual ipv4.addresses 192.168.100.1/24
   sudo nmcli con up cluster
   ```
   Use the node's own address in the second command. Check from the head:
   `ping -c3 192.168.100.2` should answer.
2. **SSH is on** on every node (it is on DGX OS): `sudo systemctl enable --now ssh`.
3. **The OS user may use Docker** on every node: `sudo usermod -aG docker $USER`, then log out
   and in again. `docker ps` should work without `sudo`.
4. **Every node reaches the internet:** `curl -sI https://huggingface.co | head -1` should say
   `HTTP/2 200`.
5. **Easypanel on the head node.** Docker is already installed on DGX OS, and ports 80, 443 and
   3000 must be free:
   ```bash
   curl -sSL https://get.easypanel.io | sudo sh
   ```
   Open `http://<head's LAN IP>:3000` and create the admin account. See the
   [Easypanel docs](https://easypanel.io/docs).

The app checks all of this again under **Nodes** and shows the command for anything missing,
so a step you miss here shows up there.

## 1. Install the app

There are two ways. Pick one.

### a. With the install script (plain DGX OS, no Easypanel)

On the head node, as your normal user (not root):

```bash
curl -fsSL https://raw.githubusercontent.com/magnusfroste/vllm-cluster-spark/main/install.sh -o install.sh
```
```bash
bash install.sh
```

It asks for this node's address on the cluster link and the workers' (it suggests both), the
login user and a Hugging Face token. It creates the admin password and the API key itself,
saves everything in `~/vllmapp/.env` (readable only by you), and runs the app as one container
from `ghcr.io/magnusfroste/vllm-cluster-spark` with its data in `~/vllmapp-data`. Then it prints
the address, `http://<head-ip>:8090`.

- `bash ~/vllmapp/install.sh --update` fetches the newest image and restarts the app. The
  cluster keeps running.
- `bash ~/vllmapp/install.sh --uninstall` removes the container and keeps settings and data.
- To change a setting, edit `~/vllmapp/.env` and run `bash ~/vllmapp/install.sh` again.
- The page is plain HTTP on your own network. There is no domain or HTTPS, and none is needed
  to serve models: clients reach vLLM on port 8000, or a marketplace such as GarageAI reaches it
  through its own encrypted tunnel. For the page from outside, use Tailscale, NetBird or a
  Cloudflare tunnel.
- The port is 8090 and not 8080 on purpose: GarageAI's gateway may reach 8080 on a garage, and
  the admin page should not be reachable from there. Pick another with `--port`.

The script needs Docker that your user can run without sudo (DGX OS has Docker; add yourself
to the `docker` group if `docker ps` fails). It never runs sudo itself.

### b. In Easypanel

Create a project and an **App** service (not Compose) in the head node's Easypanel:

- **Source:** GitHub, this repo, branch `main`, build path `/`. The Dockerfile is in the root.
- **Environment:** the env only holds what the app can't know by itself: the nodes, passwords
  and keys. You pick the model later, in the app.
  ```env
  HEAD_HOST=192.168.100.1
  WORKER_HOSTS=192.168.100.2
  SSH_USER=youruser
  ADMIN_PASSWORD=choose-one
  API_KEY=choose-a-long-random-key
  HF_TOKEN=hf_...
  ```

  | Env | What to put there |
  |---|---|
  | `HEAD_HOST`, `WORKER_HOSTS` | the **cluster link** IPs from step 0, not the LAN addresses. Workers comma-separated |
  | `SSH_USER` | the normal login user on the nodes (the same on all of them), not root |
  | `ADMIN_PASSWORD` | the password for this app's page. The user is `admin`, or `ADMIN_USER` if you set it |
  | `API_KEY` | the key every client must send to the model's API. Make one with `openssl rand -hex 32` and keep it: you give it to your apps, or to a gateway like LiteLLM |
  | `HF_TOKEN` | a **read** token from [huggingface.co/settings/tokens](https://huggingface.co/settings/tokens). For a gated model, also accept its license on the model's page first |

- **Mounts:** a bind mount from a host directory, for example `/home/youruser/vllmapp-data`,
  to `/data`. It holds the app's SSH key, settings and statistics, and must survive redeploys.
  Without it the app creates a new key that the nodes don't know.
- **Domain / port:** Easypanel sets `PORT=80` and the app listens on it. The simplest start is
  the default domain Easypanel offers under the service's **Domains** tab. For access from
  outside, add your own domain, or point a Cloudflare tunnel that runs inside Easypanel at
  `http://<project>_<service>:80` (the host's own `cloudflared` can't resolve Docker service names).

Deploy and open the page. Log in as `admin` (or your `ADMIN_USER`) with `ADMIN_PASSWORD`. The
login lasts 30 days and survives redeploys; changing `ADMIN_PASSWORD` logs everyone out. Scripts
can use Basic auth against `/api/…`.

## 2. Install the agent on every node

Open **Nodes → Install the agent on a node**, log in to each node yourself (head included) as
`SSH_USER`, and paste the command there. It installs a small agent in `~/.local/bin` and adds the app's
key to `authorized_keys` with a forced command and `restrict`, so the key can only run the
agent's allowlisted commands. Run it again whenever **Nodes** says an agent is outdated. The
same command also offers the optional sudoers file for reboot and the page cache.

Until a node has the agent, **Nodes** says the app can't reach it, with the SSH error and the
three usual causes: the link has no IP, the agent isn't installed, or SSH is off.

## 3. Pick a model

**Models** lists ready-made models with settings that suit DGX Spark: the vLLM image,
the tool and reasoning parsers, context length and GPU memory share. **Verified** means we have
run it on two Sparks and checked the answers. **Untested** means the settings come from the
model card and have not been run here yet. You can also pick any other Hugging Face repo and
give the vLLM arguments yourself. GPU memory share and max context are under **Advanced**.

Press **Use this model**. Nothing happens to a running cluster until you press Restart.

## 4. Work through the checks on Nodes

**Nodes** shows every node with a checklist, and the menu shows how many items are left. Each item that fails comes with the
command to fix it. These are one-time steps on the node itself, since they need root:

| Check | Typical fix |
|---|---|
| Docker usable by the SSH user | `sudo usermod -aG docker $USER`, then log in again |
| NVIDIA Container Toolkit | preinstalled on DGX OS; otherwise `apt install nvidia-container-toolkit` |
| Cluster link IP | a static IP on the QSFP port with `nmcli` |
| Head reachable over the link | cable, and link IPs in the same subnet |
| Disk space for the model | free space in the HF cache (`HF_CACHE_DIR`) |
| The model's vLLM image is on the node | **Pull image** on Overview. Easypanel's daily Docker cleanup removes images no container uses, on the head only; the models in the HF cache are plain files on the host and are not touched |
| Reboot and page cache (optional) | one sudoers file that only allows `systemctl reboot` and writing `/proc/sys/vm/drop_caches`. With it the app can reboot a hung node, and frees the page cache before every start so the KV cache gets all the memory |

Then press **Download model** (on **Models** or **Nodes**). It runs `hf download` in a container on every node at once and
shows the bytes on disk per node. xet and `hf_transfer` are turned off, since both have hung on
large downloads on DGX Spark. The download continues if you close the page, and a stopped
download resumes where it left off.

`hf download` fetches the whole repo, so the app checks its size first. It refuses a repo that is
much larger than the model's `size_gb` (GGUF repos often hold every quantization, terabytes in
all) or that does not fit on a node's disk. Pick a repo that holds one variant.

It takes a while: a ~190 GB model at 30 MB/s is close to two hours, and over wifi it can be
four. The progress bar under **Nodes** is the bytes on disk, so it shows a stalled download.

## 5. Start and use it

Press **Start** on **Overview**. The app checks that the model is downloaded and that RoCE is up
on every node, frees the page cache, writes the config, starts the workers and then the head.
Startup for a large model takes 10–20 minutes, and Overview follows the phases. When it says
**Responding**, go to **API** and use **Test the model** with a prompt whose answer you can check.

The line at the top of **Overview** always says what to do next. **API** shows the API addresses
(on your network, and from anywhere once you set a public URL under **Settings**), the model
name, the key, and ready-to-paste examples for curl, Python and OpenCode. The API is
OpenAI-compatible, so most tools work with those three values.

**Usage** shows input and output tokens, requests and energy per day and per week, and tokens
per kWh. The app reads vLLM's token counters and each node's GPU power every poll and keeps
them per hour in `/data/stats.db` (SQLite). The GPU reading leaves out CPU, memory, network and
disks; set **Other power per node** under **Settings**, from a wall meter, to count the whole box.

## Changing the model or the config

Pick another model in the app, download it, and press **Restart**. The page says
**Restart to apply** while the running model differs from the chosen one.

Changes to the env in Easypanel need a deploy. **A deploy only restarts the app, never the
cluster.** Every change takes effect on the next **Start** or **Restart**, which writes the
config to every node first. **Settings → Preview config** shows the diff against what the
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
| `GITHUB_REPO` | `magnusfroste/vllm-cluster-spark` | `owner/name` for the version links and the update check; point it at your fork |
| `VLLM_PORT`, `POLL_SECONDS` | `8000`, `15` | |

Auto-recover, the reboot button, the timeouts and the public URL are set in the app under
**Settings** and apply at once. They can also be set in the env (`AUTO_RECOVER`,
`ALLOW_REBOOT`, `AUTO_REBOOT`, `STALL_TIMEOUT_MIN`, `HANG_TIMEOUT_MIN`, `UNHEALTHY_GRACE_MIN`,
`MAX_AUTO_RESTARTS`, `MEM_WARN_GIB`, `EXTRA_WATTS`), and then the env wins and the page shows them as locked.

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

Optional: `revision` pins a commit; `patchset` names a folder in `app/patchsets/` (`"none"` turns
the automatic match off); `env` adds environment variables to the container; and `executor`
says how the nodes join: `ray` (the default, for images with Ray) or `mp`, vLLM's own
multi-node mode, where every node runs `vllm serve` with its node rank and the workers run
`--headless`. Official vLLM images no longer ship Ray, so newer models usually need `mp`. Set
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

## Running it by hand

The install script only wraps this. To build the image yourself instead:

```bash
docker build -t vllmapp:latest .
docker run -d --name vllmapp --restart unless-stopped -p 8090:8090 -e PORT=8090 --env-file .env -v ~/vllmapp-data:/data vllmapp:latest
```
