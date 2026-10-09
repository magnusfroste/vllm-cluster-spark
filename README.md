# vLLM Panel

**An easy-setup panel to run, administer and monitor vLLM on your own NVIDIA DGX Spark — one box or a cluster.**

One Spark serves the models that fit in its memory; two Sparks serve models no single box can
hold. Getting there by hand means vLLM images, Ray or multi-node flags, RoCE, NCCL, per-node
configs and a lot of SSH. vLLM Panel turns that into one web page: pick a model from the library,
download it, start it, and watch it — load, tokens, GPU temperature, power draw and tokens per
kWh on every node. Your prompts never leave your hardware.

vLLM Panel is an independent open-source project, not affiliated with or endorsed by the vLLM
project or NVIDIA. vLLM and DGX Spark are their owners' trademarks. Under the hood the panel's
pieces keep their original technical names: the `vllmapp-agent` on the nodes, `~/vllmapp`,
`~/vllmapp-data` and the `vllmapp` container.

## Quick start

On the head node (the one that serves the API), as your normal user:

```bash
curl -fsSL https://raw.githubusercontent.com/magnusfroste/vllm-cluster-spark/main/install.sh -o install.sh
```
```bash
bash install.sh
```

The script asks for the nodes' addresses (it suggests them) and a Hugging Face token, creates
the admin password and the API key, and starts the panel at `http://<head-ip>:8090`. From there
the panel walks you through the rest: the agent on each node, the setup checks, the model.

**Supported out of the box:** DGX Spark and the OEM GB10 units on DGX OS, which ships Docker
and the NVIDIA Container Toolkit; if Docker is missing, the script offers to install it.
Tested on ASUS Ascent GX10 (DGX OS 7.4). No Easypanel, reverse proxy or domain is needed, and
if you already run [Easypanel](https://easypanel.io), the panel deploys there too
([step 1b](#b-in-easypanel)).

| Verified models | Sparks | Notes |
|---|---|---|
| Qwen3.6-35B-A3B (Qwen FP8) | 1 | ~49 tok/s on one Spark, 256k context. MoE, the fastest here. The recommended start for one Spark |
| Qwen3.8-27B (Unsloth NVFP4) | 1 | ~11 tok/s on one Spark, 256k context. Dense and a newer generation, at a quarter of the speed |
| Qwen3.8-27B Heretic (NVFP4 W4A16) | 2 (fits on 1) | ~22 tok/s, 256k context. Verified across two Sparks |
| MiMo-V2.6-Flash-RL (FP8) | 2 | ~25 tok/s, 512k context |
| GLM-5.3-Flash (NVFP4) | 2 | 128k context |

The library has more models marked *untested*, and any Hugging Face repo vLLM supports.

## What it does

Runs a vLLM model on one NVIDIA DGX Spark (GB10), or across two or more with tensor
parallelism (over Ray or vLLM's own multi-node mode). The panel runs on the head node and
controls every node over SSH, through a small agent that can only run an allowlist of
commands.

Once the panel is deployed, everything happens from its pages, with a menu on the left. The first
time, **Overview** shows a **Get started** checklist with the whole path: reach the Sparks, the
setup checks, the Hugging Face token, a model (the catalog's recommended one for your number of
Sparks, one click), download, start, a test question with a checkable answer, then connecting
apps and, optionally, GarageAI. Each step has its button, and the list ticks itself off from
what the panel sees. Hide it when you're done; a link at the bottom of Overview brings it back.

| | Page | What it does |
|---|---|---|
| **Cluster** | **Overview** | A dashboard: status and the next step to take; load now (requests running and waiting, tokens per second, KV cache in use); the model; one line per node; today's tokens, requests and energy; the latest events. Each tile links to its page. Start, Restart, Stop, Pull image, Reboot |
| | **Nodes** | Setup checks per node (Docker, NVIDIA Container Toolkit, cluster link, RoCE, disk space) with the exact command for anything that needs `sudo`, the agent install command, and live GPU, memory and uptime per node |
| | **Logs** | The vLLM container log of each node, live, with a filter |
| | **Events** | What the panel and the cluster did: starts, config writes, downloads, automatic restarts |
| **Models** | **Models** | A catalog of ready-made models, or any model on Hugging Face; download on every node at once; the models on disk, with Delete |
| | **API** | The API addresses, model name and key, examples for curl, Python and OpenCode, and a test prompt |
| | **Usage** | Input and output tokens, requests and energy per day and week, and tokens per kWh |
| | **GarageAI** | Optional: connect the cluster to the GarageAI marketplace, and the tunnel and heartbeat status on the head |
| **Admin** | **Settings** | Auto-recover, the reboot button, timeouts, the public URL, and a preview of the config the panel writes |

The bottom of the menu shows who is logged in (`ADMIN_USER`), Log out, a theme switch, and the
panel and agent versions:

- **Theme:** System follows the computer's light or dark setting; Light and Dark override it.
  The choice is kept in the browser and also applies to the log page and the login page.
- **Version:** links to the commit history on GitHub, with a link to the repo under it. Once an
  hour the panel checks the version on the repo's `main` branch and shows **update available**
  when it is newer than the one running, with the command that updates it: `bash ~/vllmapp/install.sh --update`
  for the script install, or deploy again in Easypanel.

The browser tab's icon shows the cluster's state: green when it answers, yellow while it
starts, red on a problem.

Behind the pages the panel writes `.env`, `compose.yaml`, `entrypoint.sh` and any model patches
to every node (the network interface and RoCE HCAs are detected per node), and auto-recover
restarts a cluster whose startup hangs or that stops answering, with a reboot as the last
resort.

## What you need

- One or more DGX Spark nodes (or OEM GB10 units), with the same OS user on each. One Spark
  runs the models that fit in its memory (the catalog marks them "fits on one Spark"); the big
  ones need two
- With two or more: a direct QSFP cable between them, the **cluster link**. With more than two,
  a switch. With one Spark there is no link: use the node's own address as `HEAD_HOST`, leave
  `WORKER_HOSTS` empty, and skip the link steps below
- Internet on **every** node (wifi or the LAN port): each node downloads the image and the model
  itself, so a worker that only has the cluster link can't download anything
- A [Hugging Face](https://huggingface.co) account, for the model download

## 0. Prepare the nodes

These steps need root and are done once, on each node. The panel can't do them for you, because
it reaches the nodes **over the cluster link** — until the link has IP addresses, the panel can't
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

The panel checks all of this again under **Nodes** and shows the command for anything missing,
so a step you miss here shows up there.

## 1. Install the panel

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
saves everything in `~/vllmapp/.env` (readable only by you), and runs the panel as one container
from `ghcr.io/magnusfroste/vllm-cluster-spark` with its data in `~/vllmapp-data`. Then it prints
the address, `http://<head-ip>:8090`.

- `bash ~/vllmapp/install.sh --update` fetches the newest image and restarts the panel. The
  cluster keeps running.
- `bash ~/vllmapp/install.sh --uninstall` removes the container and keeps settings and data.
- The Hugging Face token can be left empty during the install and pasted later under
  **Settings → Hugging Face**. The admin password is changed in the profile menu, top right.
- To change a setting, edit `~/vllmapp/.env` and run `bash ~/vllmapp/install.sh` again.
- The page is plain HTTP on your own network. There is no domain or HTTPS, and none is needed
  to serve models: clients reach vLLM on port 8000, or a marketplace such as GarageAI reaches it
  through its own encrypted tunnel. For the page from outside, use Tailscale, NetBird or a
  Cloudflare tunnel.
- The port is 8090 and not 8080 on purpose: GarageAI's gateway may reach 8080 on a garage, and
  the admin page should not be reachable from there. Pick another with `--port`.

DGX OS (and the OEM units built on it) ships Docker and the NVIDIA Container Toolkit. If
Docker is missing, the script offers to install it with Docker's official script, the way
Easypanel's installer does, and if your user isn't in the `docker` group it offers to add it.
Both use sudo and ask first; nothing else does.

### b. In Easypanel

Create a project and an **App** service (not Compose) in the head node's Easypanel:

- **Source:** GitHub, this repo, branch `main`, build path `/`. The Dockerfile is in the root.
- **Environment:** the env only holds what the panel can't know by itself: the nodes, passwords
  and keys. You pick the model later, in the panel.
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
  | `ADMIN_PASSWORD` | the password for the panel. The user is `admin`, or `ADMIN_USER` if you set it |
  | `API_KEY` | the key every client must send to the model's API. Make one with `openssl rand -hex 32` and keep it: you give it to your apps, or to a gateway like LiteLLM |
  | `HF_TOKEN` | a **read** token from [huggingface.co/settings/tokens](https://huggingface.co/settings/tokens). For a gated model, also accept its license on the model's page first |

- **Mounts:** a bind mount from a host directory, for example `/home/youruser/vllmapp-data`,
  to `/data`. It holds the panel's SSH key, settings and statistics, and must survive redeploys.
  Without it the panel creates a new key that the nodes don't know.
- **Domain / port:** Easypanel sets `PORT=80` and the panel listens on it. The simplest start is
  the default domain Easypanel offers under the service's **Domains** tab. For access from
  outside, add your own domain, or point a Cloudflare tunnel that runs inside Easypanel at
  `http://<project>_<service>:80` (the host's own `cloudflared` can't resolve Docker service names).

Deploy and open the page. Log in as `admin` (or your `ADMIN_USER`) with `ADMIN_PASSWORD`. You can
change the password later in the profile menu (your name, top right); it is then stored hashed in
`credentials.json` in the data directory and replaces the env one. Forgot it? Delete that file and
the env password works again. The
login lasts 30 days and survives redeploys; changing `ADMIN_PASSWORD` logs everyone out. Scripts
can use Basic auth against `/api/…`.

## 2. Install the agent on every node

Open **Nodes → Install the agent on a node**, log in to each node yourself (head included) as
`SSH_USER`, and paste the command there. It installs a small agent in `~/.local/bin` and adds the panel's
key to `authorized_keys` with a forced command and `restrict`, so the key can only run the
agent's allowlisted commands. Run it again whenever **Nodes** says an agent is outdated. The
same command also offers the optional sudoers file for reboot and the page cache.

Until a node has the agent, **Nodes** says the panel can't reach it, with the SSH error and the
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
| Reboot and page cache (optional) | one sudoers file that only allows `systemctl reboot` and writing `/proc/sys/vm/drop_caches`. With it the panel can reboot a hung node, and frees the page cache before every start so the KV cache gets all the memory |

Then press **Download model** (on **Models** or **Nodes**). It runs `hf download` in a container on every node at once and
shows the bytes on disk per node. xet and `hf_transfer` are turned off, since both have hung on
large downloads on DGX Spark. The download continues if you close the page, and a stopped
download resumes where it left off.

`hf download` fetches the whole repo, so the panel checks its size first. It refuses a repo that is
much larger than the model's `size_gb` (GGUF repos often hold every quantization, terabytes in
all) or that does not fit on a node's disk. Pick a repo that holds one variant.

It takes a while: a ~190 GB model at 30 MB/s is close to two hours, and over wifi it can be
four. The progress bar under **Nodes** is the bytes on disk, so it shows a stalled download.

## 5. Start and use it

Press **Start** on **Overview**. The panel checks that the model is downloaded and that RoCE is up
on every node, frees the page cache, writes the config, starts the workers and then the head.
Startup for a large model takes 10–20 minutes, and Overview follows the phases. When it says
**Responding**, go to **API** and use **Test the model** with a prompt whose answer you can check.

The line at the top of **Overview** always says what to do next. **API** shows the API addresses
(on your network, and from anywhere once you set a public URL under **Settings**), the model
name, the key, and ready-to-paste examples for curl, Python and OpenCode. The API is
OpenAI-compatible, so most tools work with those three values.

**Usage** shows input and output tokens, requests and energy per day and per week, and tokens
per kWh. The panel reads vLLM's token counters and each node's GPU power every poll and keeps
them per hour in `/data/stats.db` (SQLite). The GPU reading leaves out CPU, memory, network and
disks; set **Other power per node** under **Settings**, from a wall meter, to count the whole box.

## Adding a Spark later

Start with one Spark and add a second when you want the big models. **Nodes → Sparks in this
cluster → Add a Spark** walks through it: connect the QSFP cable, give the new Spark a link
address (the page shows the command with the next free address), install the agent on it, and
add its address on the page. The panel checks that the new Spark answers on SSH first. Then
**Download model** fetches the model on it too, and **Restart** starts the model across both.

Adding works while the cluster runs and applies at the next Restart. Removing a Spark or
changing the head's address needs a stopped cluster. The list on the page replaces
`HEAD_HOST`/`WORKER_HOSTS` from env. If the head was installed as a single Spark with its
address on wifi or the LAN port, the page says so: give the head a link address as well and
change its address on the page before adding the second.

## Offering the cluster on GarageAI (optional)

[GarageAI](https://www.garageai.eu) is a marketplace where GPU owners sell inference. The
**GarageAI** page walks through connecting this cluster: create a garage in the portal with
vLLM as the runtime, run the command the portal shows on the head node (it installs NetBird and
a heartbeat with sudo, so the panel can't run it), use this cluster's API key where it says
`<YOUR_KEY>`, then turn on **Offer** for the model in the portal. The page reads the result on
the head without root: the NetBird tunnel, the heartbeat and its last report, and whether vLLM
answers on the mesh address. It never asks for the portal's setup key or register token, and
it doesn't say whether a model is live or what it earns: the portal does. After a model
switch it reminds you to offer the new model. Only the head node needs GarageAI.

## Changing the model or the config

Pick another model in the panel, download it, and press **Restart**. The page says
**Restart to apply** while the running model differs from the chosen one.

Changes to the env in Easypanel need a deploy. **A deploy only restarts the panel, never the
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
| `HF_TOKEN` | – | for gated models and the download. Easiest: paste it under **Settings → Hugging Face**, where the panel checks it with Hugging Face and shows the account. Set here in env, it wins and the page can't change it |
| `HF_CACHE_DIR` | `${HOME}/.cache/huggingface` | on the nodes |
| `TP_SIZE` | number of nodes | `--tensor-parallel-size` |
| `PATCHES` | `auto` | `auto` picks the patch set that lists `MODEL`, `none` turns patches off, or a folder name in `app/patchsets/` |
| `HF_OFFLINE` | `true` | vLLM runs with `HF_HUB_OFFLINE=1`, so only the panel's download fetches the model |
| `EXTRA_MOUNTS` | – | comma-separated `src:dst`, relative to the cluster dir, mounted `:ro` |
| `IF_NAMES` / `IB_HCAS` | detected | `;`-separated per node, if detection gets it wrong |
| `CLUSTER_DIR` | `~/vllm-cluster` | on the nodes, set in the agent's config at install |
| `CONFIG_MODE` | `managed` | `existing` never writes to the nodes (for a hand-written setup) |
| `GITHUB_REPO` | `magnusfroste/vllm-cluster-spark` | `owner/name` for the version links and the update check; point it at your fork |
| `VLLM_PORT`, `POLL_SECONDS` | `8000`, `15` | |

Auto-recover, the reboot button, the timeouts and the public URL are set in the panel under
**Settings** and apply at once. They can also be set in the env (`AUTO_RECOVER`,
`ALLOW_REBOOT`, `AUTO_REBOOT`, `STALL_TIMEOUT_MIN`, `HANG_TIMEOUT_MIN`, `UNHEALTHY_GRACE_MIN`,
`MAX_AUTO_RESTARTS`, `MEM_WARN_GIB`, `EXTRA_WATTS`), and then the env wins and the page shows them as locked.

The model settings are normally chosen in the panel. Setting any of these in the env overrides
the panel's choice, and the page shows them as locked: `MODEL`, `SERVED_MODEL_NAME`,
`VLLM_IMAGE`, `GPU_MEM_UTIL`, `MAX_MODEL_LEN`, `VLLM_EXTRA_ARGS`, `MODEL_REVISION`.

Only run one vLLM Panel per cluster, since each one does auto-recover on its own.

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
  "notes": "Shown on the model's card in the panel."
}
```

Optional: `revision` pins a commit; `patchset` names a folder in `app/patchsets/` (`"none"` turns
the automatic match off); `env` adds environment variables to the container; and `executor`
says how the nodes join: `ray` (the default, for images with Ray) or `mp`, vLLM's own
multi-node mode, where every node runs `vllm serve` with its node rank and the workers run
`--headless`. Official vLLM images no longer ship Ray, so newer models usually need `mp`. Set
`status` to `verified` once the model has run on real hardware and answered a checkable prompt
correctly, and write what you saw in `notes`. Models under ~100 GB fit on a single Spark, but
the panel always runs across all nodes.

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

With `PATCHES=auto` the panel picks the set whose `models` contains `MODEL`, writes the files to
`patches/` in the cluster dir on every node and mounts them read-only. `{revision}` in a mount
path is replaced with the set's revision. Patches are tied to one image build, so pin
`VLLM_IMAGE` to the digest the set was made for.

Included: [`glm-5.3-flash-nvfp4`](app/patchsets/glm-5.3-flash-nvfp4/README.md) for
`local-inference-lab/GLM-5.3-Flash-NVFP4-Spark`.

## How the panel reaches the nodes

The panel creates an SSH key pair in `/data` on first start. The agent on each node only accepts
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
