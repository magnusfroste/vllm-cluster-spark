# Changelog

vLLM Panel (formerly vllmapp): a panel to set up, run and monitor vLLM on one or two NVIDIA DGX Spark.
Versions follow `APP_VERSION` in `app/main.py`. "Agent vN" is the SSH agent on the nodes: after an
upgrade that names a new agent, install it again under **Nodes**.

## 1.28.3 — 2026-10-18
- The Logs page also hides `GET /api/version`, the Ollama check that GarageAI Bridge's heartbeat makes every 5 minutes (a 404 on vLLM, harmless).

## 1.28.2 — 2026-10-14
- The hang detector no longer counts torch's warnings as startup errors. Lines such as the Triton bundler's "Cubin file … not found" (it compiles the kernel again) made a healthy MiMo start look hung after 11 minutes, and auto-recover restarted it.

## 1.28.1 — 2026-10-14
- With GarageAI Bridge, the drain before a stop waits at least 30 s (was 15 s), so GarageAI's routing has taken the garage out before vLLM stops. The panel checks for Bridge itself, also when the GarageAI page hasn't been opened.

## 1.28.0 — 2026-10-14 (agent v14)
- **GarageAI Bridge knows what the panel does.** The panel writes its state to `~/.local/state/vllm-panel/state.json` on the head: serving, starting, maintenance, stopped or failed, with the reason, and each node's container, GPU temperature, power and use, and free memory. It is rewritten on every change and at least every minute.
- **Drain before a stop.** On Redeploy, Stop, a model change or a reboot the panel first writes *maintenance* (Bridge takes the garage out of GarageAI's pool within about 10 s), then waits for running requests to finish, at most a minute, before it stops vLLM. Without GarageAI it only waits for running requests.
- The GarageAI page shows what the panel last told Bridge.

## 1.27.0 — 2026-10-14
- **Chat** is its own page in the menu, right under Overview, instead of a tab under Deployment. It talks to the running model by default; when vLLM serves more than one model, a picker chooses which.
- Deployment keeps Overview, Configuration and Environment; its Logs link is gone (Logs is in the menu). The API page no longer has a card that only pointed to the chat.

## 1.26.0 — 2026-10-12 (agent v13)
- GarageAI page recognises **GarageAI Bridge**. It shows the Bridge version and tunnel (`garageai-tunnel.service`) and the heartbeat (`garageai-bridge.timer`). Garages on the older connect script + NetBird show as before.
- The connect steps are now Bridge's: run the install line, then `garageai connect`, and approve in the browser.

## 1.25.0 — 2026-10-10
- vLLM's request log (one line per request) is a setting under **Settings**, **on by default**. 1.18.0 had turned it off, which made the Logs page look dead during inference.
- The Logs page hides the panel's own `/health`, `/metrics`, `/version` and `/v1/models` calls.

## 1.24.1 — 2026-10-10
- The Easypanel deploy webhook is called with a JSON body; without one it answered 415.

## 1.24.0 — 2026-10-10 (agent v12)
- **Upgrade button** at the bottom of the menu when a newer version is out. The page waits for the new version and reloads.
- `install.sh` installs upgrade through the agent (`install.sh --update`, detached). Easypanel installs upgrade through the service's deploy webhook, which you save under **Settings → Updates**.
- `install.sh --update` works from wherever the panel was installed.

## 1.23.0 — 2026-10-10
- **Deployment → Chat**: streaming chat with the running model, with the same quick prompts as the GarageAI Operations Center, and token and speed stats.

## 1.22.0 — 2026-10-09
- **Deploy from the library.** Every model card has **Deploy**. It downloads when needed, stops the running model, and starts when every node has the weights.
- New **Deployment** page:
  - Redeploy, Stop and Change model;
  - tabs Overview, Configuration and Environment, with **Save & Redeploy**;
  - your changes are kept on top of the template, are marked against it, and **Reset to the template** removes them.

## 1.21.0 — 2026-10-09
- Less repetition:
  - status lives in the top bar and on Overview;
  - the status card is one line;
  - Start and Stop show only when they apply;
  - node actions sit on the node cards.

## 1.20.1 — 2026-10-09
- No model box in the sidebar.

## 1.20.0 — 2026-10-09
- **Uniform model cards**, each showing: summary, type, quantisation, download size, Sparks, context, measured speed, and badges (Verified, Recommended, Running, Own).
- **Your own model templates**: **+ Add your own model**, also **filled from a docker compose file**. They are stored in `/data/models` and survive upgrades.

## 1.19.0 — 2026-10-09 (agent v11)
- **Nodes → vLLM images**: images per node, their size and age, and which models use them. Remove the unused ones.
- **Alert webhook** (ntfy, Slack, Discord or JSON) for when the model stops or starts answering again, automatic restarts, and errors.

## 1.18.0 — 2026-10-09 (agent v10)
- Capped logs: vLLM 3×50 MB, the panel 3×10 MB.
- Optional firewall check that the cluster ports are only open on the cluster link.

## 1.17.0 — 2026-10-08
- Change the admin password from the profile menu, top right.

## 1.16.0 — 2026-10-08
- Renamed to **vLLM Panel**.

## 1.15.0 — 2026-10-08
- **Get started** guide on Overview: a checklist, and a recommended model for one or two Sparks.

## 1.14.0 — 2026-10-08 (agent v9)
- **GarageAI** page: the steps to sell inference in GarageAI, and the connection status.

## 1.13.0 — 2026-10-08
- **Add a Spark** from the Nodes page, step by step. Remove a node or change the head's address while stopped.

## 1.12.0 — 2026-10-08
- Hugging Face token (checked against Hugging Face before it is saved) and admin password under **Settings**.

## 1.11.0 — 2026-10-08
- Shows the vLLM version and image, and the Hugging Face token status.

## 1.10.0 — 2026-10-08
- A more modern look; theme switch and user menu top right.

## 1.9.0 — 2026-10-08
- **One Spark**: plain `vllm serve` without Ray or multi-node flags. The library shows the models that fit on one Spark first.

## 1.8.0 — 2026-10-08
- **Install without Easypanel**: `install.sh` runs the panel from `ghcr.io/magnusfroste/vllm-cluster-spark` on port 8090, with `--update` and `--uninstall`. It offers to install Docker when it is missing.

## 1.7.0 – 1.7.2 — 2026-10-05 to 2026-10-07
- Checks that the chosen model's image is on every node, and pulls it from the panel.
- Size check before a download.
- Running model as a label in the sidebar.

## 1.6.0 – 1.6.2 — 2026-10-05
- Join the nodes without Ray (executor `mp`), with environment variables per model. MiMo-V2.6-Flash verified on 2× DGX Spark.

## 1.4.1 – 1.5.2 — 2026-10-01 to 2026-10-04
- Left menu with pages.
- Overview as a dashboard.
- Theme switch.
- Version linked to GitHub, with an update check.
- Status favicon.
- First-time setup step for the nodes.

## Before 1.4 — 2026-09-24 to 2026-09-30
- Status and control of the cluster from Easypanel:
  - pick a model from a catalog;
  - set up the cluster from the app;
  - log pages per node, a stall watchdog and a RoCE check;
  - usage per day and week, including energy.
- Form login with a 30-day cookie.
