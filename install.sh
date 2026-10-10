#!/usr/bin/env bash
# install.sh — run vLLM Panel on the head node of a DGX Spark (or a cluster of them), without Easypanel.
#
# It runs the app as one Docker container (image from GitHub's registry), with its settings in
# ~/vllmapp/.env and its data in ~/vllmapp-data. Everything else, from the agent on the nodes to
# picking and starting a model, then happens in the app's page.
#
#   bash install.sh               install, or start again with the saved settings
#   bash install.sh --update      fetch the newest image and restart the app
#   bash install.sh --uninstall   stop and remove the app (settings and data are kept)
#
# Options: --port PORT (default 8090), --image REF. Environment: VLLMAPP_DIR, VLLMAPP_DATA.
# It needs Docker that your user can run. DGX OS ships Docker; if it is missing, or your user
# isn't in the docker group, the script offers to fix that with sudo and asks first.

set -euo pipefail

REPO="magnusfroste/vllm-cluster-spark"
IMAGE="ghcr.io/${REPO}:latest"
NAME="vllmapp"
# 8080 is avoided on purpose: GarageAI's gateway may reach 11434, 1234, 8080 and 8000 on a
# garage, and this admin page should not be one of them.
PORT=8090
# The copy of this script kept next to the settings knows its own directory, so
# `install.sh --update` works from wherever it was installed (the panel's Upgrade runs it that way).
HERE="$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")" 2>/dev/null && pwd || true)"
if [ -z "${VLLMAPP_DIR:-}" ] && [ -n "$HERE" ] && [ -f "$HERE/install.conf" ]; then DIR="$HERE"; else DIR="${VLLMAPP_DIR:-$HOME/vllmapp}"; fi
DATA="${VLLMAPP_DATA:-}"
[ -n "$DATA" ] || DATA="$( [ -f "$DIR/install.conf" ] && sed -n 's/^DATA=//p' "$DIR/install.conf" )"
DATA="${DATA:-$HOME/vllmapp-data}"
MODE=install
PORT_GIVEN=0
IMAGE_GIVEN=0

bold() { printf '\033[1m%s\033[0m\n' "$*"; }
info() { printf '  %s\n' "$*"; }
ok()   { printf '  \033[32m✓\033[0m %s\n' "$*"; }
warn() { printf '  \033[33m!\033[0m %s\n' "$*" >&2; }
die()  { printf '  \033[31m✗\033[0m %s\n' "$*" >&2; exit 1; }

ARGS=("$@")
while [ $# -gt 0 ]; do
  case "$1" in
    --update)    MODE=update; shift ;;
    --uninstall) MODE=uninstall; shift ;;
    --port)      PORT="${2:-}"; PORT_GIVEN=1; shift 2 ;;
    --image)     IMAGE="${2:-}"; IMAGE_GIVEN=1; shift 2 ;;
    -h|--help)   sed -n '2,14p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) die "unknown option: $1 (see --help)" ;;
  esac
done
case "$PORT" in ''|*[!0-9]*) die "invalid port: $PORT" ;; esac

# Read answers from the terminal, also when the script itself comes from a pipe.
ask() {  # ask VAR "question" "default" [secret]
  local answer="" prompt="$2"
  [ -n "$3" ] && prompt="$2 [$3]"
  if [ -n "${4:-}" ]; then read -rs -p "  $prompt: " answer </dev/tty; echo
  else read -r -p "  $prompt: " answer </dev/tty; fi
  printf -v "$1" '%s' "${answer:-$3}"
}

env_get() { [ -f "$DIR/.env" ] && sed -n "s/^$1=//p" "$DIR/.env" | tail -n 1; }

# The cluster link: the first IPv4 address on a RoCE (ConnectX) port.
link_ip() {
  local d dev ip
  for d in /sys/class/infiniband/*/device/net/*; do
    [ -e "$d" ] || continue
    dev="$(basename "$d")"
    ip="$(ip -o -4 addr show dev "$dev" 2>/dev/null | awk '{print $4}' | cut -d/ -f1 | head -n 1)"
    [ -n "$ip" ] && { echo "$ip"; return; }
  done
}
lan_ip() { ip route get 1.1.1.1 2>/dev/null | sed -n 's/.* src \([0-9.]*\).*/\1/p' | head -n 1; }

pull_image() {
  if ! docker pull -q "$IMAGE" >/dev/null 2>&1; then
    docker image inspect "$IMAGE" >/dev/null 2>&1 || die "could not pull $IMAGE"
    warn "could not pull $IMAGE; using the copy already on this machine"
  fi
}

run_container() {
  docker rm -f "$NAME" >/dev/null 2>&1 || true
  docker run -d --name "$NAME" --restart unless-stopped \
    --log-opt max-size=10m --log-opt max-file=3 \
    -p "${PORT}:${PORT}" -e "PORT=${PORT}" \
    -e "UPDATE_HINT=bash $DIR/install.sh --update" \
    --env-file "$DIR/.env" -v "$DATA:/data" "$IMAGE" >/dev/null
  local i
  for i in $(seq 1 30); do
    curl -fsS -m 3 "http://127.0.0.1:${PORT}/healthz" >/dev/null 2>&1 && return 0
    sleep 2
  done
  docker logs --tail 20 "$NAME" >&2 || true
  die "the app did not answer on port ${PORT} within a minute (log above)"
}

save_conf() {
  printf 'PORT=%s\nIMAGE=%s\nDATA=%s\n' "$PORT" "$IMAGE" "$DATA" > "$DIR/install.conf"
  # Keep a copy of this script next to the settings, for --update and --uninstall. When it was
  # piped from curl there is no file to copy, so fetch it.
  if [ -f "$0" ]; then
    [ "$(readlink -f "$0")" = "$(readlink -f "$DIR/install.sh")" ] || cp "$0" "$DIR/install.sh"
  else
    curl -fsSL "https://raw.githubusercontent.com/${REPO}/main/install.sh" -o "$DIR/install.sh" \
      || warn "could not save a copy of install.sh in $DIR; download it again to update"
  fi
}

# ---------------------------------------------------------------------------------------------
[ "$(id -u)" -ne 0 ] || die "Run this as your normal user, not as root: the app logs in to the nodes as that user."
command -v curl >/dev/null 2>&1 || die "curl is required."

confirm() {
  local answer=""
  read -r -p "  $1 [Y/n] " answer </dev/tty || true
  case "$answer" in ""|y|Y|yes) return 0 ;; *) return 1 ;; esac
}

# Docker, the way Easypanel's installer does it: Docker's own script, when it is missing.
if ! command -v docker >/dev/null 2>&1; then
  warn "Docker is not installed. (DGX OS ships it, so this system is not a standard DGX OS.)"
  confirm "Install Docker now with Docker's official script (get.docker.com)? It uses sudo." \
    || die "Install Docker yourself (docs.docker.com/engine/install), then run this again."
  curl -fsSL https://get.docker.com | sudo sh
  command -v docker >/dev/null 2>&1 || die "the Docker install did not finish"
  ok "Docker installed"
fi
if ! docker info >/dev/null 2>&1; then
  if ! id -nG "$USER" | grep -qw docker; then
    warn "Your user ($USER) can't run Docker yet."
    confirm "Add $USER to the docker group (sudo usermod -aG docker $USER)?" \
      || die "Run: sudo usermod -aG docker $USER, log out and in, then run this again."
    sudo usermod -aG docker "$USER"
    ok "added to the docker group (other terminals need a new login to see it)"
  fi
  # This shell started before the group change, so continue in a shell that has it.
  if [ -z "${VLLMAPP_IN_SG:-}" ]; then
    mkdir -p "$DIR"
    if [ -f "$0" ]; then self="$0"; else
      self="$DIR/install.sh"
      curl -fsSL "https://raw.githubusercontent.com/${REPO}/main/install.sh" -o "$self"
    fi
    exec sg docker -c "VLLMAPP_IN_SG=1 VLLMAPP_DIR=$(printf %q "$DIR") VLLMAPP_DATA=$(printf %q "$DATA") bash $(printf %q "$self") $(printf '%q ' "${ARGS[@]+"${ARGS[@]}"}")"
  fi
  die "Docker still does not answer. Check: sudo systemctl status docker"
fi
docker info 2>/dev/null | grep -qiE "nvidia.com/gpu|Runtimes:.*nvidia" \
  || warn "Docker has no NVIDIA runtime. The app's Nodes page shows how to add it; vLLM needs it, the app does not."

if [ -f "$DIR/install.conf" ]; then
  # The port and image given on the command line win; otherwise keep the saved ones.
  saved_port="$(sed -n 's/^PORT=//p' "$DIR/install.conf")"; saved_image="$(sed -n 's/^IMAGE=//p' "$DIR/install.conf")"
  [ "$PORT_GIVEN" -eq 1 ] || PORT="${saved_port:-$PORT}"
  [ "$IMAGE_GIVEN" -eq 1 ] || IMAGE="${saved_image:-$IMAGE}"
fi

if [ "$MODE" = uninstall ]; then
  bold "Remove vLLM Panel"
  docker rm -f "$NAME" >/dev/null 2>&1 && ok "container removed" || info "no container named $NAME"
  info "Kept: $DIR (settings) and $DATA (the app's SSH key, settings, statistics)."
  info "The cluster itself is not touched. Stop it from the app first if you want it stopped."
  exit 0
fi

if [ "$MODE" = update ]; then
  [ -f "$DIR/.env" ] || die "no installation in $DIR — run without --update first"
  bold "Update vLLM Panel"
  pull_image
  run_container
  save_conf
  ok "updated and running: $(docker inspect --format '{{.Config.Image}}' "$NAME")"
  info "The cluster kept running; only the panel restarted."
  exit 0
fi

bold "vLLM Panel — install on $(hostname)"
mkdir -p "$DIR" "$DATA"
chmod 700 "$DIR"

if [ -f "$DIR/.env" ]; then
  ok "using the settings in $DIR/.env (edit that file to change them)"
else
  echo
  info "The nodes are reached over the cluster link (the cable between the Sparks)."
  info "With one Spark, use this node's own address and leave the workers empty."
  head_default="$(link_ip)"; head_default="${head_default:-$(lan_ip)}"
  ask HEAD_HOST "This node's address on the cluster link" "$head_default"
  worker_default=""
  if [[ "$HEAD_HOST" =~ ^(.*)\.1$ ]] && ping -c 1 -W 1 "${BASH_REMATCH[1]}.2" >/dev/null 2>&1; then
    worker_default="${BASH_REMATCH[1]}.2"
  fi
  ask WORKER_HOSTS "Worker addresses, comma-separated (empty for one Spark)" "$worker_default"
  ask SSH_USER "Login user on the nodes (the same on all of them)" "$USER"
  echo
  info "A Hugging Face read token (huggingface.co/settings/tokens). Needed for gated models;"
  info "you can leave it empty and add it to $DIR/.env later."
  ask HF_TOKEN "HF token" "" secret
  command -v openssl >/dev/null 2>&1 || die "openssl is required to create the password and API key."
  ADMIN_PASSWORD="$(openssl rand -base64 18 | tr -d '/+=' | cut -c1-20)"
  API_KEY="$(openssl rand -hex 32)"
  umask 077
  cat > "$DIR/.env" <<EOF
# vLLM Panel settings. Change them here, then run: bash $DIR/install.sh
HEAD_HOST=$HEAD_HOST
WORKER_HOSTS=$WORKER_HOSTS
SSH_USER=$SSH_USER
ADMIN_PASSWORD=$ADMIN_PASSWORD
API_KEY=$API_KEY
HF_TOKEN=$HF_TOKEN
EOF
  umask 022
  ok "settings saved in $DIR/.env (only your user can read it)"
fi

echo
bold "Starting the app"
pull_image
run_container
save_conf
ok "vLLM Panel is running"

url_ip="$(lan_ip)"; url_ip="${url_ip:-$(env_get HEAD_HOST)}"
echo
bold "Open http://${url_ip}:${PORT}"
user="$(env_get ADMIN_USER)"
info "User:     ${user:-admin}"
info "Password: in $DIR/.env (ADMIN_PASSWORD)"
info "API key for your clients: in $DIR/.env (API_KEY); the app's API page shows it too."
echo
info "Next, in the app: Nodes → install the agent on every node, then work through the checks."
info "Update later with:  bash $DIR/install.sh --update"
