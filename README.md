# vllmapp

Status och styrning av ett vLLM/Ray-kluster på DGX Spark, avsett att köras som en app i
Easypanel på head-noden. Easypanel behövs bara på head; workers behöver docker,
nvidia-runtime och appens publika SSH-nyckel.

- **Status:** health, `/v1/models`, uppstartsfas (laddar vikter %, KV-profilering, autotune,
  CUDA-grafer), KV-cache och samtidighet, felrader sedan start, GPU, minne, image-ID per nod
- **Styrning:** starta, stoppa, starta om (head stoppas, workers startas om, head startas),
  hämta image, boota om noderna, testprompt
- **Auto-recover:** om uppstarten inte svarat efter `HANG_TIMEOUT_MIN`, eller visar fel efter
  3 min, startar appen om klustret (högst `MAX_AUTO_RESTARTS` gånger), och bootar sedan om
  om `ALLOW_REBOOT` och `AUTO_REBOOT` är på (högst en gång per 6 h)
- **Managed-läge:** appen genererar `.env`, `compose.yaml` och `entrypoint.sh` för varje nod
  från sin env och skriver dem över SSH. Nätgränssnitt och RoCE-HCA detekteras per nod.
  `existing`-läget (standard) rör aldrig filerna, men förhandsgranskningen visar diffen.

## Så når appen noderna

Appen skapar ett nyckelpar i `/data` vid första start. Nyckeln läggs i varje nods
`authorized_keys` med `command="…/vllmapp-agent",restrict`, så den kan bara köra agentens
vitlistade kommandon (`status`, `scan`, `logs`, `up`, `stop`, `restart`, `pull`, `reboot`,
`netdetect`, `read`/`put` av klusterfilerna). Installationskommandot visas på sidan under
**Konfiguration → Installera agenten**.

Reboot kräver en sudoers-rad per nod som bara tillåter `systemctl reboot`:

```bash
echo "$USER ALL=(root) NOPASSWD: /usr/bin/systemctl reboot" | sudo tee /etc/sudoers.d/vllmapp-reboot
sudo chmod 440 /etc/sudoers.d/vllmapp-reboot
```

## Env

| Env | Krävs | Exempel / standard |
|---|---|---|
| `HEAD_HOST` | ja | `192.168.100.1` (klusterlänkens IP) |
| `WORKER_HOSTS` | ja | `192.168.100.2` (kommaseparerad) |
| `SSH_USER` | ja | `spark1` |
| `ADMIN_PASSWORD` | ja | lösenord för webbsidan (användare `ADMIN_USER`, standard `admin`) |
| `API_KEY` | ja | vLLM:s API-nyckel, för health, modeller och testprompt |
| `SERVED_MODEL_NAME` | | `glm-5.3-flash` |
| `CLUSTER_DIR` | | `~/vllm-cluster`, sätts i agentens konfig vid installation |
| `CONFIG_MODE` | | `existing` eller `managed` |
| `AUTO_RECOVER` | | `false` |
| `HANG_TIMEOUT_MIN` | | `40` |
| `MAX_AUTO_RESTARTS` | | `1` |
| `ALLOW_REBOOT` / `AUTO_REBOOT` | | `false` / `false` |
| `VLLM_PORT`, `PORT`, `POLL_SECONDS` | | `8000`, `8080`, `15` |

Bara i managed-läget: `VLLM_IMAGE` (lås gärna en digest), `MODEL`, `TP_SIZE`
(standard antal noder), `GPU_MEM_UTIL`, `MAX_MODEL_LEN`, `VLLM_EXTRA_ARGS`, `HF_TOKEN`,
`HF_CACHE_DIR`, `EXTRA_MOUNTS` (kommaseparerade `src:dst`, relativt klusterkatalogen,
monteras `:ro`), och valfria override `IF_NAMES` / `IB_HCAS` (`;`-separerade i nodordning).

## Bygga och köra

```bash
docker build -t vllmapp:latest .
```

I Easypanel: en App-tjänst med imagen `vllmapp:latest`, env enligt tabellen, en volym eller
bindmount på `/data` och port 8080. Ingen domän behövs om Cloudflare-tunneln pekar på
`http://<projekt>_<tjänst>_vllmapp:8080` (compose-tjänst).
