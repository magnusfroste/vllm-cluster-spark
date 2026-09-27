# Återställning: GLM-5.3-Flash på tvånods DGX Spark

Körklart läge: `glm-5.3-flash` på `http://192.168.100.1:8000/v1`, tensorparallellt över två
DGX Spark (GB10), Ray som executor. Verifierat 2026-09-20.

Den här filen antar att **allt är borta** — inklusive en bricked enhet. Hoppa över steg du inte behöver.

---

## 0. Hårdvara och roller

| | Head | Worker |
|---|---|---|
| Hostname | `spark1` | `gx10-34ee` |
| Klusterlänk | `enp1s0f0np0` → `192.168.100.1/24` | `enp1s0f1np1` → `192.168.100.2/24` |
| RoCE HCA | `rocep1s0f0,roceP2p1s0f0` | `rocep1s0f1,roceP2p1s0f1` |
| Kör | Ray head + `vllm serve` | Ray worker |

Internet går via wifi (`wlP9s9`). Klusterlänken är en direktkabel mellan noderna — den behöver
ingen router, bara statiska adresser i samma /24.

## 1. Bricked enhet: ny OS-installation

1. Ladda ner ASUS Ascent GX10 OS-imagen (vi körde `7.5.0-2-20260525090919`).
2. Skriv den till ett **USB-minne**, inte en extern hårddisk — GX10:s boot-meny listade inte
   den externa disken. Med USB3-adapter fungerade det.
   ```bash
   lsblk                                  # identifiera rätt enhet, t.ex. /dev/sdb
   sudo dd if=~/Downloads/asus.iso of=/dev/sdb bs=4M status=progress conv=fsync
   ```
3. Boota från USB och installera. Skapa användaren `spark1`.
4. Efter installation: sätt statisk IP på klusterporten och stäng av wifi-powersave
   (annars tappar långa nedladdningar kontakten):
   ```bash
   sudo iw dev wlP9s9 set power_save off
   sudo tee /etc/NetworkManager/conf.d/90-wifi-powersave-off.conf >/dev/null <<'EOF'
   [connection]
   wifi.powersave = 2
   EOF
   sudo systemctl restart NetworkManager
   ```
5. Verifiera länken mellan noderna: `ping -c3 192.168.100.2` från head.

## 2. Docker och image

```bash
docker pull ghcr.io/spark-arena/dgx-vllm-eugr-nightly-tf5:latest
docker image inspect ghcr.io/spark-arena/dgx-vllm-eugr-nightly-tf5:latest \
  --format '{{index .RepoDigests 0}}'
```
Verifierad digest: `sha256:1b0be62c1ccb37f738746cd8fbcec73b46f0c283082a6d46aebf9f19b5747563`
(vLLM `0.3.1.dev19+g08633cb5c`, byggd 2026-09-17). **Samma digest måste ligga på båda noderna** —
Ray kräver identiska byggen.

## 3. Modellen (~188 GB, per nod)

Båda noderna behöver hela modellen i sin egen HF-cache.

> **Viktigt:** på den här maskinen hänger både xet och `hf_transfer` — processen somnar på futex
> med 0 sockets och 0 % CPU, utan felmeddelande. Stäng av båda. Varje byte av backend slänger
> dessutom pågående partialer.

```bash
docker run -d --name hf-dl --network host \
  -e HF_TOKEN=$HF_TOKEN \
  -e HF_HUB_ENABLE_HF_TRANSFER=0 -e HF_HUB_DISABLE_XET=1 -e HF_HUB_DOWNLOAD_TIMEOUT=30 \
  -v ~/.cache/huggingface:/root/.cache/huggingface \
  --entrypoint bash ghcr.io/spark-arena/dgx-vllm-eugr-nightly-tf5:latest \
  -c 'hf download local-inference-lab/GLM-5.3-Flash-NVFP4-Spark --max-workers 8'
```

Följ progress med storleken, inte med att processen lever:
```bash
du -sb ~/.cache/huggingface/hub/models--local-inference-lab--GLM-5.3-Flash-NVFP4-Spark
find ~/.cache/huggingface -name '*.incomplete' | wc -l    # ska bli 0
```
Klart = ~187,7 GB, 44 shards, 0 `.incomplete`.

## 4. Konfiguration

```bash
git clone <detta repo> ~/vllm-cluster && cd ~/vllm-cluster
cp env.head.example .env       # på head
cp env.worker.example .env     # på worker
$EDITOR .env                   # fyll i VLLM_API_KEY och HF_TOKEN
```

Snapshot-hashen i `compose.yaml` (monteringen av `patches/config.json`) måste matcha den
faktiska katalogen under `snapshots/`. Kontrollera:
```bash
ls ~/.cache/huggingface/hub/models--local-inference-lab--GLM-5.3-Flash-NVFP4-Spark/snapshots/
```

## 5. Starta

```bash
# worker först
ssh 192.168.100.2 'cd ~/vllm-cluster && docker compose up -d'
# sedan head
cd ~/vllm-cluster && docker compose up -d
docker logs -f vllm-head
```

Uppstarten tar ~12 minuter: Ray-anslutning, viktladdning (~9 min, den långsammare noden styr),
KV-cache, graph-capture. Vänta på `Application startup complete`.

## 6. Verifiera — med ett svar som går att kontrollera

Att det kommer text räcker inte. Fel dequantisering ger flytande nonsens som ser rimligt ut.
Använd något med ett facit:

```bash
KEY=$(grep -oP '(?<=^VLLM_API_KEY=).*' .env)
curl -s http://localhost:8000/v1/chat/completions \
  -H "Content-Type: application/json" -H "Authorization: Bearer $KEY" \
  -d '{"model":"glm-5.3-flash","messages":[{"role":"user",
       "content":"Svara kort: vad är huvudstaden i Sverige, och vad blir 17*23?"}],
       "max_tokens":300,"temperature":0}' | jq -r '.choices[0].message.content'
```
Förväntat: **Stockholm** och **391**.

Referensvärden vid lyckad start:
- Modellen: 86,25 GiB per nod
- KV-cache: ~78 000 tokens, 2,39x samtidighet vid 32k
- `Using TRITON_MLA attention backend`

---

## Varför patcharna behövs

Se [`patches/README.md`](patches/README.md). Kort: bygget antar BF16 i linear attention, medan
checkpointen levererar MXFP8 med E8M0-skalor — och på sm121 kräver den enda sparse-MLA-backenden
ett cacheformat som checkpointens NoPE-geometri inte kan leverera.

## Kända kvarvarande punkter

- Workern rapporterade bara 1,36 GiB ledigt till KV-cache mot heads 4,24 GiB. Något håller minne
  på `gx10-34ee`; städas det kan cachen växa.
- Sparse-läget (top-2048) är avstängt. Att få tillbaka det kräver antingen ett nyare bygge från
  spark-arena eller en checkpoint med `qk_rope_head_dim = 64`.
- `--gpu-memory-utilization` är 0,82. Går inte att höja mycket: modellen väger 86,25 GiB av
  121,63 GiB, och startkontrollen mäter *fritt* minne, som sjunker när sidcachen är varm.
