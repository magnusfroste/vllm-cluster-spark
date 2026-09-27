# vllm-cluster

Tvånods DGX Spark (GB10) som kör `GLM-5.3-Flash-NVFP4` under vLLM med Ray, tensorparallellt.

- **[RECOVERY.md](RECOVERY.md)** — full återställning från bricked enhet till fungerande API
- **[patches/README.md](patches/README.md)** — varför de lokala vLLM-ändringarna behövs

```bash
cp env.head.example .env     # head (eller env.worker.example på worker)
$EDITOR .env                 # VLLM_API_KEY + HF_TOKEN
docker compose up -d
```

Modellen serveras som `glm-5.3-flash` på port 8000.

**Hemligheter ligger aldrig i repot.** `.env`, `env.head` och `env.worker` är gitignorerade;
mallarna innehåller platshållare.
