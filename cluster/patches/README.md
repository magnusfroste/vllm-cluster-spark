# Patchar mot `ghcr.io/spark-arena/dgx-vllm-eugr-nightly-tf5:latest`

Alla filer monteras read-only över imagens egna via `compose.yaml`, på **båda** noderna.
De är kopior av imagens filer med de ändringar som beskrivs nedan — vid ny image måste de
rebasas, annars skriver de över nyare kod.

Digest de togs från: `sha256:1b0be62c1ccb37f738746cd8fbcec73b46f0c283082a6d46aebf9f19b5747563`

## Grundproblemet

Bygget antar att linear attention (KDA) och MLA körs i **BF16**. Checkpointen
`local-inference-lab/GLM-5.3-Flash-NVFP4-Spark` levererar **MXFP8** där:

- vikter `F8_E4M3`
- skalor `U8` i **E8M0** — värdet är `2^(v-127)`, inte `v` (typiska värden 115–118 → ~1e-4)
- blockform **[1, 32]** (en skala per 32 element längs indata), inte kvadratiska [128, 128]

Experterna är NVFP4, dense-MLP i lager 0–2 är BF16. Det är alltså en blandad checkpoint.

## Filerna

### `kda.py`
Tar bort `vllm_config.quant_config = None` runt `super().__init__()`. Kommentaren i originalet
säger *"KDA projections remain BF16 because fp8 checkpoints omit their scales"* — den här
checkpointen har scales, så lagren ska vara kvantiserade.

### `modelopt.py`
`_resolve_quant_algo`: `fused_projection_shards` utökad med
`in_proj_qkvbfg_a` → (`q_proj`, `k_proj`, `v_proj`, `b_proj`, `f_a_proj`, `g_a_proj`) och
`wk_weights_proj` → (`wk`, `weights_proj`).

Matchningen sker på **svansen** av modulsökvägen (`layers.N.self_attn.q_proj`), eftersom
`hf_quant_config.json` använder prefixet `model.language_model.…` medan vLLM slår upp lagret
som `language_model.model.…`.

### `model.py`
- `_dequant_fp8_block` härleder blockformen ur skal-tensorns form i stället för att anta
  kvadratiska block, och tolkar `uint8`-skalor som E8M0.
- `_try_load_fp8_attn_proj` och `_try_load_fp8_indexer_wk` lämnar lager som faktiskt är
  kvantiserade till den normala vägen (de har `weight_scale`, inte bara `weight_scale_inv`).
- Ny `_try_load_mxfp8_as_bf16`: dequantiserar generellt alla kvarvarande `self_attn`-vikter
  vars modell-lager hålls i BF16. Utan den blir det ett nytt `KeyError` per projektion.
- Indexer-vikter hoppas över när indexern inte byggs (dense-läge, se nedan).

### `config.json`
Kopia av checkpointens `config.json` med **`index_topk: null`**, monterad över snapshotens fil.
Det stänger av sparse-indexern (`is_v32 = config.index_topk is not None`).

Nödvändigt eftersom sm121 (GB10) bara har **en** sparse-MLA-backend i det här bygget,
`FLASHINFER_MLA_SPARSE_SM120`, som kräver KV-cacheformatet `fp8_ds_mla`, vars CUDA-kernel
kräver `pe_dim == 64`. Checkpointen är NoPE (`qk_rope_head_dim = 0`), så kravet kan inte
uppfyllas. De andra sparse-backendsen är låsta till compute capability 9–10.

Dense-läget väljer `TRITON_MLA`, som stöder alla arkitekturer, bf16-cache och saknar
head-size-krav. Kostnaden är hastighet vid långa kontexter — inte kvalitet; dense attention
är den exakta varianten av det sparse approximerar.

**Vid byte av modellrevision:** uppdatera snapshot-hashen i `compose.yaml`.

### `workspace.py`
`WorkspaceManager.lock()` golvar varje lane till 256 MB (`VLLM_WORKSPACE_FLOOR_MB`) **innan**
den låser. Uppvärmningen dimensionerar aldrig FlashKDA:s prefill-buffert: den låses på ~32 MB,
och första riktiga prompten begär 89 MB → `AssertionError: Workspace is locked`.

Golvet läggs före låsningen i stället för att tillåta tillväxt efteråt — omallokering senare
kan dra undan buffertar som redan fångats i CUDA-grafer.

## Kontroll efter ändring

Kör alltid en prompt med verifierbart facit (se RECOVERY.md steg 6). Fel skalor ger text som
ser ut som text men innehåller nonsens — inga felmeddelanden.
