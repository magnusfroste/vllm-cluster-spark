# Patch set: GLM-5.3-Flash NVFP4 on the eugr nightly image

Used automatically when `MODEL=local-inference-lab/GLM-5.3-Flash-NVFP4-Spark` (see `set.json`).
Managed mode writes the files to `patches/` in the cluster dir on every node and mounts them
read-only over the image's own files.

The files are copies of the image's files with the changes below. They were taken from
`ghcr.io/spark-arena/dgx-vllm-eugr-nightly-tf5@sha256:1b0be62c1ccb37f738746cd8fbcec73b46f0c283082a6d46aebf9f19b5747563`.
**With a different image they must be rebased**, or they overwrite newer code. Pin `VLLM_IMAGE`
to that digest.

## The underlying problem

The build assumes that linear attention (KDA) and MLA run in **BF16**. The checkpoint
`local-inference-lab/GLM-5.3-Flash-NVFP4-Spark` ships **MXFP8** there:

- weights `F8_E4M3`
- scales `U8` in **E8M0**: the value is `2^(v-127)`, not `v` (typical values 115–118 → ~1e-4)
- block shape **[1, 32]** (one scale per 32 elements along the input), not square [128, 128]

The experts are NVFP4 and the dense MLP in layers 0–2 is BF16, so it is a mixed checkpoint.

## The files

### `kda.py`
Removes `vllm_config.quant_config = None` around `super().__init__()`. The original comment says
*"KDA projections remain BF16 because fp8 checkpoints omit their scales"*. This checkpoint has
the scales, so the layers should be quantized.

### `modelopt.py`
`_resolve_quant_algo`: `fused_projection_shards` extended with
`in_proj_qkvbfg_a` → (`q_proj`, `k_proj`, `v_proj`, `b_proj`, `f_a_proj`, `g_a_proj`) and
`wk_weights_proj` → (`wk`, `weights_proj`).

Matching is on the **tail** of the module path (`layers.N.self_attn.q_proj`), because
`hf_quant_config.json` uses the prefix `model.language_model.…` while vLLM looks the layer up
as `language_model.model.…`.

### `model.py`
- `_dequant_fp8_block` derives the block shape from the scale tensor's shape instead of assuming
  square blocks, and reads `uint8` scales as E8M0.
- `_try_load_fp8_attn_proj` and `_try_load_fp8_indexer_wk` leave layers that really are
  quantized to the normal path (they have `weight_scale`, not just `weight_scale_inv`).
- New `_try_load_mxfp8_as_bf16`: dequantizes all remaining `self_attn` weights whose model layer
  is kept in BF16. Without it every projection raises its own `KeyError`.
- Indexer weights are skipped when the indexer is not built (dense mode, see below).

### `config.json`
A copy of the checkpoint's `config.json` with **`index_topk: null`**, mounted over the snapshot's
file. It turns off the sparse indexer (`is_v32 = config.index_topk is not None`).

This is needed because sm121 (GB10) has only **one** sparse MLA backend in this build,
`FLASHINFER_MLA_SPARSE_SM120`. It requires the KV cache format `fp8_ds_mla`, whose CUDA kernel
requires `pe_dim == 64`. The checkpoint is NoPE (`qk_rope_head_dim = 0`), so that can't be met.
The other sparse backends are limited to compute capability 9–10.

Dense mode picks `TRITON_MLA`, which supports every architecture and a bf16 cache and has no
head size requirement. The cost is speed at long contexts, not quality: dense attention is the
exact version of what sparse approximates.

The mount path contains the snapshot hash, so `set.json` pins the model `revision`. The app
downloads that revision and passes `--revision` to vLLM.

### `workspace.py`
`WorkspaceManager.lock()` floors every lane at 256 MB (`VLLM_WORKSPACE_FLOOR_MB`) **before** it
locks. The warmup never sizes FlashKDA's prefill buffer: it is locked at ~32 MB, and the first
real prompt asks for 89 MB → `AssertionError: Workspace is locked`.

The floor goes in before the lock rather than allowing growth afterwards, since reallocating
later can pull buffers out from under CUDA graphs that already captured them.

## Checking after a change

Always run a prompt with a checkable answer, for example *"What is the capital of Sweden, and
what is 17*23?"* → Stockholm and 391. Wrong scales give text that reads like text but is
nonsense, with no error message.

Reference values from a good start (2 nodes, `GPU_MEM_UTIL=0.83`, `MAX_MODEL_LEN=262144`):
the model takes 86.25 GiB per node, the KV cache holds ~344k tokens, and the log says
`Using TRITON_MLA attention backend`.
