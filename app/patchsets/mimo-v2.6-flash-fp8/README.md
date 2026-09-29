# Patch set: MiMo-V2.6-Flash (FP8) with tensor parallelism

Used automatically when `MODEL=XiaomiMiMo/MiMo-V2.6-Flash-RL`. `mimo_v2.py` is a copy of the
image's file with one function changed. It was taken from
`ghcr.io/spark-arena/dgx-vllm-eugr-nightly-tf5@sha256:1b0be62c1ccb37f738746cd8fbcec73b46f0c283082a6d46aebf9f19b5747563`
and must be rebased for another image.

## The bug

`_shard_fp8_qkv_proj` splits the fused FP8 `qkv_proj` weight across TP ranks when a rank gets
more than one KV head. It dequantizes each KV group with its block scales, reorders the rows
into `[Q… | K… | V…]` and quantizes again.

The 128×128 scale blocks run over the whole concatenated weight, but the function slices the
scales per group as if every group started on a block boundary
(`scale_rows_per_group = s_full.shape[0] // num_kv_heads`). A group is not a whole number of
blocks:

| Layers | KV heads | Rows per group | Blocks per group |
|---|---|---|---|
| sliding window | 8 | 1856 | 14.5 |
| full attention | 4 | 3392 | 26.5 |

With TP=2 on two DGX Sparks:

- **Sliding-window layers crash** at load: `The size of tensor a (1856) must match the size of
  tensor b (1792)`.
- **Full-attention layers load silently wrong**: every group after the first gets the wrong
  scales. Measured against the checkpoint, the weights are off by 29 % (rank 0) and 56 %
  (rank 1), which would give fluent nonsense.

## The fix

Expand the row scales for the whole weight first, then take each group's rows from that.
Tested on CPU against the real checkpoint: the resharded weights match a reference
dequantization within 0.6–1.6 %, the normal noise of quantizing back to FP8.
