# Patch set: MiMo-V2.6-Flash (FP8) with tensor parallelism

Used automatically when `MODEL=XiaomiMiMo/MiMo-V2.6-Flash-RL`. `mimo_v2.py` is a copy of the
image's file with one function changed. It was taken from
`ghcr.io/spark-arena/dgx-vllm-eugr-nightly-tf5@sha256:1b0be62c1ccb37f738746cd8fbcec73b46f0c283082a6d46aebf9f19b5747563`
and must be rebased for another image.

## The bug

`_shard_fp8_qkv_proj` splits the fused FP8 `qkv_proj` weight across TP ranks. It assumed
the checkpoint interleaves Q, K and V per KV group (`[Q_1 K_1 V_1 | Q_2 K_2 V_2 | …]`), but
the model's own code (`modeling_mimo_v2.py` in the checkpoint) splits the fused output with
`qkv.split([q_size, k_size, v_size])`: the layout is `[Q_all | K_all | V_all]`. At TP=2:

- **Sliding-window layers crash** at load, because the per-group slicing of the scale blocks
  doesn't fit (`The size of tensor a (1856) must match the size of tensor b (1792)`).
- With only that fixed (our first version of this patch, 29/9), the model **loaded and
  answered with fluent nonsense**: every rank got the wrong rows of Q, K and V.

## The fix

Each rank takes its share of the heads from each of Q, K and V and returns them as
`[Q_r | K_r | V_r]`. At TP=2 those shares start and end on 128-row scale blocks in both kinds
of layer (Q 6144 rows; K 768 or 384; V 512 or 256), so the FP8 weights and scales are sliced
as they are, with no requantization. Tested on CPU against the real checkpoint, with the
reference split the way the model's own code splits it: the difference is exactly zero for
both ranks and both layer types. If the shares don't align with the blocks (another TP size),
the function falls back to dequantizing the share and quantizing it again.
