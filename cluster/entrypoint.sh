#!/bin/bash
# ROLE=head: startar Ray-head, väntar på worker, kör vLLM (OpenAI-API på :8000)
# ROLE=worker: ansluter till head och blockerar
set -euo pipefail

# Kör FlashInfer-autotunen från noll varje start. Med en sparad cache träffar
# rankerna olika och hamnar i otakt: gloo-väntan på den ena, NCCL-väntan på den
# andra, och timeout efter 30 min (23–27/9). Utan cache tar tuningen ~1 min.
rm -rf /root/.cache/vllm/flashinfer_autotune_cache

if [ "$ROLE" = "worker" ]; then
  until ray start --block --address="${HEAD_IP}:6379" --node-ip-address="${HOST_IP}"; do
    echo "Väntar på head ${HEAD_IP}..."; sleep 5
  done
  exit 0
fi

ray start --head --node-ip-address="${HOST_IP}" --port=6379 --dashboard-host=127.0.0.1
echo "Väntar på ${NUM_NODES} noder i Ray-klustret..."
until [ "$(python3 -c 'import ray; ray.init(address="auto", logging_level="ERROR"); print(sum(n["Alive"] for n in ray.nodes()))' 2>/dev/null)" -ge "${NUM_NODES}" ]; do
  sleep 5
done
echo "Alla noder uppe – startar vLLM med ${MODEL}"

exec vllm serve "${MODEL}" \
  --tensor-parallel-size "${NUM_NODES}" \
  --distributed-executor-backend ray \
  --host 0.0.0.0 --port 8000 \
  --api-key "${VLLM_API_KEY}" \
  ${VLLM_EXTRA_ARGS:-}
