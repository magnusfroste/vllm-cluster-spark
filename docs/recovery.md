# Recovery and troubleshooting

Start with the app. The **Setup** card lists everything that is missing on each node, with the
command to run. This page covers what the app can't do for you: reinstalling a node, and the
failures we have actually seen.

## Reinstalling a node from scratch

1. Download the OS image for your unit (DGX OS, or the vendor image for OEM units such as the
   ASUS Ascent GX10).
2. Write it to a **USB stick**. On the GX10 the boot menu did not list an external SSD, but a
   USB stick in a USB 3 adapter worked.
   ```bash
   lsblk                                   # find the right device, e.g. /dev/sdb
   sudo dd if=image.iso of=/dev/sdX bs=4M status=progress conv=fsync
   ```
3. Boot from the stick and install. Use the same username on every node, since the app logs in
   as `SSH_USER` everywhere.
4. If internet goes over wifi, turn off wifi power saving. Otherwise long model downloads lose
   the connection:
   ```bash
   sudo tee /etc/NetworkManager/conf.d/90-wifi-powersave-off.conf >/dev/null <<'EOF'
   [connection]
   wifi.powersave = 2
   EOF
   sudo systemctl restart NetworkManager
   ```
5. Open the app, run **Configuration → Install the agent** on the node, and work through the
   Setup card: link IP, Docker group, then **Download model** and **Start**.

## Known failures

### NCCL "unhandled system error" at startup, after a node rebooted
The RoCE v2 GID on index 3 is empty on one node. It disappears when the link flaps, for example
when the other node reboots, and `NCCL_IB_GID_INDEX=3` needs it. The app checks for this
before every start and shows the fix: disconnect and reconnect the link interfaces with
`nmcli`. Run it over a different network than the link itself (wifi or the LAN port).
```bash
cat /sys/class/infiniband/*/ports/1/gids/3     # must not be all zeros on the active ports
```

### Startup hangs in the FlashInfer autotune, then a gloo timeout after 30 min
With a saved autotune cache the ranks fall out of step. The entrypoint the app writes clears
`/root/.cache/vllm/flashinfer_autotune_cache` on every start, and the autotune then takes about
a minute. The app's stall watchdog restarts the cluster if the log goes silent.

### The head node runs out of memory (kernel OOM killer, NVRM "Out of memory")
CPU and GPU share the same memory on DGX Spark. The head also runs Easypanel and whatever else
is on it, so it has less to spare than the workers. Lower `GPU_MEM_UTIL` or `MAX_MODEL_LEN`,
or stop services on the head (a desktop session, remote desktop tools, a browser). Remote
desktop tools that probe the hardware video encoder at startup log NVRM "Out of memory"
warnings when vLLM holds most of the memory.

### Model download hangs with no error
On DGX Spark both xet and `hf_transfer` have hung on large downloads: the process sleeps with
no open sockets and 0 % CPU. The app's download turns both off. Judge progress by the bytes on
disk, which the Setup card shows, not by whether the process is alive. Changing download
backend throws away partial files.

### Nodes run different image IDs
Ray requires identical builds on every node. Pin `VLLM_IMAGE` to a digest
(`image@sha256:…`) and press **Pull image**. The status card warns when the IDs differ.

### Wifi drops and does not come back (NetworkManager "no-secrets")
When a WPA handshake breaks during roaming, NetworkManager treats it as a wrong password,
drops the key and gives up. It never retries on its own. Reconnect with
`nmcli con up <connection>` in a local session. Locking the connection to one band
(`802-11-wireless.band a`) removes the trigger. This does not affect inference, which only
uses the cluster link, but model downloads and the public API need the internet.

## Checking the model after a change

Text coming out is not enough: wrong dequantization gives fluent nonsense. Use the app's test
prompt with a checkable answer, for example *"What is the capital of Sweden, and what is
17*23?"* → Stockholm and 391.
