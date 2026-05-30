# OpenMLS Containerized Benchmark

## External IoT Devices

For a LuckFox Pico Plus over USB RNDIS:

```bash
adb devices
ip -br addr
rustup target add armv7-unknown-linux-musleabihf
RUSTFLAGS='-C linker=rust-lld' cargo build --profile minsize --target armv7-unknown-linux-musleabihf --bin worker
```

Then edit `devices.yaml`: set `connection.serial` from `adb devices`,
`transport.device_ip` to the Pico IP, and `transport.host_ip` to the Ubuntu
RNDIS IP. External device orchestration reads YAML, so the Python environment
running `scripts/run_compose_benchmark.py` must have `pyyaml` installed.

`--build-images` builds Docker images only. The ARM `worker` binary must already
exist locally at the path configured by `devices.yaml`, currently
`target/armv7-unknown-linux-musleabihf/minsize/worker`, or remotely at
`worker.remote_binary`.

For a Raspberry Pi 5 over SSH on the LAN:

```bash
rustup target add aarch64-unknown-linux-musl
RUSTFLAGS='-C linker=rust-lld' cargo build --profile minsize --target aarch64-unknown-linux-musl --bin worker
ssh diel@192.168.178.33 'uname -m; hostname'
```

The SSH backend supports either `connection.identity_file` or
`connection.password` in `devices.yaml`. Password-based SSH uses Python
`pexpect`, which is available in the project environment used for these runs.
The Raspberry Pi entry writes under `/home/diel/openmls-benchmark` so it does
not require root-owned `/results` or `/opt` directories on the device.

## Runner Example

OpenMLS, 512 active participants with the Pico and Raspberry Pi 5 included:

```bash
OPENMLS_SERVICE_METRICS_WARN_IN_FLIGHT=512 \
.venv/bin/python scripts/run_compose_benchmark.py \
  --workers 512 \
  --worker-layout-mode hybrid \
  --singleton-min-count 64 \
  --singleton-fraction 0.125 \
  --singleton-selection-strategy evenly-spaced \
  --packed-clients-per-container 16 \
  --packed-worker-internal-parallelism 16 \
  --bridge-count 4 \
  --build-images \
  --force-cleanup-mls-ports \
  --runner-in-docker \
  --ds-delivery-mode group-log \
  --process-pending-fanout \
  --fanout-adaptive \
  --max-fanout-parallelism 128 \
  --min-fanout-parallelism 16 \
  --fanout-error-rate-threshold 0.01 \
  --fanout-p95-threshold-ms 8000 \
  --http-pool-max-idle-per-host 64 \
  --runner-http-connect-timeout-ms 5000 \
  --runner-http-request-timeout-ms 120000 \
  --worker-http-pool-max-idle-per-host 64 \
  --worker-http-connect-timeout-ms 5000 \
  --worker-http-request-timeout-ms 45000 \
  --worker-outbound-http-permits 32 \
  --compose-parallel-limit 48 \
  --startup-batch-size 64 \
  --startup-batch-sleep-seconds 0.5 \
  --post-startup-settle-seconds 10 \
  --health-timeout-seconds 240 \
  --health-poll-seconds 0.5 \
  --worker-health-timeout-seconds 600 \
  --worker-health-poll-ms 250 \
  --compose-down-timeout-seconds 2 \
  --teardown-batch-size 64 \
  --teardown-batch-sleep-seconds 0.1 \
  --min-size 2 \
  --max-size 512 \
  --step-size 256 \
  --roundtrips 1 \
  --update-rounds 1 \
  --app-rounds 1 \
  --max-update-samples-per-plateau 1 \
  --max-app-samples-per-payload 1 \
  --payload-sizes 32 \
  --devices-file devices.yaml \
  --enable-external-devices \
  --external-device luckfox-pico-plus-01 \
  --external-device raspberry-pi-01 \
  --external-coverage-lane \
  --wipe-device-run-dirs
```

With `--workers 512` and two external devices enabled, the Pico and Raspberry
Pi 5 are inserted after the leader in runner order. A `--max-size 512` plateau
includes both real devices and leaves two Docker workers idle.
`--enable-external-devices` requires `--devices-file`; `--external-device` can
be repeated to select multiple devices. `--external-coverage-lane` keeps active
external devices sampled as profiled actors/recipients in the small verification
runs.
