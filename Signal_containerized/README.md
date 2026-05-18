# Signal Containerized Benchmark

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

## Runner Example

Signal, 512 active participants with the Pico included:

```bash
SIGNAL_SERVICE_METRICS_WARN_IN_FLIGHT=512 \
python3 scripts/run_compose_benchmark.py \
  --workers 512 \
  --worker-layout-mode hybrid \
  --singleton-min-count 64 \
  --singleton-fraction 0.125 \
  --singleton-selection-strategy evenly-spaced \
  --packed-clients-per-container 16 \
  --packed-worker-internal-parallelism 16 \
  --bridge-count 4 \
  --build-images \
  --force-cleanup-signal-ports \
  --runner-in-docker \
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
  --app-rounds 1 \
  --max-app-samples-per-payload 1 \
  --payload-sizes 32 \
  --devices-file devices.yaml \
  --enable-external-devices \
  --external-device luckfox-pico-plus-01 \
  --wipe-device-run-dirs
```

With `--workers 512` and one external device enabled, the Pico is inserted after
the leader in runner order. A `--max-size 512` plateau includes the Pico and
leaves one Docker worker idle. `--enable-external-devices` requires
`--devices-file`; `--external-device` can be repeated to select multiple devices.
