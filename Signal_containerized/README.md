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

## Benchmark Schema And Validation

Signal events are Signal-native protocol measurements. They are not MLS epochs, commits, proposals, or tree transitions.

Current event families:

- `RegisterParticipant`: participant identity object is present in the worker process. This is lifecycle/bootstrap bookkeeping and is not interpreted as libsignal cryptographic cost.
- `PublishPrekeyBundle`: local identity, signed prekey, Kyber prekey, and one-time prekey records are stored in the participant protocol store. This separates device key-material preparation from later publication/session work.
- `GeneratePrekeyBundle`: libsignal prekey bundle material is generated/serialized and published to the benchmark key repository. Output records include `prekey_bundle_count` and `artifact_size_bytes`.
- `EstablishSessions`: initiator-side session setup from fetched prekey bundles using libsignal `process_prekey_bundle`. Output records include `participant_count`, `conversation_size`, `prekey_bundle_count`, `session_count`, and `ratchet_step_count`; this event is initiator-side protocol work plus separately visible key-repository fetch bytes, not container startup.
- `EncryptMessage`: send-side Signal message protection using libsignal `message_encrypt`, followed by relay publication. Output records include plaintext/ciphertext sizes, session count, and ratchet progression count.
- `DecryptMessage`: receive-side Signal message recovery using libsignal `message_decrypt`, after relay fetch/deserialization. Output records include plaintext/ciphertext sizes and receive-side ratchet progression count.
- `ProcessPending`: wrapper/relay draining work used for backlog cleanup. It aggregates message metrics and must not be treated as a single cryptographic protocol operation.
- `RemoveParticipants` and `ShowParticipantState`: lifecycle/state-inspection events. They are operational metadata, not protocol-cost measurements.

Every raw profile event and aggregated `events.csv` row carries explicit event attribution: `profile_schema_version`, `protocol_stack`, `measurement_class`, participant/device id, role, peer id/device/count where applicable, event family/subtype, side, direction, phase, and success state. Aggregated rows also carry the worker-layout context needed for OpenMLS infrastructure comparison: logical/physical worker counts, layout mode, execution backend, device kind, transport, access backend, architecture, Rust target, physical worker id, and resource envelope columns (`resource_limit_cpus`, memory/swap bytes, pids limit, and `resource_profile`). External devices explicitly emit empty resource-limit fields because host Docker cgroup caps do not describe real-device capacity.

## Resource Envelope And Telemetry

`run_compose_benchmark.py` and `generate_compose.py` accept these singleton-container resource flags:

```bash
--singleton-cpus 0.25
--singleton-memory 128m
--singleton-memory-swap 128m
--singleton-pids-limit 128
```

The generator writes the envelope into `worker_layout.json` and applies it to every containerized singleton worker service. Packed workers and real devices remain unconstrained and are marked with empty resource-limit fields.

A run writes these additional audit artifacts:

- `benchmark_run_metadata.json`: run configuration, host CPU/platform data, Git provenance for this benchmark and libsignal, Docker version/info, and external-device binary hashes when available.
- `resource_limits_verified.json`: Docker inspect verification that requested singleton caps were applied.
- `resource_samples.jsonl`: cgroup samples for constrained singleton containers while the runner is active.
- `resource_summary.csv`: per-container memory, CPU throttling, pids, OOM, and exit-status summary.
- `benchmark_outcome.json`: success/failure class with resource-pressure evidence when applicable.

A configured resource envelope that cannot be verified is an invalid run. The orchestrator writes `benchmark_outcome.json` with `invalid_resource_envelope` and stops instead of producing ambiguous benchmark output.

## Migration Notes

This refactor is additive for raw Signal profile JSONL and aggregated CSV consumers. `events.csv` gained resource-cap columns and run-level resource/provenance artifacts. Existing event names are preserved, but analysis should treat lifecycle/helper events separately from libsignal protocol-layer events listed above.

For comparable OpenMLS/Signal infrastructure experiments, use the same worker layout mode, bridge count, health/fanout settings, and singleton resource envelope flags on both stacks. Compare resource and orchestration metrics by shared infrastructure columns; compare cryptographic behavior only through each stack's protocol-native event families.
