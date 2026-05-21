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

## Benchmark Semantics

The Signal benchmark is a pairwise fan-out benchmark. Each application send encrypts one pairwise Signal message per recipient and publishes those messages through the relay. It is not a Sender Keys group messaging benchmark unless a future runner explicitly switches to Sender Keys and labels those rows separately.

Current session establishment uses PQXDH. Classical X3DH-only handshakes are not faked; rows should be filtered with `handshake_protocol=pqxdh` when studying setup cost. The key repository distinguishes classical one-time prekeys, signed classical prekeys, one-time signed PQ prekeys, and the signed PQ last-resort prekey. Each recipient device uploads a small initial stock for itself. The server hands out one-time classical and one-time PQ material first, then falls back to no classical one-time prekey plus the PQ last-resort prekey. After a recipient accepts an inbound initial session, its worker performs its own low-watermark stock check and uploads a measured OPK refill batch when its published stock is low; the runner does not pre-generate key material for the future staircase size.

Scientific protocol rows are emitted from `libsignal-main` with `profile_schema_version=3`, `span_layer=libsignal_main`, and `measurement_class=protocol`. Wrapper rows are emitted by the benchmark worker with `span_layer=benchmark_wrapper` and `measurement_class=wrapper`; they include queueing, HTTP, relay, serialization, repository, and command bookkeeping and must not be compared to OpenMLS library-internal spans.

Protocol event names:

- `pqxdh_initiator_process_bundle_protocol`: initiator processes a fetched PQXDH prekey bundle inside libsignal.
- `pqxdh_responder_receive_prekey_message_protocol`: responder processes the first `PreKeySignalMessage` and establishes its session.
- `signal_update_opks_generate_protocol`: recipient generates its own OPK/PQ-OPK publication batch through the libsignal profiling layer for initial upload or low-watermark refill.
- `signal_message_encrypt_protocol`: libsignal message encryption entry point.
- `signal_message_decrypt_protocol`: libsignal ordinary `SignalMessage` decrypt entry point.
- `signal_ratchet_send_chain_advance`: send-chain key advancement.
- `signal_ratchet_receive_chain_advance`: receive-chain key advancement.
- `signal_ratchet_dh_step`: DH ratchet step; only this span sets `dh_ratchet_performed=true` and `root_chain_updated=true`.
- `signal_ratchet_spqr_send` and `signal_ratchet_spqr_recv`: SPQR ratchet work on send and receive.
- `signal_message_aead_encrypt` and `signal_message_aead_decrypt`: AEAD protection/recovery for message payloads.

Non-protocol wrapper/helper event names include `participant_register_lifecycle`, `prekey_store_local_material`, `prekey_publish_bundle_batch_repository_io`, `prekey_update_opks_repository_io`, `session_establish_pair_wrapper`, `pairwise_fanout_send_wrapper`, `pairwise_fanout_receive_wrapper`, `relay_drain_wrapper`, `participant_state_inspection_wrapper`, and `participant_remove_lifecycle`. Treat these as operational rows, not cryptographic protocol costs.

To filter scientific rows from `events.csv`, require:

```text
profile_schema_version == 3
span_layer == libsignal_main
measurement_class == protocol
success == true
```

For protocol rows, `participant_id`, `participant_device_id`, `peer_id`, `peer_device_id`, `pair_id`, `role`, `direction`, wall time, thread CPU time, allocation bytes/count, and protocol-path metadata are required for interpretation.

## Ratchet Metadata

Signal has no MLS epoch. `ratchet_progression_value` is a protocol-local Double Ratchet value and must be interpreted through `ratchet_progression_kind`:

- `send_chain_index_before` / `send_chain_index_after`: send chain index around ordinary message key derivation.
- `receive_chain_index_before` / `receive_chain_index_after`: receive chain index around inbound message key derivation.
- `message_counter`: counter carried in the Signal message.
- `previous_counter`: previous-chain counter carried in the Signal message.
- `sender_ratchet_key_fingerprint`: fingerprint of the message sender ratchet public key.
- `receiver_chain_matched`: whether an existing receiver chain matched the sender ratchet key.
- `dh_ratchet_performed`: true only on an actual DH ratchet step.
- `root_chain_updated`: true only when the root key changes.
- `skipped_message_keys_used` / `skipped_message_keys_stored`: skipped-key behavior during receive-chain advancement.
- `spqr_step_performed`: whether an SPQR send/receive step ran.
- `ciphertext_message_type`: `PreKeySignalMessage`, `SignalMessage`, `SenderKeyMessage`, or `Unknown`.

The deprecated `ratchet_step_count` column is retained only for old-schema compatibility and is not populated by new wrapper rows.

## Prekey And Ciphertext Metadata

PQXDH rows expose `classical_one_time_prekey_present`, `classical_one_time_prekey_id`, `signed_prekey_id`, `pq_prekey_id`, `pq_prekey_type`, and `pq_prekey_signature_present`. `pq_prekey_type=one_time` and `pq_prekey_type=last_resort` are separate paths and must not be pooled unless the analysis explicitly intends to combine them.

OPK stock rows expose `prekey_stock_before`, `prekey_stock_after`, `prekey_refill_count`, and `prekey_refill_trigger`. The default recipient policy is an initial stock of 16, refill batches of 16, and a low watermark of 4. Docker workers can override that policy with `SIGNAL_INITIAL_ONE_TIME_PREKEY_COUNT`, `SIGNAL_ONE_TIME_PREKEY_REFILL_COUNT`, and `SIGNAL_ONE_TIME_PREKEY_LOW_WATERMARK`.

Initial session setup is explicit: the initiator processes a bundle, encrypts a deterministic initial message, and the responder decrypts that `PreKeySignalMessage`. Later application messages should appear as ordinary `SignalMessage` rows.

## Resource Envelope And Telemetry

`run_compose_benchmark.py` and `generate_compose.py` accept these singleton-container resource flags:

```bash
--singleton-cpus 0.25
--singleton-memory 128m
--singleton-memory-swap 128m
--singleton-pids-limit 128
```

The generator writes the envelope into `worker_layout.json` and applies it to every containerized singleton worker service. Packed workers and real devices remain unconstrained and are marked with empty Docker resource-limit fields.

Operation-local protocol rows include thread CPU time, allocation bytes/count, process CPU envelope utilization, throttling ratio, RSS delta, and RSS utilization where the local platform exposes those counters. Run-level resource artifacts are separate:

- `benchmark_run_metadata.json`: run configuration, host CPU/platform data, Git provenance for this benchmark and libsignal, Docker version/info, and external-device binary hashes when available.
- `resource_limits_verified.json`: Docker inspect verification that requested singleton caps were applied.
- `resource_samples.jsonl`: cgroup samples for constrained singleton containers while the runner is active.
- `resource_summary.csv`: per-container memory, CPU throttling, pids, OOM, and exit-status summary.
- `benchmark_outcome.json`: success/failure class with resource-pressure evidence when applicable.

A configured resource envelope that cannot be verified is an invalid run. The orchestrator writes `benchmark_outcome.json` with `invalid_resource_envelope` and stops instead of producing ambiguous benchmark output.

## External Device Metrics

External devices such as LuckFox Pico Plus and Raspberry Pi workers are real-device backends, not Docker cgroups. Their layout rows include execution backend, device kind, transport, access backend, architecture, and Rust target, while Docker resource-cap fields remain empty. Device-local CPU/RAM samples are separate artifacts when external resource sampling is enabled; do not compare them as if they were Docker cgroup metrics.

## Migration Notes

Schema version 3 is not poolable with older Signal output directories. Older outputs may use command names such as `EncryptMessage`, `DecryptMessage`, or `EstablishSessions`, may mix wrapper wall time with narrower CPU/allocation measurements, and may contain synthetic ratchet counts. Use only schema-3 `libsignal_main` protocol rows for OpenMLS-vs-Signal protocol-cost analysis.

For comparable infrastructure experiments, use the same worker layout mode, bridge count, health/fanout settings, and singleton resource envelope flags on both stacks. Compare resource and orchestration metrics through shared infrastructure columns; compare cryptographic behavior only through each stack's protocol-native event families.
