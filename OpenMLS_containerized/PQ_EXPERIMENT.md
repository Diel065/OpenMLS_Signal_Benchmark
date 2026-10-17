# Matched MLS crypto profiles

The existing benchmark and native `worker` support two profiles:

| `--crypto-profile` | MLS ciphersuite |
| --- | --- |
| `classical` (default) | `MLS_128_DHKEMX25519_AES128GCM_SHA256_Ed25519` (`0x0001`) |
| `hybrid-pq` | `MLS_128_MLKEM768X25519_AES128GCM_SHA256_Ed25519` (`0x004f`, provisional) |

Both use OpenMLS 0.8.1 and `OpenMlsRustCrypto` 0.5.1, the same operation bodies,
profiling boundaries, and workload planner. The hybrid changes the KEM to
ML-KEM-768 + X25519 (X-Wing); AES-128-GCM, SHA-256, and Ed25519 remain matched.
The suite is TBD1 in [draft-ietf-mls-pq-ciphersuites-06](https://datatracker.ietf.org/doc/html/draft-ietf-mls-pq-ciphersuites-06),
an active Internet-Draft, not a standardized MLS ciphersuite. Authentication still
uses Ed25519.

## Selection

Add `--crypto-profile hybrid-pq` to the existing
`scripts/run_compose_benchmark.py` invocation. Its default is `classical`.
The option is forwarded to the Compose workers, runner, and existing ADB/SSH
native workers. Direct native launches accept the same option on both `worker`
and `benchmark_runner_http_staircase`; workers also accept
`OPENMLS_CRYPTO_PROFILE` for Compose. Preflight rejects a worker whose profile or
exact suite disagrees with the runner. Use separate run IDs for the two runs.
All other workload and resource options retain their existing meanings.

JSONL and `events.csv` schema 14 add `crypto_profile` and `mls_ciphersuite`;
canonical operation names and all existing fields retain their meanings.
`scenario_plan.json` records both factors, including for native runs.
`benchmark_run_metadata.json` schema 6 records the selected profile and source
provenance. Existing R analysis entry points reject mixed crypto configurations
before grouping; no comparison plots are added.

## Source pin and historical data

The repository-local source is retained with a selective mapping/dispatch backport
from OpenMLS commit
[`be2368a21ec5e0380abd568821addc57b79013ea`](https://github.com/openmls/openmls/tree/be2368a21ec5e0380abd568821addc57b79013ea).
Only the selected suite is backported behind
`draft-ietf-mls-pq-ciphersuites` in OpenMLS, traits, and RustCrypto. The existing
locked `hpke-rs`/`hpke-rs-rust-crypto` 0.6.0 already implement X-Wing natively;
no primitive code or dependencies are replaced. Their locked `x-wing`
0.1.0-rc.0 and `ml-kem` 0.3.0-rc.0 are prerelease dependencies.
`profiling-json` remains enabled. See `openmls-main/UPSTREAM_REVISION.json`.

The previous source is exactly identified by benchmark commit
`710b8974b3a40a3bac00fb85a3b07e8cc9589fb4`, OpenMLS subtree
`32ea5afbf466ee6729df7a4a1814f1419289ae31`. Its original upstream commit was not
recorded; an upstream revision is not inferred from its package version.

Historical measurements stay historical. The PQ-capable build advertises one
additional two-byte ciphersuite in each leaf's default capabilities under both
profiles and emits additional profiling metadata. Numerical equivalence with old
classical results is not claimed. For a controlled comparison, rerun **both**
profiles from the same branch commit, lockfile, binary, platform, resource treatment,
and workload settings, including baseline, constrained, and external-device lanes.
Do not compare historical classical data with new PQ data as a KEM-only experiment.

## Focused validation

`cargo test --locked --test crypto_profiles` exercises the benchmark `Client`
workflow for both profiles: decoded KeyPackage suite and actual HPKE public-key
length, group suite, Add/Welcome, existing-member Commit processing, converged
state, application traffic, SelfUpdate, Remove, removed-member exclusion, and
rejoin. It also verifies the default and rejects a mixed-suite addition.

The task validation uses small ordinary and remove/rejoin staircases; no campaigns,
publication results, or plots are regenerated. Exact build sizes and validation
results are recorded in `PQ_VALIDATION.md`.
