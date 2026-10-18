# PQ extension validation — 2026-10-17

Reference: local `main` at `710b8974b3a40a3bac00fb85a3b07e8cc9589fb4`.
The protocol snapshot remains 0.8.1 with the selective upstream backport documented
in `PQ_EXPERIMENT.md` and `openmls-main/UPSTREAM_REVISION.json`.

## Checks

- 61 library tests, one worker test, and two stateful crypto-profile tests pass.
  Command: `cargo test --locked --lib --bins --test crypto_profiles`.
- Ordinary native staircases `2,4,6,4,2`, 32-byte payload, one update and send per
  plateau: reference, new classical, and hybrid each produce 790 rows and 74
  operation names. All eight canonical totals are exercised. Operation/child names,
  classifications, inclusive flags, parent-ID edges and parent labels, allocation
  scopes, and metric availability match. Role, population, epoch, tree/path,
  proposal, HPKE-counter, and sampling metadata match exactly across all rows.
  Timing/allocation values and serialized artifact values are not required equal.
- `ProfileScope` timer/counter capture code is byte-identical to `main`. All
  instrumented MLS operation/child-span bodies remain unchanged. Allocator,
  cgroup-counter, heap-baseline, and resource-control code is retained. Heap budgets
  follow the configuration-time baseline used by current code and the paper.
- Both profiles pass ordinary and remove/rejoin runs, plus six logical clients in
  two measured singletons and two packed workers. Actual decoded KeyPackages have
  the selected suite and 32-byte / 1216-byte HPKE public keys; group-state suites,
  convergence, plaintext recovery, removal, and rejoin are checked.
- Both profiles pass 10 MiB heap-budget runs. A one-byte budget preserves
  `app_heap_budget_exceeded` attribution to KeyPackage creation, including
  `operation_entry` crossings. Synthetic failure JSONL, normal aggregation, and
  fallback failure CSV also retain both crypto fields.
- Both profiles pass small Docker runs at 0.25 CPU with six distinct CPU affinities.
  Effective affinities are checked through `/proc`; Docker quota evidence is
  retained. Cgroup counters are present, and CPU-envelope/throttled-time formulas
  agree with the emitted counters. Temporary validation containers/images removed.
- The existing output validator accepts ordinary, packed, heap, and quota traces.
  Wrong worker profiles, noncanonical profile names, and corrupted event factors
  are rejected. R guards accept either profile separately and reject mixed or
  unlabeled new inputs. Python compilation and R syntax checks pass.
- Native `x86_64-unknown-linux-gnu` development binaries build. Static musl worker
  builds pass for x86_64, AArch64, and ARMv7 EABI hard-float; ARM builds use the
  installed Rust toolchain's `rust-lld`. No physical ARM execution was performed.
  All locked dependency versions and checksums remain unchanged.

Validation artifacts and temporary drivers are under `/tmp/pq-mls-validation`
and `/tmp/pq-mls-*.{py,json,log}` in this workspace session. These are focused
validation runs, not publication campaigns. No plots or manuscript results changed.

## Binary bytes

Toolchain: `rustc 1.95.0 (59807616e 2026-04-14)`. The publication-profile x86_64
before/after pairs were freshly built from the reference and branch with identical
compiler settings. The resulting worker supports both profiles in one binary.

| Target | Profile | Binary | Before bytes | After bytes |
| --- | --- | --- | ---: | ---: |
| x86_64-unknown-linux-musl | release | `benchmark_runner_http_staircase` | 3054016 | 3070432 |
| x86_64-unknown-linux-musl | release | `ds` | 1286848 | 1286848 |
| x86_64-unknown-linux-musl | release | `message_relay` | 1114784 | 1114784 |
| x86_64-unknown-linux-musl | minsize | `worker` | 3048544 | 3056768 |
| aarch64-unknown-linux-musl | minsize | `worker` | 2515552 (cached) | 2520832 |
| armv7-unknown-linux-musleabihf | minsize | `worker` | 2452872 (cached) | 2455656 |

ARM “before” entries are pre-existing cached artifacts, preserved before building;
their source provenance was not independently established. They are byte inventories,
not controlled estimates of PQ overhead. ARM “after” entries are verified builds.

Other built development binaries (`x86_64-unknown-linux-gnu`, `dev`):

| Binary | Before bytes | After bytes |
| --- | ---: | ---: |
| `worker` | 114625832 | 120465216 |
| `benchmark_runner_http_staircase` | 100973168 | 101202008 |
| `aggregate_profiles` | 91746456 | 91763480 |
| `ds` | 94090704 | 99812232 |
| `message_relay` | 92256912 | 97975864 |

No historical numerical equivalence is asserted: capability advertisement and
profiling metadata changed. Both profiles require matched reruns for baseline,
heap/CPU treatments, and external-device comparisons. Remaining qualifications are
the imported snapshot's unrecorded original upstream SHA (its exact Git tree is
recorded), the draft suite/provisional code point and prerelease crypto dependencies,
and physical-board runtime validation. No architecture build failure remains.

Changed implementation files are the Cargo manifests/lockfile; upstream trait
mappings, RustCrypto dispatch, default capabilities and profiler metadata;
`src/{crypto_profile,client,staircase_runner,lib}.rs` and the worker/runner binaries;
`scripts/{generate_compose,run_compose_benchmark,external_devices,validate_benchmark_outputs,resource_experiment_failures}.py`;
the two current R analysis entry points and their shared crypto guard; the stateful
integration test; source provenance and these two documents. Workload planning,
resource controls, libsignal, historical datasets and manuscripts are untouched.
