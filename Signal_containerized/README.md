# Signal measurement interpretation

Launch and build instructions are in the repository README.

The benchmark uses independent pairwise PQXDH sessions. Sending performs one encryption per recipient; reception is local to each recipient. Participant stores persist across plateaus. Initial session messages use `PreKeySignalMessage`; subsequent application messages use `SignalMessage`.

`span_layer` and `measurement_class` distinguish libsignal internals, benchmark wrappers and lifecycle/service work. The measured pairwise workflow rows are `session_establish_pair_wrapper`, `pairwise_fanout_send_wrapper` and `pairwise_fanout_receive_wrapper`. Use workflow IDs, pair indices/counts, success, roles and population to select complete operations. Do not add inclusive child spans to their parents or interpret registration/prekey service work as message encryption.

Handshake metadata distinguishes classical one-time prekeys, signed prekeys, one-time PQ prekeys and PQ last-resort paths. `pq_prekey_type=one_time` and `last_resort` are different paths. Recipients maintain their own OPK stock: initial stock 16, refill batch 16, low watermark 4. Settings are `SIGNAL_INITIAL_ONE_TIME_PREKEY_COUNT`, `SIGNAL_ONE_TIME_PREKEY_REFILL_COUNT` and `SIGNAL_ONE_TIME_PREKEY_LOW_WATERMARK`. Stock/refill fields record that behavior.

Signal has no MLS epoch. Interpret `ratchet_progression_value` through `ratchet_progression_kind`. Message/previous counters, ratchet-key fingerprints, chain matches, DH/root-chain updates, send/receive chain indices, skipped-key counts and SPQR flags describe actual progression.

Internal protocol spans expose thread CPU, allocation bytes/count and allocation scope. Wrapper process CPU measures the defined pairwise workflow. `cpu_envelope_utilization` and `cpu_throttled_time_ratio` retain their existing layer-specific meanings; the wrapper throttling defect documented in the root README is intentionally unchanged.

Docker/cgroup resource evidence, OOM classification and allocator heap-budget crossings are separate. Crossing a heap budget does not reject the allocation: the worker reports the latched logical failure after its command. Resource limits, failure sidecars, run status, device identities and binary hashes must accompany performance or feasibility interpretation.

## Native Sender Keys workload

Select `--group-model sender-keys` on `scripts/run_compose_benchmark.py` or
`benchmark_runner_http_staircase`. The default is `--group-model pairwise`.
The runner archives `group_model` as `pairwise` or `sender_keys` in its scenario
plan; the orchestrator also records it in run metadata. Aggregation attaches this
run factor to every `events.csv` row, including unchanged libsignal spans.
Historical traces without this factor remain pairwise; Sender Keys traces without
an explicit Sender Keys scenario plan are rejected.

Sender Keys uses the existing participant, in-memory store, RNG, PQXDH code,
repository, relay and native worker. Each application send invokes `group_encrypt`
once and publishes the resulting ciphertext once with all recipients in the relay
header. Each recipient invokes `group_decrypt` once. The native distribution APIs
are `create_sender_key_distribution_message` and
`process_sender_key_distribution_message`. Distribution bytes are encrypted with
`message_encrypt`, relayed as pairwise ciphertext, decrypted with `message_decrypt`,
then parsed and processed. Distribution and application deliveries have distinct
conversation identifiers. No Sender Key secret is sent in plaintext.

At a join, existing endpoints establish any missing pairwise initiator sessions
using the unchanged `EstablishSessions` command and send current Sender Key
snapshots. The joiner's `signal_sender_key_onboarding.total` covers its local
PQXDH responder/pairwise decrypt work, processing all snapshots, creating its own
fresh Sender Key and encrypting/publishing its distribution to existing members.
Existing endpoints process that distribution separately before traffic resumes.
Initial prekey provisioning and subsequent stock maintenance remain separate,
like OpenMLS KeyPackage preparation. Rejoining retains pairwise sessions and uses
a fresh local Sender Key generation. Existing snapshots start at their current
chain iteration; they do not grant earlier-message access.

The benchmark-defined removal policy is
`eager_all_senders_fresh_generation`, recorded in run metadata and the scenario
plan. Following each removal batch, every remaining sender discards obsolete
Sender Key storage, creates a fresh generation and distributes it through retained
pairwise sessions only to remaining members. All required distributions are
processed before application traffic resumes. This also applies after classified
resource attrition. Interrupted refresh attempts restart with fresh generations;
obsolete queued application ciphertexts are acknowledged and discarded at this
barrier, and obsolete distributions are pairwise-decrypted and discarded before
the expected generation is processed. This synchronous policy does not model
Signal Messenger's membership policy or offline delivery. The native primitives
have no membership-policy API. The policy follows the fresh-key redistribution
principle described in [RFC 9420, introduction](https://www.rfc-editor.org/rfc/rfc9420.html#section-1)
and is validated against the vendored `group_cipher.rs` and `tests/groups.rs`.
Distribution UUIDs encode checked, increasing runner workflow identifiers; they
are unique within one run without deriving cryptographic secrets from those IDs.
Fresh workers are required per run, as in the existing benchmark.

For complete command process CPU, select the existing `measurement_plane=wrapper_total`
rows: `pairwise_fanout_send_wrapper` / `pairwise_fanout_receive_wrapper` or
`sender_key_send_wrapper` / `sender_key_receive_wrapper`. Sum pairwise send rows
within one participant/workflow; Sender Keys has one send row. Canonical
`signal_application_message_create.total` and
`signal_application_message_receive.total` also remain available. Their API CPU and
allocation metrics use the existing synchronous `measure_profile` scopes; wall
time includes local relay waits. Sender Keys onboarding and distribution totals
use the same scopes, summing only that endpoint's measured local API work.
Full command CPU includes its HTTP/bookkeeping work. These layers must not be mixed.

`signal_sender_key_distribution_send.total` and
`signal_sender_key_distribution_receive.total` retain join/distribution components.
`signal_sender_key_removal_refresh.total` measures one remaining endpoint's outgoing
refresh; that endpoint's incoming distribution totals are separate. Group local
maintenance components by participant, plateau and removal phase when studying
complete endpoint maintenance. Do not attribute other endpoints' work to that
participant. Totals are inclusive; child spans explain them and must not be added
again. Packed members execute the same state transitions without contributing
isolated measurements. The existing pairwise diagnostics fail closed on Sender
Keys input; this extension does not implement publication analysis or figures.

Focused validation, with binaries built using `cargo build --profile minsize --bins`:

```sh
cargo test --all-targets
python3 scripts/smoke_group_models.py --bin-dir target/minsize
```

The smoke uses populations 2–4, repeated joins/removals, packed participants,
the external profiling interface on the local host, and a classified heap failure.
`--baseline-worker PATH` additionally compares pairwise command structure and
state progression with a pre-extension worker. Local interface tests do not
constitute ARM hardware measurements. ADB/SSH deployment, provenance, affinity,
quotas, heap controls and failure classification use the existing infrastructure.

Extension build check: `x86_64-unknown-linux-gnu`, `minsize` profile, rustc 1.95.0.
The worker built from base `710b8974b3a40a3bac00fb85a3b07e8cc9589fb4` was
2,575,416 bytes; the extended worker is 2,661,032 bytes. Both were freshly built
with identical target/profile settings. These are build artifacts, not new
performance findings. The `aarch64-unknown-linux-musl` worker also passed a Cargo
cross-target check; no ARM hardware workload was executed.
