# Signal measurement interpretation

Launch and build instructions are in the repository README.

The benchmark uses independent pairwise PQXDH sessions. Sending performs one encryption per recipient; reception is local to each recipient. Participant stores persist across plateaus. Initial session messages use `PreKeySignalMessage`; subsequent application messages use `SignalMessage`.

`span_layer` and `measurement_class` distinguish libsignal internals, benchmark wrappers and lifecycle/service work. The measured pairwise workflow rows are `session_establish_pair_wrapper`, `pairwise_fanout_send_wrapper` and `pairwise_fanout_receive_wrapper`. Use workflow IDs, pair indices/counts, success, roles and population to select complete operations. Do not add inclusive child spans to their parents or interpret registration/prekey service work as message encryption.

Handshake metadata distinguishes classical one-time prekeys, signed prekeys, one-time PQ prekeys and PQ last-resort paths. `pq_prekey_type=one_time` and `last_resort` are different paths. Recipients maintain their own OPK stock: initial stock 16, refill batch 16, low watermark 4. Settings are `SIGNAL_INITIAL_ONE_TIME_PREKEY_COUNT`, `SIGNAL_ONE_TIME_PREKEY_REFILL_COUNT` and `SIGNAL_ONE_TIME_PREKEY_LOW_WATERMARK`. Stock/refill fields record that behavior.

Signal has no MLS epoch. Interpret `ratchet_progression_value` through `ratchet_progression_kind`. Message/previous counters, ratchet-key fingerprints, chain matches, DH/root-chain updates, send/receive chain indices, skipped-key counts and SPQR flags describe actual progression.

Internal protocol spans expose thread CPU, allocation bytes/count and allocation scope. Wrapper process CPU measures the defined pairwise workflow. `cpu_envelope_utilization` and `cpu_throttled_time_ratio` retain their existing layer-specific meanings; the wrapper throttling defect documented in the root README is intentionally unchanged.

Docker/cgroup resource evidence, OOM classification and allocator heap-budget crossings are separate. Crossing a heap budget does not reject the allocation: the worker reports the latched logical failure after its command. Resource limits, failure sidecars, run status, device identities and binary hashes must accompany performance or feasibility interpretation.
