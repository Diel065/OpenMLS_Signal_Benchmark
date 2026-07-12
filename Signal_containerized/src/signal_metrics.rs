use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SignalProfileEvent {
    #[serde(default)]
    pub profile_schema_version: u32,
    pub ts_unix_ns: u128,
    pub op: String,
    #[serde(default)]
    pub span_layer: String,
    #[serde(default)]
    pub protocol_stack: String,
    pub implementation: String,
    #[serde(default)]
    pub measurement_class: String,
    #[serde(default)]
    pub event_family: String,
    #[serde(default)]
    pub event_subtype: String,
    #[serde(default)]
    pub error_class: Option<String>,
    #[serde(default)]
    pub runner_event_kind: Option<String>,
    #[serde(default)]
    pub failed_worker_id: Option<String>,
    #[serde(default)]
    pub failed_physical_worker_id: Option<String>,
    #[serde(default)]
    pub failure_detail: Option<String>,
    #[serde(default)]
    pub failure_evidence_source: Option<String>,
    #[serde(default)]
    pub failure_evidence_detail: Option<String>,
    #[serde(default)]
    pub failure_action: Option<String>,
    #[serde(default)]
    pub reassigned_to_worker_id: Option<String>,
    #[serde(default)]
    pub benchmark_plateau_index: Option<usize>,
    #[serde(default)]
    pub benchmark_target_size: Option<usize>,
    #[serde(default)]
    pub benchmark_active_size: Option<usize>,
    #[serde(default)]
    pub benchmark_phase: Option<String>,
    #[serde(default)]
    pub benchmark_operation: Option<String>,
    #[serde(default)]
    pub benchmark_operation_seq: Option<usize>,
    #[serde(default)]
    pub benchmark_payload_size: Option<usize>,
    #[serde(default)]
    pub benchmark_workflow_id: Option<u64>,
    #[serde(default)]
    pub workflow_pair_index: Option<u32>,
    #[serde(default)]
    pub workflow_pair_count: Option<u32>,
    #[serde(default)]
    pub new_session_established: Option<bool>,
    #[serde(default)]
    pub span_id: Option<u64>,
    #[serde(default)]
    pub parent_span_id: Option<u64>,
    #[serde(default)]
    pub parent_operation: Option<String>,
    #[serde(default)]
    pub span_name: Option<String>,
    #[serde(default)]
    pub span_kind: Option<String>,
    #[serde(default)]
    pub measurement_plane: Option<String>,
    #[serde(default)]
    pub span_inclusive: Option<bool>,
    #[serde(default)]
    pub worker_id: Option<String>,
    #[serde(default)]
    pub global_span_id: Option<String>,
    #[serde(default)]
    pub parent_global_span_id: Option<String>,
    #[serde(default)]
    pub request_id: Option<String>,
    #[serde(default)]
    pub participant_id: Option<String>,
    #[serde(default)]
    pub participant_device_id: Option<u32>,
    #[serde(default)]
    pub role: Option<String>,
    #[serde(default)]
    pub peer_id: Option<String>,
    #[serde(default)]
    pub peer_device_id: Option<u32>,
    #[serde(default)]
    pub pair_id: Option<String>,
    #[serde(default)]
    pub peer_count: Option<usize>,
    #[serde(default)]
    pub event_side: Option<String>,
    #[serde(default)]
    pub direction: Option<String>,
    #[serde(default)]
    pub phase: Option<String>,
    #[serde(default)]
    pub success: bool,
    pub wall_ns: u128,
    pub cpu_thread_ns: Option<u128>,
    #[serde(default)]
    pub cpu_process_ns: Option<u128>,
    #[serde(default)]
    pub cpu_envelope_utilization: Option<f64>,
    #[serde(default)]
    pub cpu_throttled_time_ratio: Option<f64>,
    pub alloc_bytes: Option<u64>,
    pub alloc_count: Option<u64>,
    #[serde(default)]
    pub alloc_measurement_scope: Option<String>,
    #[serde(default)]
    pub l1d_cache_accesses: Option<u64>,
    #[serde(default)]
    pub l1d_cache_misses: Option<u64>,
    #[serde(default)]
    pub ram_rss_delta_bytes: Option<i64>,
    #[serde(default)]
    pub ram_rss_utilization: Option<f64>,
    pub artifact_size_bytes: Option<usize>,
    pub participant_count: Option<usize>,
    pub conversation_size: Option<usize>,
    pub prekey_bundle_count: Option<usize>,
    #[serde(default)]
    pub prekey_stock_before: Option<usize>,
    #[serde(default)]
    pub prekey_stock_after: Option<usize>,
    #[serde(default)]
    pub prekey_refill_count: Option<usize>,
    #[serde(default)]
    pub prekey_refill_trigger: Option<String>,
    pub session_count: Option<usize>,
    pub ratchet_step_count: Option<usize>,
    pub ciphertext_bytes: Option<usize>,
    pub plaintext_bytes: Option<usize>,
    #[serde(default)]
    pub handshake_protocol: Option<String>,
    #[serde(default)]
    pub handshake_side: Option<String>,
    #[serde(default)]
    pub classical_one_time_prekey_present: Option<bool>,
    #[serde(default)]
    pub classical_one_time_prekey_id: Option<u32>,
    #[serde(default)]
    pub signed_prekey_id: Option<u32>,
    #[serde(default)]
    pub pq_prekey_id: Option<u32>,
    #[serde(default)]
    pub pq_prekey_type: Option<String>,
    #[serde(default)]
    pub pq_prekey_signature_present: Option<bool>,
    #[serde(default)]
    pub ciphertext_message_type: Option<String>,
    #[serde(default)]
    pub message_counter: Option<u32>,
    #[serde(default)]
    pub previous_counter: Option<u32>,
    #[serde(default)]
    pub sender_ratchet_key_fingerprint: Option<String>,
    #[serde(default)]
    pub receiver_chain_matched: Option<bool>,
    #[serde(default)]
    pub dh_ratchet_performed: Option<bool>,
    #[serde(default)]
    pub root_chain_updated: Option<bool>,
    #[serde(default)]
    pub send_chain_index_before: Option<u32>,
    #[serde(default)]
    pub send_chain_index_after: Option<u32>,
    #[serde(default)]
    pub receive_chain_index_before: Option<u32>,
    #[serde(default)]
    pub receive_chain_index_after: Option<u32>,
    #[serde(default)]
    pub skipped_message_keys_used: Option<u32>,
    #[serde(default)]
    pub skipped_message_keys_stored: Option<u32>,
    #[serde(default)]
    pub spqr_step_performed: Option<bool>,
    #[serde(default)]
    pub ratchet_progression_kind: Option<String>,
    #[serde(default)]
    pub ratchet_progression_value: Option<u64>,
    pub pid: u32,
    pub thread_id: String,
    pub run_id: Option<String>,
    pub scenario: Option<String>,
    #[serde(default)]
    pub scenario_seed: Option<u64>,
    pub node_name: Option<String>,
    pub pod_name: Option<String>,
    pub device_kind: Option<String>,
    pub execution_backend: Option<String>,
    #[serde(default)]
    pub cpu_model: Option<String>,
    #[serde(default)]
    pub requested_cpu_fraction: Option<f64>,
    #[serde(default)]
    pub applied_cpu_fraction: Option<f64>,
    #[serde(default)]
    pub cpu_period_us: Option<u64>,
    #[serde(default)]
    pub cpu_quota_us: Option<u64>,
    #[serde(default)]
    pub cgroup_cpu_max: Option<String>,
    #[serde(default)]
    pub cpuset_cpus_requested: Option<String>,
    #[serde(default)]
    pub cpuset_cpus_effective: Option<String>,
    #[serde(default)]
    pub cpu_nr_periods: Option<u64>,
    #[serde(default)]
    pub cpu_nr_throttled: Option<u64>,
    #[serde(default)]
    pub cpu_throttled_usec: Option<u64>,
    #[serde(default)]
    pub memory_model: Option<String>,
    #[serde(default)]
    pub requested_memory_limit: Option<String>,
    #[serde(default)]
    pub requested_memory_limit_bytes: Option<u64>,
    #[serde(default)]
    pub applied_memory_limit_bytes: Option<u64>,
    #[serde(default)]
    pub memory_current_bytes: Option<u64>,
    #[serde(default)]
    pub memory_peak_bytes: Option<u64>,
    #[serde(default)]
    pub memory_events_max: Option<u64>,
    #[serde(default)]
    pub memory_events_oom: Option<u64>,
    #[serde(default)]
    pub memory_events_oom_kill: Option<u64>,
    #[serde(default)]
    pub app_heap_budget: Option<String>,
    #[serde(default)]
    pub app_heap_budget_bytes: Option<u64>,
    #[serde(default)]
    pub heap_current_live_bytes: Option<u64>,
    #[serde(default)]
    pub heap_peak_live_bytes: Option<u64>,
    #[serde(default)]
    pub heap_operation_peak_live_bytes: Option<u64>,
    #[serde(default)]
    pub heap_total_allocated_bytes: Option<u64>,
    #[serde(default)]
    pub heap_allocation_count: Option<u64>,
    #[serde(default)]
    pub heap_deallocation_count: Option<u64>,
    #[serde(default)]
    pub heap_failed_allocation_size_bytes: Option<u64>,
    #[serde(default)]
    pub heap_failure_context: Option<String>,
    #[serde(default)]
    pub resource_profile_id: Option<String>,
    #[serde(default)]
    pub resource_profile_index: Option<i32>,
    #[serde(default)]
    pub failure_class: Option<String>,
    #[serde(default)]
    pub failure_operation: Option<String>,
    #[serde(default)]
    pub failure_span: Option<String>,
    #[serde(default)]
    pub failure_phase: Option<String>,
    #[serde(default)]
    pub container_exit_code: Option<i32>,
    #[serde(default)]
    pub oom_killed: Option<bool>,
}

#[derive(Debug, Clone, Serialize)]
pub struct SignalCsvRow<'a> {
    pub client_id: &'a str,
    pub worker_id: &'a str,
    pub physical_worker_id: &'a str,
    pub container_mode: &'a str,
    pub execution_backend: &'a str,
    pub device_kind: &'a str,
    pub transport: &'a str,
    pub access_backend: &'a str,
    pub arch: &'a str,
    pub rust_target: &'a str,
    pub profile_schema_version: u32,
    pub ts_unix_ns: u128,
    pub op: String,
    pub span_name: String,
    pub span_layer: String,
    pub protocol_stack: String,
    pub implementation: String,
    pub measurement_class: String,
    pub event_family: String,
    pub event_subtype: String,
    pub success: bool,
    pub error_class: Option<String>,
    pub runner_event_kind: Option<String>,
    pub failed_worker_id: Option<String>,
    pub failed_physical_worker_id: Option<String>,
    pub failure_detail: Option<String>,
    pub failure_evidence_source: Option<String>,
    pub failure_evidence_detail: Option<String>,
    pub failure_action: Option<String>,
    pub reassigned_to_worker_id: Option<String>,
    pub benchmark_plateau_index: Option<usize>,
    pub benchmark_target_size: Option<usize>,
    pub benchmark_active_size: Option<usize>,
    pub benchmark_phase: Option<String>,
    pub benchmark_operation: Option<String>,
    pub benchmark_operation_seq: Option<usize>,
    pub benchmark_payload_size: Option<usize>,
    pub benchmark_workflow_id: Option<u64>,
    pub workflow_pair_index: Option<u32>,
    pub workflow_pair_count: Option<u32>,
    pub new_session_established: Option<bool>,
    pub span_id: Option<u64>,
    pub parent_span_id: Option<u64>,
    pub parent_operation: Option<String>,
    pub span_kind: Option<String>,
    pub measurement_plane: Option<String>,
    pub span_inclusive: Option<bool>,
    pub global_span_id: Option<String>,
    pub parent_global_span_id: Option<String>,
    pub profiling_worker_id: Option<String>,
    pub request_id: Option<String>,
    pub participant_id: Option<String>,
    pub participant_device_id: Option<u32>,
    pub role: Option<String>,
    pub peer_id: Option<String>,
    pub peer_device_id: Option<u32>,
    pub pair_id: Option<String>,
    pub peer_count: Option<usize>,
    pub event_side: Option<String>,
    pub direction: Option<String>,
    pub phase: Option<String>,
    pub wall_ns: u128,
    pub cpu_thread_ns: Option<u128>,
    pub cpu_process_ns: Option<u128>,
    pub cpu_envelope_utilization: Option<f64>,
    pub cpu_throttled_time_ratio: Option<f64>,
    pub alloc_bytes: Option<u64>,
    pub alloc_count: Option<u64>,
    pub alloc_measurement_scope: Option<String>,
    pub l1d_cache_accesses: Option<u64>,
    pub l1d_cache_misses: Option<u64>,
    pub ram_rss_delta_bytes: Option<i64>,
    pub ram_rss_utilization: Option<f64>,
    pub artifact_size_bytes: Option<usize>,
    pub participant_count: Option<usize>,
    pub conversation_size: Option<usize>,
    pub prekey_bundle_count: Option<usize>,
    pub prekey_stock_before: Option<usize>,
    pub prekey_stock_after: Option<usize>,
    pub prekey_refill_count: Option<usize>,
    pub prekey_refill_trigger: Option<String>,
    pub session_count: Option<usize>,
    pub ratchet_step_count: Option<usize>,
    pub ciphertext_bytes: Option<usize>,
    pub plaintext_bytes: Option<usize>,
    pub handshake_protocol: Option<String>,
    pub handshake_side: Option<String>,
    pub classical_one_time_prekey_present: Option<bool>,
    pub classical_one_time_prekey_id: Option<u32>,
    pub signed_prekey_id: Option<u32>,
    pub pq_prekey_id: Option<u32>,
    pub pq_prekey_type: Option<String>,
    pub pq_prekey_signature_present: Option<bool>,
    pub ciphertext_message_type: Option<String>,
    pub message_counter: Option<u32>,
    pub previous_counter: Option<u32>,
    pub sender_ratchet_key_fingerprint: Option<String>,
    pub receiver_chain_matched: Option<bool>,
    pub dh_ratchet_performed: Option<bool>,
    pub root_chain_updated: Option<bool>,
    pub send_chain_index_before: Option<u32>,
    pub send_chain_index_after: Option<u32>,
    pub receive_chain_index_before: Option<u32>,
    pub receive_chain_index_after: Option<u32>,
    pub skipped_message_keys_used: Option<u32>,
    pub skipped_message_keys_stored: Option<u32>,
    pub spqr_step_performed: Option<bool>,
    pub ratchet_progression_kind: Option<String>,
    pub ratchet_progression_value: Option<u64>,
    pub pid: u32,
    pub thread_id: String,
    pub run_id: Option<String>,
    pub scenario: Option<String>,
    pub scenario_seed: Option<u64>,
    pub node_name: Option<String>,
    pub pod_name: Option<String>,
    pub logical_worker_count: usize,
    pub physical_worker_count: usize,
    pub singleton_count: usize,
    pub packed_clients_per_container: usize,
    pub layout_mode: &'a str,
    pub resource_limit_cpus: Option<f64>,
    pub resource_limit_memory: Option<&'a str>,
    pub resource_limit_memory_bytes: Option<u64>,
    pub resource_limit_memory_swap: Option<&'a str>,
    pub resource_limit_memory_swap_bytes: Option<u64>,
    pub resource_limit_pids: Option<u64>,
    pub resource_profile: &'a str,
    pub cpu_model: Option<String>,
    pub requested_cpu_fraction: Option<f64>,
    pub applied_cpu_fraction: Option<f64>,
    pub cpu_period_us: Option<u64>,
    pub cpu_quota_us: Option<u64>,
    pub cgroup_cpu_max: Option<String>,
    pub cpuset_cpus_requested: Option<String>,
    pub cpuset_cpus_effective: Option<String>,
    pub cpu_nr_periods: Option<u64>,
    pub cpu_nr_throttled: Option<u64>,
    pub cpu_throttled_usec: Option<u64>,
    pub memory_model: Option<String>,
    pub requested_memory_limit: Option<String>,
    pub requested_memory_limit_bytes: Option<u64>,
    pub applied_memory_limit_bytes: Option<u64>,
    pub memory_current_bytes: Option<u64>,
    pub memory_peak_bytes: Option<u64>,
    pub memory_events_max: Option<u64>,
    pub memory_events_oom: Option<u64>,
    pub memory_events_oom_kill: Option<u64>,
    pub app_heap_budget: Option<String>,
    pub app_heap_budget_bytes: Option<u64>,
    pub heap_current_live_bytes: Option<u64>,
    pub heap_peak_live_bytes: Option<u64>,
    pub heap_operation_peak_live_bytes: Option<u64>,
    pub heap_total_allocated_bytes: Option<u64>,
    pub heap_allocation_count: Option<u64>,
    pub heap_deallocation_count: Option<u64>,
    pub heap_failed_allocation_size_bytes: Option<u64>,
    pub heap_failure_context: Option<String>,
    pub resource_profile_id: Option<String>,
    pub resource_profile_index: Option<i32>,
    pub failure_class: Option<String>,
    pub failure_operation: Option<String>,
    pub failure_span: Option<String>,
    pub failure_phase: Option<String>,
    pub container_exit_code: Option<i32>,
    pub oom_killed: Option<bool>,
}
