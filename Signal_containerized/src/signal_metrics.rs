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
    pub cpu_envelope_utilization: Option<f64>,
    #[serde(default)]
    pub cpu_throttled_time_ratio: Option<f64>,
    pub alloc_bytes: Option<u64>,
    pub alloc_count: Option<u64>,
    #[serde(default)]
    pub ram_rss_delta_bytes: Option<i64>,
    #[serde(default)]
    pub ram_rss_utilization: Option<f64>,
    pub artifact_size_bytes: Option<usize>,
    pub participant_count: Option<usize>,
    pub conversation_size: Option<usize>,
    pub prekey_bundle_count: Option<usize>,
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
    pub span_layer: String,
    pub protocol_stack: String,
    pub implementation: String,
    pub measurement_class: String,
    pub event_family: String,
    pub event_subtype: String,
    pub success: bool,
    pub error_class: Option<String>,
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
    pub cpu_envelope_utilization: Option<f64>,
    pub cpu_throttled_time_ratio: Option<f64>,
    pub alloc_bytes: Option<u64>,
    pub alloc_count: Option<u64>,
    pub ram_rss_delta_bytes: Option<i64>,
    pub ram_rss_utilization: Option<f64>,
    pub artifact_size_bytes: Option<usize>,
    pub participant_count: Option<usize>,
    pub conversation_size: Option<usize>,
    pub prekey_bundle_count: Option<usize>,
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
}
