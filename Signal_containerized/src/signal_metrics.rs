use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignalProfileEvent {
    #[serde(default)]
    pub profile_schema_version: u32,
    pub ts_unix_ns: u128,
    pub op: String,
    #[serde(default)]
    pub protocol_stack: String,
    pub implementation: String,
    #[serde(default)]
    pub measurement_class: String,
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
    pub peer_count: Option<usize>,
    #[serde(default)]
    pub event_family: String,
    #[serde(default)]
    pub event_subtype: String,
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
    pub alloc_bytes: Option<u64>,
    pub alloc_count: Option<u64>,
    pub artifact_size_bytes: Option<usize>,
    pub participant_count: Option<usize>,
    pub conversation_size: Option<usize>,
    pub prekey_bundle_count: Option<usize>,
    pub session_count: Option<usize>,
    pub ratchet_step_count: Option<usize>,
    pub ciphertext_bytes: Option<usize>,
    pub plaintext_bytes: Option<usize>,
    pub pid: u32,
    pub thread_id: String,
    pub run_id: Option<String>,
    pub scenario: Option<String>,
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
    pub protocol_stack: String,
    pub implementation: String,
    pub measurement_class: String,
    pub participant_id: Option<String>,
    pub participant_device_id: Option<u32>,
    pub role: Option<String>,
    pub peer_id: Option<String>,
    pub peer_device_id: Option<u32>,
    pub peer_count: Option<usize>,
    pub event_family: String,
    pub event_subtype: String,
    pub event_side: Option<String>,
    pub direction: Option<String>,
    pub phase: Option<String>,
    pub success: bool,
    pub wall_ns: u128,
    pub cpu_thread_ns: Option<u128>,
    pub alloc_bytes: Option<u64>,
    pub alloc_count: Option<u64>,
    pub artifact_size_bytes: Option<usize>,
    pub participant_count: Option<usize>,
    pub conversation_size: Option<usize>,
    pub prekey_bundle_count: Option<usize>,
    pub session_count: Option<usize>,
    pub ratchet_step_count: Option<usize>,
    pub ciphertext_bytes: Option<usize>,
    pub plaintext_bytes: Option<usize>,
    pub pid: u32,
    pub thread_id: String,
    pub run_id: Option<String>,
    pub scenario: Option<String>,
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
