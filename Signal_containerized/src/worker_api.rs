use std::{
    collections::{HashMap, VecDeque},
    error::Error as StdError,
    path::PathBuf,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::{anyhow, Context, Result};
use cpu_time::{ProcessTime, ThreadTime};
use libsignal_core::DeviceId;
use libsignal_protocol::kem;
use libsignal_protocol::{
    KyberPreKeyId, PreKeyBundle, PreKeyId, PreKeySignalMessage, SignalMessage, SignedPreKeyId,
};
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};

use crate::http_retry::{
    is_connect_stage_reqwest_error, is_transient_reqwest_error, is_transient_status,
    retry_transient_http_async, RetryDecision,
};
use crate::key_repository::{
    OneTimePrekeyStorable, PrekeyBundleBatchStorable, PrekeyBundleStorable, PrekeyStock,
};
use crate::l1d_cache::{L1DCacheCounterScope, L1DCacheCounts};
use crate::signal_metrics::SignalProfileEvent;
use crate::signal_participant::SignalParticipant;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "cmd", rename_all = "snake_case")]
pub enum Command {
    RegisterParticipant,
    GeneratePrekeyBundle,
    PublishPrekeyBundle,
    UpdateOneTimePrekeys,
    EstablishSessions {
        participants: Vec<String>,
        #[serde(default)]
        conversation_size: Option<usize>,
    },
    EncryptMessage {
        recipient: String,
        message: String,
        #[serde(default)]
        conversation_size: Option<usize>,
    },
    DecryptMessage {
        sender: String,
        profile: bool,
        #[serde(default)]
        conversation_size: Option<usize>,
        #[serde(default)]
        expected_plaintext_bytes: Option<usize>,
    },
    ProcessPending {
        max_messages: Option<usize>,
    },
    ShowParticipantState,
    RemoveParticipants {
        participants: Vec<String>,
    },
}

impl Command {
    pub fn kind(&self) -> &'static str {
        match self {
            Command::RegisterParticipant => "RegisterParticipant",
            Command::GeneratePrekeyBundle => "GeneratePrekeyBundle",
            Command::PublishPrekeyBundle => "PublishPrekeyBundle",
            Command::UpdateOneTimePrekeys => "UpdateOneTimePrekeys",
            Command::EstablishSessions { .. } => "EstablishSessions",
            Command::EncryptMessage { .. } => "EncryptMessage",
            Command::DecryptMessage { .. } => "DecryptMessage",
            Command::ProcessPending { .. } => "ProcessPending",
            Command::ShowParticipantState => "ShowParticipantState",
            Command::RemoveParticipants { .. } => "RemoveParticipants",
        }
    }

    pub fn is_mutating(&self) -> bool {
        matches!(
            self,
            Command::RegisterParticipant
                | Command::GeneratePrekeyBundle
                | Command::PublishPrekeyBundle
                | Command::UpdateOneTimePrekeys
                | Command::EstablishSessions { .. }
                | Command::EncryptMessage { .. }
                | Command::DecryptMessage { .. }
                | Command::ProcessPending { .. }
                | Command::RemoveParticipants { .. }
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommandRequestEnvelope {
    pub request_id: String,
    pub command: Command,
    #[serde(default)]
    pub phase: Option<String>,
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
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum IncomingCommandRequest {
    Envelope(CommandRequestEnvelope),
    Raw(Command),
}

#[derive(Debug, Clone)]
pub struct RequestEnvelopeParts {
    pub request_id: Option<String>,
    pub command: Command,
    pub phase: Option<String>,
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
}

impl IncomingCommandRequest {
    pub fn into_parts(self) -> RequestEnvelopeParts {
        match self {
            IncomingCommandRequest::Envelope(envelope) => RequestEnvelopeParts {
                request_id: Some(envelope.request_id),
                command: envelope.command,
                phase: envelope.phase,
                benchmark_plateau_index: envelope.benchmark_plateau_index,
                benchmark_target_size: envelope.benchmark_target_size,
                benchmark_active_size: envelope.benchmark_active_size,
                benchmark_phase: envelope.benchmark_phase,
                benchmark_operation: envelope.benchmark_operation,
                benchmark_operation_seq: envelope.benchmark_operation_seq,
                benchmark_payload_size: envelope.benchmark_payload_size,
                benchmark_workflow_id: envelope.benchmark_workflow_id,
                workflow_pair_index: envelope.workflow_pair_index,
                workflow_pair_count: envelope.workflow_pair_count,
            },
            IncomingCommandRequest::Raw(command) => RequestEnvelopeParts {
                request_id: None,
                command,
                phase: None,
                benchmark_plateau_index: None,
                benchmark_target_size: None,
                benchmark_active_size: None,
                benchmark_phase: None,
                benchmark_operation: None,
                benchmark_operation_seq: None,
                benchmark_payload_size: None,
                benchmark_workflow_id: None,
                workflow_pair_index: None,
                workflow_pair_count: None,
            },
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommandResponse {
    pub status: String,
    pub message: String,
}

impl CommandResponse {
    pub fn ok(message: impl Into<String>) -> Self {
        Self {
            status: "ok".to_string(),
            message: message.into(),
        }
    }

    pub fn error(message: impl Into<String>) -> Self {
        Self {
            status: "error".to_string(),
            message: message.into(),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct CommandMetrics {
    pub cpu_thread_ns: Option<u128>,
    pub cpu_process_ns: Option<u128>,
    pub alloc_bytes: Option<u64>,
    pub alloc_count: Option<u64>,
    pub l1d_cache_accesses: Option<u64>,
    pub l1d_cache_misses: Option<u64>,
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
    pub new_session_established: Option<bool>,
}

impl CommandMetrics {
    fn merge_message(&mut self, other: &CommandMetrics) {
        self.merge_profile(other);
        self.artifact_size_bytes =
            add_usize_options(self.artifact_size_bytes, other.artifact_size_bytes);
        self.prekey_bundle_count =
            add_usize_options(self.prekey_bundle_count, other.prekey_bundle_count);
        self.session_count = add_usize_options(self.session_count, other.session_count);
        self.ratchet_step_count =
            add_usize_options(self.ratchet_step_count, other.ratchet_step_count);
        self.ciphertext_bytes = add_usize_options(self.ciphertext_bytes, other.ciphertext_bytes);
        self.plaintext_bytes = add_usize_options(self.plaintext_bytes, other.plaintext_bytes);
    }

    fn merge_profile(&mut self, other: &CommandMetrics) {
        self.cpu_thread_ns = add_u128_options(self.cpu_thread_ns, other.cpu_thread_ns);
        self.cpu_process_ns = add_u128_options(self.cpu_process_ns, other.cpu_process_ns);
        self.alloc_bytes = add_u64_options(self.alloc_bytes, other.alloc_bytes);
        self.alloc_count = add_u64_options(self.alloc_count, other.alloc_count);
        self.l1d_cache_accesses =
            add_u64_options(self.l1d_cache_accesses, other.l1d_cache_accesses);
        self.l1d_cache_misses = add_u64_options(self.l1d_cache_misses, other.l1d_cache_misses);
    }
}

#[derive(Debug, Clone)]
pub struct CommandOutcome {
    pub message: String,
    pub metrics: CommandMetrics,
}

impl CommandOutcome {
    fn new(message: impl Into<String>, metrics: CommandMetrics) -> Self {
        Self {
            message: message.into(),
            metrics,
        }
    }

    fn message(message: impl Into<String>) -> Self {
        Self::new(message, CommandMetrics::default())
    }
}

fn add_u128_options(a: Option<u128>, b: Option<u128>) -> Option<u128> {
    match (a, b) {
        (Some(a), Some(b)) => Some(a.saturating_add(b)),
        (Some(a), None) => Some(a),
        (None, Some(b)) => Some(b),
        (None, None) => None,
    }
}

fn add_u64_options(a: Option<u64>, b: Option<u64>) -> Option<u64> {
    match (a, b) {
        (Some(a), Some(b)) => Some(a.saturating_add(b)),
        (Some(a), None) => Some(a),
        (None, Some(b)) => Some(b),
        (None, None) => None,
    }
}

fn add_usize_options(a: Option<usize>, b: Option<usize>) -> Option<usize> {
    match (a, b) {
        (Some(a), Some(b)) => Some(a.saturating_add(b)),
        (Some(a), None) => Some(a),
        (None, Some(b)) => Some(b),
        (None, None) => None,
    }
}

fn measure_profile<R>(run: impl FnOnce() -> R) -> (R, CommandMetrics) {
    let _ = L1DCacheCounterScope::counters_available();
    let process_start = ProcessTime::now();
    let cpu_start = ThreadTime::now();
    let mut result = None;
    let mut l1d_cache_counts = L1DCacheCounts::default();
    let allocation_info = allocation_counter::measure(|| {
        let l1d_cache_scope = L1DCacheCounterScope::start();
        result = Some(run());
        l1d_cache_counts = l1d_cache_scope
            .map(L1DCacheCounterScope::finish)
            .unwrap_or_default();
    });

    let cpu_thread_ns = cpu_start.elapsed().as_nanos();
    let cpu_process_ns = process_start.elapsed().as_nanos();
    let result = result.expect("allocation_counter measure closure did not run");

    (
        result,
        CommandMetrics {
            cpu_thread_ns: Some(cpu_thread_ns),
            cpu_process_ns: Some(cpu_process_ns),
            alloc_bytes: Some(allocation_info.bytes_total),
            alloc_count: Some(allocation_info.count_total),
            l1d_cache_accesses: l1d_cache_counts.accesses,
            l1d_cache_misses: l1d_cache_counts.misses,
            ..Default::default()
        },
    )
}

pub fn write_subspan_event(profile_path: Option<&PathBuf>, event: &SignalProfileEvent) {
    if let Some(path) = profile_path {
        if let Some(parent) = path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        if let Ok(mut file) = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
        {
            if let Ok(json_line) = serde_json::to_string(event) {
                let _ = std::io::Write::write(&mut file, json_line.as_bytes());
                let _ = std::io::Write::write(&mut file, b"\n");
            }
        }
    }
}

pub fn make_subspan_event(
    op: &str,
    event_family: &str,
    event_subtype: &str,
    span_layer: &str,
    measurement_class: &str,
    participant_id: &str,
    wall_ns: u128,
    cpu_thread_ns: Option<u128>,
    alloc_bytes: Option<u64>,
    alloc_count: Option<u64>,
    success: bool,
) -> SignalProfileEvent {
    make_subspan_event_with_span(
        op,
        event_family,
        event_subtype,
        span_layer,
        measurement_class,
        participant_id,
        wall_ns,
        cpu_thread_ns,
        alloc_bytes,
        alloc_count,
        success,
        None,
        None,
        None,
    )
}

pub fn make_subspan_event_with_span(
    op: &str,
    event_family: &str,
    event_subtype: &str,
    span_layer: &str,
    measurement_class: &str,
    participant_id: &str,
    wall_ns: u128,
    cpu_thread_ns: Option<u128>,
    alloc_bytes: Option<u64>,
    alloc_count: Option<u64>,
    success: bool,
    span_id: Option<u64>,
    parent_span_id: Option<u64>,
    parent_operation: Option<String>,
) -> SignalProfileEvent {
    let heap_snapshot = allocation_counter::embedded_heap_budget::snapshot();
    let worker_id = libsignal_protocol::profiling::current_worker_id();
    let global_span_id = worker_id
        .as_ref()
        .zip(span_id)
        .map(|(w, s)| format!("{}:{}", w, s));
    let parent_global_span_id = worker_id
        .as_ref()
        .zip(parent_span_id)
        .map(|(w, s)| format!("{}:{}", w, s));
    let bench_ctx = libsignal_protocol::profiling::current_benchmark_context();
    let request_id = bench_ctx.as_ref().and_then(|c| c.request_id.clone());
    SignalProfileEvent {
        profile_schema_version: 5,
        span_id,
        parent_span_id,
        parent_operation,
        span_name: Some(op.to_string()),
        span_kind: if op.contains(".total") || event_subtype.ends_with(".total") {
            Some("total".to_string())
        } else {
            Some("subspan".to_string())
        },
        measurement_plane: if op.contains(".total") {
            Some("protocol_total".to_string())
        } else {
            Some("wrapper_child".to_string())
        },
        span_inclusive: Some(true),
        worker_id,
        global_span_id,
        parent_global_span_id,
        request_id,
        benchmark_plateau_index: bench_ctx.as_ref().and_then(|c| c.benchmark_plateau_index),
        benchmark_target_size: bench_ctx.as_ref().and_then(|c| c.benchmark_target_size),
        benchmark_active_size: bench_ctx.as_ref().and_then(|c| c.benchmark_active_size),
        benchmark_phase: bench_ctx.as_ref().and_then(|c| c.benchmark_phase.clone()),
        benchmark_operation: bench_ctx
            .as_ref()
            .and_then(|c| c.benchmark_operation.clone()),
        benchmark_operation_seq: bench_ctx.as_ref().and_then(|c| c.benchmark_operation_seq),
        benchmark_payload_size: bench_ctx.as_ref().and_then(|c| c.benchmark_payload_size),
        benchmark_workflow_id: bench_ctx.as_ref().and_then(|c| c.benchmark_workflow_id),
        workflow_pair_index: bench_ctx.as_ref().and_then(|c| c.workflow_pair_index),
        workflow_pair_count: bench_ctx.as_ref().and_then(|c| c.workflow_pair_count),
        ts_unix_ns: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos(),
        op: op.to_string(),
        span_layer: span_layer.to_string(),
        protocol_stack: "signal".to_string(),
        implementation: "libsignal".to_string(),
        measurement_class: measurement_class.to_string(),
        event_family: event_family.to_string(),
        event_subtype: event_subtype.to_string(),
        participant_id: Some(participant_id.to_string()),
        success,
        wall_ns,
        cpu_thread_ns,
        alloc_bytes,
        alloc_count,
        cpu_model: env_nonempty("SIGNAL_RESOURCE_CPU_MODEL"),
        requested_cpu_fraction: env_nonempty("SIGNAL_RESOURCE_REQUESTED_CPU_FRACTION")
            .and_then(|v| v.parse().ok()),
        applied_cpu_fraction: env_nonempty("SIGNAL_RESOURCE_APPLIED_CPU_FRACTION")
            .and_then(|v| v.parse().ok()),
        cpu_period_us: env_nonempty("SIGNAL_RESOURCE_CPU_PERIOD_US").and_then(|v| v.parse().ok()),
        cpu_quota_us: env_nonempty("SIGNAL_RESOURCE_CPU_QUOTA_US").and_then(|v| v.parse().ok()),
        cgroup_cpu_max: env_nonempty("SIGNAL_RESOURCE_CGROUP_CPU_MAX"),
        cpuset_cpus_requested: env_nonempty("SIGNAL_RESOURCE_CPUSET_CPUS_REQUESTED"),
        cpuset_cpus_effective: read_cgroup_cpuset_effective()
            .or_else(|| env_nonempty("SIGNAL_RESOURCE_CPUSET_CPUS_EFFECTIVE")),
        memory_model: env_nonempty("SIGNAL_RESOURCE_MEMORY_MODEL"),
        requested_memory_limit: env_nonempty("SIGNAL_RESOURCE_REQUESTED_MEMORY_LIMIT"),
        requested_memory_limit_bytes: env_nonempty("SIGNAL_RESOURCE_REQUESTED_MEMORY_LIMIT_BYTES")
            .and_then(|v| v.parse().ok()),
        applied_memory_limit_bytes: env_nonempty("SIGNAL_RESOURCE_APPLIED_MEMORY_LIMIT_BYTES")
            .and_then(|v| v.parse().ok()),
        app_heap_budget: env_nonempty("SIGNAL_APP_HEAP_BUDGET"),
        app_heap_budget_bytes: env_nonempty("SIGNAL_APP_HEAP_BUDGET_BYTES")
            .and_then(|v| v.parse().ok()),
        heap_current_live_bytes: (heap_snapshot.configured_heap_budget_bytes > 0)
            .then_some(heap_snapshot.current_live_heap_bytes),
        heap_peak_live_bytes: (heap_snapshot.configured_heap_budget_bytes > 0)
            .then_some(heap_snapshot.peak_live_heap_bytes),
        resource_profile_id: env_nonempty("SIGNAL_RESOURCE_PROFILE_ID"),
        resource_profile_index: env_nonempty("SIGNAL_RESOURCE_PROFILE_INDEX")
            .and_then(|v| v.parse().ok()),
        ..SignalProfileEvent::default()
    }
}

fn env_nonempty(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|value| !value.is_empty())
}

fn read_cgroup_cpuset_effective() -> Option<String> {
    for path in [
        "/sys/fs/cgroup/cpuset.cpus.effective",
        "/sys/fs/cgroup/cpuset.cpus",
    ] {
        if let Ok(value) = std::fs::read_to_string(path) {
            let trimmed = value.trim();
            if !trimmed.is_empty() {
                return Some(trimmed.to_string());
            }
        }
    }
    None
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchCommandRequest {
    pub items: Vec<BatchCommandItem>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchCommandItem {
    pub participant_id: String,
    #[serde(default)]
    pub request_id: Option<String>,
    pub command: Command,
    #[serde(default)]
    pub phase: Option<String>,
    #[serde(default)]
    pub profile: Option<bool>,
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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchCommandResponse {
    pub items: Vec<BatchCommandResult>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchCommandResult {
    pub participant_id: String,
    #[serde(default)]
    pub request_id: Option<String>,
    pub response: CommandResponse,
}

#[derive(Debug, Clone)]
struct CachedCommandResponse {
    response: CommandResponse,
    completed_at: Instant,
}

#[derive(Debug)]
pub struct CompletedCommandCache {
    entries: HashMap<String, CachedCommandResponse>,
    order: VecDeque<String>,
    max_entries: usize,
    ttl: Duration,
}

impl CompletedCommandCache {
    pub fn new(max_entries: usize, ttl: Duration) -> Self {
        Self {
            entries: HashMap::new(),
            order: VecDeque::new(),
            max_entries,
            ttl,
        }
    }

    pub fn get(&mut self, request_id: &str) -> Option<CommandResponse> {
        self.prune_expired();
        self.entries
            .get(request_id)
            .map(|cached| cached.response.clone())
    }

    pub fn insert(&mut self, request_id: String, response: CommandResponse) {
        if self.max_entries == 0 {
            return;
        }

        self.prune_expired();

        if !self.entries.contains_key(&request_id) {
            self.order.push_back(request_id.clone());
        }

        self.entries.insert(
            request_id,
            CachedCommandResponse {
                response,
                completed_at: Instant::now(),
            },
        );

        while self.entries.len() > self.max_entries {
            let Some(oldest) = self.order.pop_front() else {
                break;
            };
            self.entries.remove(&oldest);
        }
    }

    fn prune_expired(&mut self) {
        if self.ttl.is_zero() {
            self.entries.clear();
            self.order.clear();
            return;
        }

        let now = Instant::now();

        while let Some(request_id) = self.order.front() {
            let expired = self
                .entries
                .get(request_id)
                .map(|cached| now.duration_since(cached.completed_at) > self.ttl)
                .unwrap_or(true);

            if !expired {
                break;
            }

            let request_id = self.order.pop_front().expect("front checked above");
            self.entries.remove(&request_id);
        }
    }
}

static CONTROL_HTTP_CLIENT: Lazy<reqwest::Client> = Lazy::new(|| {
    reqwest::Client::builder()
        .connect_timeout(Duration::from_millis(worker_http_connect_timeout_ms()))
        .timeout(Duration::from_millis(worker_http_request_timeout_ms()))
        .pool_max_idle_per_host(control_http_pool_max_idle_per_host())
        .pool_idle_timeout(Duration::from_secs(30))
        .tcp_keepalive(Some(Duration::from_secs(60)))
        .build()
        .expect("failed to build HTTP client")
});

static OUTBOUND_HTTP_SEMAPHORE: Lazy<tokio::sync::Semaphore> =
    Lazy::new(|| tokio::sync::Semaphore::new(worker_outbound_http_permits()));

fn control_http_pool_max_idle_per_host() -> usize {
    std::env::var("SIGNAL_WORKER_HTTP_POOL_MAX_IDLE_PER_HOST")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(32)
}

fn worker_http_connect_timeout_ms() -> u64 {
    std::env::var("SIGNAL_WORKER_HTTP_CONNECT_TIMEOUT_MS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(5_000)
}

fn worker_http_request_timeout_ms() -> u64 {
    std::env::var("SIGNAL_WORKER_HTTP_REQUEST_TIMEOUT_MS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(30_000)
}

fn worker_outbound_http_permits() -> usize {
    std::env::var("SIGNAL_WORKER_OUTBOUND_HTTP_PERMITS")
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|permits| *permits > 0)
        .unwrap_or(32)
}

fn control_http_client() -> &'static reqwest::Client {
    &CONTROL_HTTP_CLIENT
}

async fn acquire_http_permit() -> tokio::sync::SemaphorePermit<'static> {
    OUTBOUND_HTTP_SEMAPHORE
        .acquire()
        .await
        .expect("HTTP semaphore was closed")
}

fn transient_or_fatal<T>(err: reqwest::Error) -> RetryDecision<T> {
    if is_transient_reqwest_error(&err) {
        RetryDecision::Transient(reqwest_error_diagnostic(&err))
    } else {
        RetryDecision::Fatal(anyhow!(err))
    }
}

fn reqwest_error_diagnostic(err: &reqwest::Error) -> String {
    let mut parts = Vec::new();
    parts.push(format!("top_level={}", err));
    parts.push(format!("is_connect={}", err.is_connect()));
    parts.push(format!("is_timeout={}", err.is_timeout()));
    parts.push(format!("is_request={}", err.is_request()));
    parts.push(format!("is_body={}", err.is_body()));
    parts.push(format!(
        "connect_stage={}",
        is_connect_stage_reqwest_error(err)
    ));

    let mut source = err.source();
    let mut idx = 0usize;
    while let Some(err) = source {
        parts.push(format!("source[{}]={}", idx, err));
        source = err.source();
        idx += 1;
    }

    parts.join("; ")
}

async fn read_response_text(response: reqwest::Response) -> String {
    response.text().await.unwrap_or_default()
}

pub async fn kr_post_bytes(
    kr_url: &str,
    path: &str,
    bytes: Vec<u8>,
    op: &str,
    participant_id: &str,
) -> Result<()> {
    let url = format!("{kr_url}{path}");
    let http = control_http_client();

    retry_transient_http_async(op, Some(participant_id), &url, || {
        let request_bytes = bytes.clone();
        async {
            let _permit = acquire_http_permit().await;
            let response = match http.post(&url).body(request_bytes).send().await {
                Ok(response) => response,
                Err(err) => return transient_or_fatal(err),
            };

            let status = response.status();

            if status.is_success() {
                return RetryDecision::Success(());
            }

            let body = read_response_text(response).await;

            if is_transient_status(status) {
                return RetryDecision::Transient(format!("HTTP {}: {}", status, body));
            }

            RetryDecision::Fatal(anyhow!("KR POST failed with status {}: {}", status, body))
        }
    })
    .await
}

pub async fn kr_post_empty(kr_url: &str, path: &str, op: &str, participant_id: &str) -> Result<()> {
    let url = format!("{kr_url}{path}");
    let http = control_http_client();

    retry_transient_http_async(op, Some(participant_id), &url, || async {
        let _permit = acquire_http_permit().await;
        let response = match http.post(&url).send().await {
            Ok(response) => response,
            Err(err) => return transient_or_fatal(err),
        };

        let status = response.status();

        if status.is_success() {
            return RetryDecision::Success(());
        }

        let body = read_response_text(response).await;

        if is_transient_status(status) {
            return RetryDecision::Transient(format!("HTTP {}: {}", status, body));
        }

        RetryDecision::Fatal(anyhow!(
            "KR POST empty failed with status {}: {}",
            status,
            body
        ))
    })
    .await
}

pub async fn kr_put_json<T: Serialize>(
    kr_url: &str,
    path: &str,
    value: &T,
    op: &str,
    participant_id: &str,
) -> Result<()> {
    let url = format!("{kr_url}{path}");
    let http = control_http_client();

    retry_transient_http_async(op, Some(participant_id), &url, || async {
        let _permit = acquire_http_permit().await;
        let response = match http.put(&url).json(value).send().await {
            Ok(response) => response,
            Err(err) => return transient_or_fatal(err),
        };

        let status = response.status();

        if status.is_success() {
            return RetryDecision::Success(());
        }

        let response_body = read_response_text(response).await;

        if is_transient_status(status) {
            return RetryDecision::Transient(format!("HTTP {}: {}", status, response_body));
        }

        RetryDecision::Fatal(anyhow!(
            "KR PUT failed with status {}: {}",
            status,
            response_body
        ))
    })
    .await
}

pub async fn kr_get_bytes(
    kr_url: &str,
    path: &str,
    op: &str,
    participant_id: &str,
) -> Result<Vec<u8>> {
    let url = format!("{kr_url}{path}");
    let http = control_http_client();

    retry_transient_http_async(op, Some(participant_id), &url, || async {
        let _permit = acquire_http_permit().await;
        let response = match http.get(&url).send().await {
            Ok(response) => response,
            Err(err) => return transient_or_fatal(err),
        };

        let status = response.status();

        if !status.is_success() {
            let body = read_response_text(response).await;

            if is_transient_status(status) {
                return RetryDecision::Transient(format!("HTTP {}: {}", status, body));
            }

            return RetryDecision::Fatal(anyhow!("KR GET failed with status {}: {}", status, body));
        }

        match response.bytes().await {
            Ok(bytes) => RetryDecision::Success(bytes.to_vec()),
            Err(err) => transient_or_fatal(err),
        }
    })
    .await
}

pub async fn kr_get_json<T: for<'de> Deserialize<'de>>(
    kr_url: &str,
    path: &str,
    op: &str,
    participant_id: &str,
) -> Result<T> {
    let url = format!("{kr_url}{path}");
    let http = control_http_client();

    retry_transient_http_async(op, Some(participant_id), &url, || async {
        let _permit = acquire_http_permit().await;
        let response = match http.get(&url).send().await {
            Ok(response) => response,
            Err(err) => return transient_or_fatal(err),
        };

        let status = response.status();

        if !status.is_success() {
            let body = read_response_text(response).await;

            if is_transient_status(status) {
                return RetryDecision::Transient(format!("HTTP {}: {}", status, body));
            }

            return RetryDecision::Fatal(anyhow!("KR GET failed with status {}: {}", status, body));
        }

        match response.json::<T>().await {
            Ok(value) => RetryDecision::Success(value),
            Err(err) => transient_or_fatal(err),
        }
    })
    .await
}

pub async fn relay_post_message(
    relay_url: &str,
    conversation_id: &str,
    sender: &str,
    recipients: &[String],
    bytes: Vec<u8>,
) -> Result<()> {
    let url = format!(
        "{}/conversation/{}/message/{}",
        relay_url.trim_end_matches('/'),
        conversation_id,
        sender
    );

    let recipients_header = recipients.join(",");
    let http = control_http_client();

    retry_transient_http_async("relay.publish_message", Some(sender), &url, || {
        let recipients_header = recipients_header.clone();
        let request_bytes = bytes.clone();
        async {
            let _permit = acquire_http_permit().await;
            let response = match http
                .post(&url)
                .header("x-recipients", recipients_header)
                .body(request_bytes)
                .send()
                .await
            {
                Ok(response) => response,
                Err(err) => return transient_or_fatal(err),
            };

            let status = response.status();

            if status.is_success() {
                return RetryDecision::Success(());
            }

            let body = read_response_text(response).await;

            if is_transient_status(status) {
                return RetryDecision::Transient(format!("HTTP {}: {}", status, body));
            }

            RetryDecision::Fatal(anyhow!(
                "Relay POST failed with status {}: {}",
                status,
                body
            ))
        }
    })
    .await
}

pub async fn relay_get_message(relay_url: &str, recipient: &str) -> Result<Vec<u8>> {
    let url = format!("{}/message/{}", relay_url.trim_end_matches('/'), recipient);
    let http = control_http_client();

    retry_transient_http_async("relay.fetch_message", Some(recipient), &url, || async {
        let _permit = acquire_http_permit().await;
        let response = match http.get(&url).send().await {
            Ok(response) => response,
            Err(err) => return transient_or_fatal(err),
        };

        let status = response.status();

        if !status.is_success() {
            let body = read_response_text(response).await;

            if is_transient_status(status) {
                return RetryDecision::Transient(format!("HTTP {}: {}", status, body));
            }

            return RetryDecision::Fatal(anyhow!(
                "Relay GET failed with status {}: {}",
                status,
                body
            ));
        }

        match response.bytes().await {
            Ok(bytes) => RetryDecision::Success(bytes.to_vec()),
            Err(err) => transient_or_fatal(err),
        }
    })
    .await
}

#[derive(Debug, Clone, Deserialize)]
struct PendingMessageResponse {
    id: String,
    conversation_id: String,
    sender: String,
    message_hex: String,
}

async fn relay_get_pending_message(
    relay_url: &str,
    recipient: &str,
    sender: Option<&str>,
) -> Result<PendingMessageResponse> {
    let url = match sender {
        Some(sender) => format!(
            "{}/message/{}/pending/{}",
            relay_url.trim_end_matches('/'),
            recipient,
            sender
        ),
        None => format!(
            "{}/message/{}/pending",
            relay_url.trim_end_matches('/'),
            recipient
        ),
    };
    let http = control_http_client();

    retry_transient_http_async(
        "relay.fetch_pending_message",
        Some(recipient),
        &url,
        || async {
            let _permit = acquire_http_permit().await;
            let response = match http.get(&url).send().await {
                Ok(response) => response,
                Err(err) => return transient_or_fatal(err),
            };

            let status = response.status();

            if !status.is_success() {
                let body = read_response_text(response).await;

                if is_transient_status(status) {
                    return RetryDecision::Transient(format!("HTTP {}: {}", status, body));
                }

                return RetryDecision::Fatal(anyhow!(
                    "Relay pending message GET failed with status {}: {}",
                    status,
                    body
                ));
            }

            match response.json::<PendingMessageResponse>().await {
                Ok(message) => RetryDecision::Success(message),
                Err(err) => transient_or_fatal(err),
            }
        },
    )
    .await
}

async fn relay_ack_message(relay_url: &str, recipient: &str, message_id: &str) -> Result<()> {
    let url = format!(
        "{}/message/{}/ack/{}",
        relay_url.trim_end_matches('/'),
        recipient,
        message_id
    );
    let http = control_http_client();

    retry_transient_http_async("relay.ack_message", Some(recipient), &url, || async {
        let _permit = acquire_http_permit().await;
        let response = match http.post(&url).send().await {
            Ok(response) => response,
            Err(err) => return transient_or_fatal(err),
        };

        let status = response.status();

        if status.is_success() {
            return RetryDecision::Success(());
        }

        let body = read_response_text(response).await;

        if is_transient_status(status) {
            return RetryDecision::Transient(format!("HTTP {}: {}", status, body));
        }

        RetryDecision::Fatal(anyhow!(
            "Relay ack message failed with status {}: {}",
            status,
            body
        ))
    })
    .await
}

fn looks_like_duplicate_receive(error: &str) -> bool {
    let lower = error.to_ascii_lowercase();
    lower.contains("replay")
        || lower.contains("duplicate")
        || lower.contains("already")
        || lower.contains("generation")
        || lower.contains("out of order")
        || lower.contains("stale")
}

fn ciphertext_from_bytes(bytes: &[u8]) -> Result<libsignal_protocol::CiphertextMessage> {
    if let Ok(msg) = PreKeySignalMessage::try_from(bytes) {
        return Ok(libsignal_protocol::CiphertextMessage::PreKeySignalMessage(
            msg,
        ));
    }
    if let Ok(msg) = SignalMessage::try_from(bytes) {
        return Ok(libsignal_protocol::CiphertextMessage::SignalMessage(msg));
    }
    Err(anyhow!("unrecognized ciphertext message type"))
}

async fn receive_message_delivery(
    participant: &mut SignalParticipant,
    relay_url: &str,
    sender: Option<&str>,
    profile: bool,
    conversation_size: usize,
    expected_plaintext_bytes: Option<usize>,
    phase: Option<&str>,
    profile_path: Option<&PathBuf>,
) -> Result<CommandOutcome> {
    let total_start = Instant::now();
    let io_start = Instant::now();
    let delivery = relay_get_pending_message(relay_url, &participant.name, sender).await?;
    if let Some(expected_sender) = sender {
        if delivery.sender != expected_sender {
            return Err(anyhow!(
                "Relay returned sender {} while {} was requested",
                delivery.sender,
                expected_sender
            ));
        }
    }
    let io_wall = io_start.elapsed().as_nanos();
    write_subspan_event(
        profile_path,
        &make_subspan_event(
            "signal_application_message_receive.relay_fetch_pending_message_io",
            "message_recovery",
            "receive_relay_fetch",
            "repository_or_relay_io",
            "io",
            &participant.name,
            io_wall,
            None,
            None,
            None,
            true,
        ),
    );

    let message_bytes = hex::decode(&delivery.message_hex).with_context(|| {
        format!(
            "decode pending message id={} conversation={} sender={}",
            delivery.id, delivery.conversation_id, delivery.sender
        )
    })?;

    let sender_address = libsignal_core::ProtocolAddress::new(
        delivery.sender.clone(),
        DeviceId::new(1).expect("valid device id"),
    );
    let ciphertext = match ciphertext_from_bytes(message_bytes.as_slice()) {
        Ok(ct) => ct,
        Err(_) => {
            let io_start = Instant::now();
            relay_ack_message(relay_url, &participant.name, &delivery.id).await?;
            let io_wall = io_start.elapsed().as_nanos();
            write_subspan_event(
                profile_path,
                &make_subspan_event(
                    "signal_application_message_receive.relay_ack_message_io",
                    "message_recovery",
                    "receive_relay_ack",
                    "repository_or_relay_io",
                    "io",
                    &participant.name,
                    io_wall,
                    None,
                    None,
                    None,
                    true,
                ),
            );
            return Ok(CommandOutcome::new(
                format!(
                    "message already processed (deserialize failed): message_id={} conversation={} sender={}",
                    delivery.id, delivery.conversation_id, delivery.sender
                ),
                CommandMetrics {
                    artifact_size_bytes: Some(message_bytes.len()),
                    conversation_size: Some(conversation_size),
                    ciphertext_bytes: Some(message_bytes.len()),
                    ..Default::default()
                },
            ));
        }
    };

    let decrypt_start = Instant::now();
    let (decrypt_result, mut profile_metrics) = if profile {
        measure_profile(|| {
            participant.decrypt_message(
                &sender_address,
                &ciphertext,
                phase,
                Some(conversation_size),
            )
        })
    } else {
        let result = participant.decrypt_message(
            &sender_address,
            &ciphertext,
            phase,
            Some(conversation_size),
        );
        (result, CommandMetrics::default())
    };
    let decrypt_wall = decrypt_start.elapsed().as_nanos();

    match decrypt_result {
        Ok(plaintext) => {
            if let Some(expected) = expected_plaintext_bytes {
                if plaintext.len() != expected {
                    return Err(anyhow!(
                        "Decrypted {} plaintext bytes from {}, expected {}",
                        plaintext.len(),
                        delivery.sender,
                        expected
                    ));
                }
            }
            write_subspan_event(
                profile_path,
                &make_subspan_event(
                    "signal_application_message_receive.message_decrypt",
                    "message_recovery",
                    "signal_application_message_receive.message_decrypt",
                    "protocol_core",
                    "protocol_core",
                    &participant.name,
                    decrypt_wall,
                    profile_metrics.cpu_thread_ns,
                    profile_metrics.alloc_bytes,
                    profile_metrics.alloc_count,
                    true,
                ),
            );
            let io_start = Instant::now();
            relay_ack_message(relay_url, &participant.name, &delivery.id).await?;
            let io_wall = io_start.elapsed().as_nanos();
            write_subspan_event(
                profile_path,
                &make_subspan_event(
                    "signal_application_message_receive.relay_ack_message_io",
                    "message_recovery",
                    "receive_relay_ack",
                    "repository_or_relay_io",
                    "io",
                    &participant.name,
                    io_wall,
                    None,
                    None,
                    None,
                    true,
                ),
            );
            let text = String::from_utf8_lossy(&plaintext).to_string();
            let plaintext_len = plaintext.len();
            profile_metrics.artifact_size_bytes = Some(message_bytes.len());
            profile_metrics.conversation_size = Some(conversation_size);
            profile_metrics.session_count = Some(1);
            profile_metrics.ciphertext_bytes = Some(message_bytes.len());
            profile_metrics.plaintext_bytes = Some(plaintext_len);
            write_subspan_event(
                profile_path,
                &make_subspan_event(
                    "signal_application_message_receive.total",
                    "message_recovery",
                    "signal_application_message_receive.total",
                    "benchmark_wrapper",
                    "protocol_operation",
                    &participant.name,
                    total_start.elapsed().as_nanos(),
                    profile_metrics.cpu_thread_ns,
                    profile_metrics.alloc_bytes,
                    profile_metrics.alloc_count,
                    true,
                ),
            );
            Ok(CommandOutcome::new(
                format!(
                    "pairwise message received: {}; message_id={} conversation={} sender={}",
                    text, delivery.id, delivery.conversation_id, delivery.sender
                ),
                profile_metrics,
            ))
        }
        Err(err) => {
            let text = format!("{:#}", err);
            if looks_like_duplicate_receive(&text) {
                let io_start = Instant::now();
                relay_ack_message(relay_url, &participant.name, &delivery.id).await?;
                let io_wall = io_start.elapsed().as_nanos();
                write_subspan_event(
                    profile_path,
                    &make_subspan_event(
                        "signal_application_message_receive.relay_ack_message_io",
                        "message_recovery",
                        "receive_relay_ack",
                        "repository_or_relay_io",
                        "io",
                        &participant.name,
                        io_wall,
                        None,
                        None,
                        None,
                        true,
                    ),
                );
                profile_metrics.artifact_size_bytes = Some(message_bytes.len());
                profile_metrics.conversation_size = Some(conversation_size);
                profile_metrics.ciphertext_bytes = Some(message_bytes.len());
                return Ok(CommandOutcome::new(
                    format!(
                        "pairwise message already processed: message_id={} conversation={} sender={}",
                        delivery.id, delivery.conversation_id, delivery.sender
                    ),
                    profile_metrics,
                ));
            }

            Err(anyhow!(text))
        }
    }
}

async fn process_pending(
    participant: &mut SignalParticipant,
    relay_url: &str,
    max_messages: Option<usize>,
    profile_path: Option<&PathBuf>,
) -> Result<CommandOutcome> {
    let max_messages = max_messages.unwrap_or(usize::MAX);
    let mut remaining = max_messages;

    let mut messages_processed = 0usize;
    let mut metrics = CommandMetrics::default();
    let mut errors = Vec::new();

    while remaining > 0 {
        match receive_message_delivery(
            participant,
            relay_url,
            None,
            true,
            2,
            None,
            Some("process_pending"),
            profile_path,
        )
        .await
        {
            Ok(outcome) => {
                messages_processed += 1;
                metrics.merge_message(&outcome.metrics);
                remaining = remaining.saturating_sub(1);
            }
            Err(err) => {
                let text = format!("{:#}", err);
                if text.contains("404 Not Found") {
                    break;
                }
                errors.push(format!("message error={}", text));
                break;
            }
        }
    }

    if errors.is_empty() {
        Ok(CommandOutcome::new(
            format!(
                "process_pending processed; messages_processed={} errors=[]",
                messages_processed,
            ),
            metrics,
        ))
    } else {
        Err(anyhow!(
            "process_pending errors; messages_processed={} errors={:?}",
            messages_processed,
            errors
        ))
    }
}

fn prekey_bundle_batch(bundles: &[PreKeyBundle]) -> Result<PrekeyBundleBatchStorable> {
    let first = bundles
        .first()
        .ok_or_else(|| anyhow!("no prekey bundles generated"))?;
    let mut one_time_prekeys = Vec::new();
    let mut signed_prekey_fallback = false;
    let mut last_resort_pq_prekey_id = None;
    let mut last_resort_pq_prekey_public = None;
    let mut last_resort_pq_prekey_signature = None;

    for bundle in bundles {
        let pq_prekey_id: u32 = bundle.kyber_pre_key_id()?.into();
        let pq_prekey_public = bundle.kyber_pre_key_public()?.serialize().to_vec();
        let pq_prekey_signature = bundle.kyber_pre_key_signature()?.to_vec();
        match (bundle.pre_key_id()?, bundle.pre_key_public()?) {
            (Some(prekey_id), Some(prekey_public)) => {
                one_time_prekeys.push(OneTimePrekeyStorable {
                    prekey_id: prekey_id.into(),
                    prekey_public: prekey_public.serialize().to_vec(),
                    pq_prekey_id,
                    pq_prekey_public,
                    pq_prekey_signature,
                });
            }
            (None, None) => {
                signed_prekey_fallback = true;
                last_resort_pq_prekey_id = Some(pq_prekey_id);
                last_resort_pq_prekey_public = Some(pq_prekey_public);
                last_resort_pq_prekey_signature = Some(pq_prekey_signature);
            }
            (Some(id), None) => {
                return Err(anyhow!("prekey_id {} was present without public key", id));
            }
            (None, Some(_)) => {
                return Err(anyhow!("prekey public key was present without id"));
            }
        }
    }

    Ok(PrekeyBundleBatchStorable {
        registration_id: first.registration_id()?,
        device_id: first.device_id()?.into(),
        signed_prekey_id: first.signed_pre_key_id()?.into(),
        signed_prekey_public: first.signed_pre_key_public()?.serialize().to_vec(),
        signed_prekey_signature: first.signed_pre_key_signature()?.to_vec(),
        identity_key_public: first.identity_key()?.public_key().serialize().to_vec(),
        last_resort_pq_prekey_id: last_resort_pq_prekey_id
            .ok_or_else(|| anyhow!("no last-resort PQ prekey bundle generated"))?,
        last_resort_pq_prekey_public: last_resort_pq_prekey_public
            .ok_or_else(|| anyhow!("no last-resort PQ prekey public key generated"))?,
        last_resort_pq_prekey_signature: last_resort_pq_prekey_signature
            .ok_or_else(|| anyhow!("no last-resort PQ prekey signature generated"))?,
        one_time_prekeys,
        signed_prekey_fallback,
    })
}

async fn published_prekey_stock(kr_url: &str, participant_id: &str) -> Result<PrekeyStock> {
    kr_get_json(
        kr_url,
        &format!("/prekey-bundle/{participant_id}/stock"),
        "prekey_stock",
        participant_id,
    )
    .await
}

pub async fn handle_command(
    participant: &mut SignalParticipant,
    kr_url: &str,
    relay_url: &str,
    command: Command,
    phase: Option<&str>,
    profile_path: Option<&std::path::PathBuf>,
) -> Result<CommandOutcome> {
    match command {
        Command::RegisterParticipant => Ok(CommandOutcome::new(
            format!("participant {} registered", participant.name),
            CommandMetrics {
                participant_count: Some(1),
                ..Default::default()
            },
        )),

        Command::GeneratePrekeyBundle => {
            let (bundles, mut profile_metrics) =
                measure_profile(|| participant.generate_prekey_bundles());
            let bundles = bundles?;
            let batch = prekey_bundle_batch(&bundles)?;
            let artifact_size_bytes = serde_json::to_vec(&batch)?.len();
            let bundle_count =
                batch.one_time_prekeys.len() + usize::from(batch.signed_prekey_fallback);
            let one_time_count = batch.one_time_prekeys.len();

            let io_start = Instant::now();
            let stock_before = published_prekey_stock(kr_url, &participant.name).await?;
            let io_wall = io_start.elapsed().as_nanos();
            write_subspan_event(
                profile_path,
                &make_subspan_event(
                    "signal_prekey_bundle_create.stock_fetch_repository_io",
                    "prekey_publication",
                    "prekey_bundle_create_stock_fetch",
                    "repository_or_relay_io",
                    "io",
                    &participant.name,
                    io_wall,
                    None,
                    None,
                    None,
                    true,
                ),
            );

            let io_start = Instant::now();
            let path = format!("/prekey-bundles/{}", participant.name);
            kr_put_json(
                kr_url,
                &path,
                &batch,
                "store_prekey_bundles",
                &participant.name,
            )
            .await?;
            let io_wall = io_start.elapsed().as_nanos();
            write_subspan_event(
                profile_path,
                &make_subspan_event(
                    "signal_prekey_bundle_create.bundle_publish_repository_io",
                    "prekey_publication",
                    "prekey_bundle_create_publish",
                    "repository_or_relay_io",
                    "io",
                    &participant.name,
                    io_wall,
                    None,
                    None,
                    None,
                    true,
                ),
            );

            let io_start = Instant::now();
            let stock_after = published_prekey_stock(kr_url, &participant.name).await?;
            let io_wall = io_start.elapsed().as_nanos();
            write_subspan_event(
                profile_path,
                &make_subspan_event(
                    "signal_prekey_bundle_create.stock_after_fetch_repository_io",
                    "prekey_publication",
                    "prekey_bundle_create_stock_after_fetch",
                    "repository_or_relay_io",
                    "io",
                    &participant.name,
                    io_wall,
                    None,
                    None,
                    None,
                    true,
                ),
            );

            Ok(CommandOutcome::new(
                format!(
                    "prekey bundle generated and published for {}; bundles={}",
                    participant.name, bundle_count
                ),
                {
                    profile_metrics.artifact_size_bytes = Some(artifact_size_bytes);
                    profile_metrics.prekey_bundle_count = Some(bundle_count);
                    profile_metrics.prekey_stock_before = Some(stock_before.one_time_available);
                    profile_metrics.prekey_stock_after = Some(stock_after.one_time_available);
                    profile_metrics.prekey_refill_count = Some(one_time_count);
                    profile_metrics.prekey_refill_trigger =
                        Some("registration_initial_stock".to_string());
                    profile_metrics.participant_count = Some(1);
                    profile_metrics
                },
            ))
        }

        Command::PublishPrekeyBundle => {
            let (result, mut profile_metrics) = measure_profile(|| participant.store_own_prekeys());
            result?;
            profile_metrics.prekey_bundle_count = Some(participant.remaining_prekeys() + 1);
            profile_metrics.participant_count = Some(1);
            Ok(CommandOutcome::new(
                format!("prekeys stored locally for {}", participant.name),
                profile_metrics,
            ))
        }

        Command::UpdateOneTimePrekeys => {
            let stock_before = published_prekey_stock(kr_url, &participant.name).await?;
            let threshold = participant.one_time_prekey_low_watermark();
            if stock_before.one_time_available > threshold {
                return Ok(CommandOutcome::new(
                    format!(
                        "one-time prekey stock for {} is above low watermark: available={} threshold={}",
                        participant.name, stock_before.one_time_available, threshold
                    ),
                    CommandMetrics {
                        participant_count: Some(1),
                        prekey_stock_before: Some(stock_before.one_time_available),
                        prekey_stock_after: Some(stock_before.one_time_available),
                        prekey_refill_count: Some(0),
                        prekey_refill_trigger: Some("stock_above_low_watermark".to_string()),
                        ..Default::default()
                    },
                ));
            }

            let (bundles, mut profile_metrics) =
                measure_profile(|| participant.generate_replenishment_prekey_bundles());
            let bundles = bundles?;
            let batch = prekey_bundle_batch(&bundles)?;
            let artifact_size_bytes = serde_json::to_vec(&batch)?.len();
            let refill_count = batch.one_time_prekeys.len();
            let bundle_count = refill_count + usize::from(batch.signed_prekey_fallback);
            kr_put_json(
                kr_url,
                &format!("/prekey-bundles/{}", participant.name),
                &batch,
                "update_one_time_prekeys",
                &participant.name,
            )
            .await?;
            let stock_after = published_prekey_stock(kr_url, &participant.name).await?;
            profile_metrics.artifact_size_bytes = Some(artifact_size_bytes);
            profile_metrics.prekey_bundle_count = Some(bundle_count);
            profile_metrics.prekey_stock_before = Some(stock_before.one_time_available);
            profile_metrics.prekey_stock_after = Some(stock_after.one_time_available);
            profile_metrics.prekey_refill_count = Some(refill_count);
            profile_metrics.prekey_refill_trigger = Some("low_watermark".to_string());
            profile_metrics.participant_count = Some(1);
            Ok(CommandOutcome::new(
                format!(
                    "one-time prekey stock updated for {}; before={} after={} refill={}",
                    participant.name,
                    stock_before.one_time_available,
                    stock_after.one_time_available,
                    refill_count,
                ),
                profile_metrics,
            ))
        }

        Command::EstablishSessions {
            participants,
            conversation_size,
        } => {
            let conversation_size =
                conversation_size.unwrap_or(participants.len().saturating_add(1));
            let mut established = 0usize;
            let mut existing = 0usize;
            let mut fetched = 0usize;
            let mut artifact_size_bytes = 0usize;
            let mut profile_metrics = CommandMetrics::default();

            for peer in &participants {
                let peer_address = libsignal_core::ProtocolAddress::new(
                    peer.clone(),
                    DeviceId::new(1).expect("valid device id"),
                );

                let (has_session, has_session_metrics) =
                    measure_profile(|| participant.has_session_with(&peer_address));
                profile_metrics.merge_profile(&has_session_metrics);
                if has_session {
                    existing += 1;
                    continue;
                }

                let path = format!("/prekey-bundle/{peer}");
                let io_start = Instant::now();
                let bundle_bytes =
                    kr_get_bytes(kr_url, &path, "fetch_prekey_bundle", &participant.name).await?;
                let io_wall = io_start.elapsed().as_nanos();
                write_subspan_event(
                    profile_path,
                    &make_subspan_event(
                        "signal_session_establish.prekey_bundle_fetch_repository_io",
                        "session_establishment",
                        "session_establish_prekey_fetch",
                        "repository_or_relay_io",
                        "io",
                        &participant.name,
                        io_wall,
                        None,
                        None,
                        None,
                        true,
                    ),
                );
                artifact_size_bytes = artifact_size_bytes.saturating_add(bundle_bytes.len());
                let bundle_storable: PrekeyBundleStorable =
                    serde_json::from_slice(&bundle_bytes)
                        .with_context(|| format!("decode fetched prekey bundle for {peer}"))?;
                fetched += 1;

                let prekey_public = match (
                    bundle_storable.prekey_id,
                    bundle_storable.prekey_public.as_ref(),
                ) {
                    (Some(_), Some(bytes)) => Some(
                        libsignal_core::curve::PublicKey::deserialize(bytes)
                            .map_err(|e| anyhow!("invalid prekey public: {}", e))?,
                    ),
                    (Some(id), None) => {
                        return Err(anyhow!(
                            "prekey_id {} was present without prekey_public",
                            id
                        ));
                    }
                    (None, Some(_)) => {
                        return Err(anyhow!("prekey_public was present without prekey_id"));
                    }
                    (None, None) => None,
                };

                let identity_key = libsignal_protocol::IdentityKey::new(
                    libsignal_core::curve::PublicKey::deserialize(
                        &bundle_storable.identity_key_public,
                    )
                    .map_err(|e| anyhow!("invalid identity key: {}", e))?,
                );

                let kyber_prekey_public =
                    kem::PublicKey::deserialize(&bundle_storable.kyber_prekey_public)
                        .map_err(|e| anyhow!("invalid kyber prekey: {}", e))?;

                let bundle = libsignal_protocol::PreKeyBundle::new(
                    bundle_storable.registration_id,
                    DeviceId::new(
                        bundle_storable
                            .device_id
                            .try_into()
                            .map_err(|_| anyhow!("invalid device_id"))?,
                    )
                    .map_err(|_| anyhow!("invalid device_id value"))?,
                    bundle_storable.prekey_id.map(|id| {
                        (
                            PreKeyId::from(id),
                            prekey_public.expect("validated prekey_public"),
                        )
                    }),
                    SignedPreKeyId::from(bundle_storable.signed_prekey_id),
                    libsignal_core::curve::PublicKey::deserialize(
                        &bundle_storable.signed_prekey_public,
                    )
                    .map_err(|e| anyhow!("invalid signed prekey: {}", e))?,
                    bundle_storable.signed_prekey_signature,
                    KyberPreKeyId::from(bundle_storable.kyber_prekey_id),
                    kyber_prekey_public,
                    bundle_storable.kyber_prekey_signature,
                    identity_key,
                )?;

                let total_start = Instant::now();
                let core_start = Instant::now();
                let (result, establish_metrics) = measure_profile(|| {
                    participant.establish_session_from_bundle(
                        &peer_address,
                        &bundle,
                        phase,
                        Some(conversation_size),
                    )
                });
                let core_wall = core_start.elapsed().as_nanos();
                result?;
                write_subspan_event(
                    profile_path,
                    &make_subspan_event(
                        "signal_session_establish.process_prekey_bundle",
                        "session_establishment",
                        "signal_session_establish.process_prekey_bundle",
                        "protocol_core",
                        "protocol_core",
                        &participant.name,
                        core_wall,
                        establish_metrics.cpu_thread_ns,
                        establish_metrics.alloc_bytes,
                        establish_metrics.alloc_count,
                        true,
                    ),
                );
                profile_metrics.merge_profile(&establish_metrics);
                let consume_path = format!("/prekey-bundle/{}/consume", peer);
                kr_post_empty(kr_url, &consume_path, "consume_prekey", &participant.name)
                    .await
                    .ok();
                write_subspan_event(
                    profile_path,
                    &make_subspan_event(
                        "signal_session_establish.total",
                        "session_establishment",
                        "signal_session_establish.total",
                        "benchmark_wrapper",
                        "protocol_operation",
                        &participant.name,
                        total_start.elapsed().as_nanos(),
                        establish_metrics.cpu_thread_ns,
                        establish_metrics.alloc_bytes,
                        establish_metrics.alloc_count,
                        true,
                    ),
                );
                established += 1;
            }

            Ok(CommandOutcome::new(
                format!(
                    "session establishment: new={} existing={} total_target={}",
                    established,
                    existing,
                    participants.len()
                ),
                {
                    profile_metrics.participant_count = Some(participants.len().saturating_add(1));
                    profile_metrics.conversation_size = Some(conversation_size);
                    profile_metrics.prekey_bundle_count = Some(fetched);
                    profile_metrics.session_count = Some(established.saturating_add(existing));
                    profile_metrics.new_session_established = Some(established > 0);
                    if artifact_size_bytes > 0 {
                        profile_metrics.artifact_size_bytes = Some(artifact_size_bytes);
                    }
                    profile_metrics
                },
            ))
        }

        Command::EncryptMessage {
            recipient,
            message,
            conversation_size,
        } => {
            let conversation_size = conversation_size.unwrap_or(2);
            let plaintext = message.into_bytes();
            let plaintext_bytes = plaintext.len();
            let total_start = Instant::now();
            let recipient_address = libsignal_core::ProtocolAddress::new(
                recipient.clone(),
                DeviceId::new(1).expect("valid device id"),
            );

            let core_start = Instant::now();
            let (ciphertext, mut profile_metrics) = measure_profile(|| {
                participant.encrypt_message(
                    &recipient_address,
                    &plaintext,
                    phase,
                    Some(conversation_size),
                )
            });
            let core_wall = core_start.elapsed().as_nanos();
            let ciphertext = ciphertext?;
            let ciphertext_bytes = ciphertext.serialize().to_vec();
            let ciphertext_len = ciphertext_bytes.len();
            write_subspan_event(
                profile_path,
                &make_subspan_event(
                    "signal_application_message_create.ratchet_encrypt_payload",
                    "message_protection",
                    "signal_application_message_create.ratchet_encrypt_payload",
                    "protocol_core",
                    "protocol_core",
                    &participant.name,
                    core_wall,
                    profile_metrics.cpu_thread_ns,
                    profile_metrics.alloc_bytes,
                    profile_metrics.alloc_count,
                    true,
                ),
            );

            let conversation_id = format!("conversation-{}", participant.name);
            let io_start = Instant::now();
            relay_post_message(
                relay_url,
                &conversation_id,
                &participant.name,
                &[recipient.clone()],
                ciphertext_bytes,
            )
            .await?;
            let io_wall = io_start.elapsed().as_nanos();
            write_subspan_event(
                profile_path,
                &make_subspan_event(
                    "signal_application_message_create.relay_publish_message_io",
                    "message_protection",
                    "encrypt_relay_publish",
                    "repository_or_relay_io",
                    "io",
                    &participant.name,
                    io_wall,
                    None,
                    None,
                    None,
                    true,
                ),
            );

            Ok(CommandOutcome::new(
                format!("pairwise message encrypted and sent to {}", recipient),
                {
                    profile_metrics.artifact_size_bytes = Some(ciphertext_len);
                    profile_metrics.conversation_size = Some(conversation_size);
                    profile_metrics.session_count = Some(1);
                    profile_metrics.ciphertext_bytes = Some(ciphertext_len);
                    profile_metrics.plaintext_bytes = Some(plaintext_bytes);
                    write_subspan_event(
                        profile_path,
                        &make_subspan_event(
                            "signal_application_message_create.total",
                            "message_protection",
                            "signal_application_message_create.total",
                            "benchmark_wrapper",
                            "protocol_operation",
                            &participant.name,
                            total_start.elapsed().as_nanos(),
                            profile_metrics.cpu_thread_ns,
                            profile_metrics.alloc_bytes,
                            profile_metrics.alloc_count,
                            true,
                        ),
                    );
                    profile_metrics
                },
            ))
        }

        Command::DecryptMessage {
            sender,
            profile,
            conversation_size,
            expected_plaintext_bytes,
        } => {
            receive_message_delivery(
                participant,
                relay_url,
                Some(&sender),
                profile,
                conversation_size.unwrap_or(2),
                expected_plaintext_bytes,
                phase,
                profile_path,
            )
            .await
        }

        Command::ProcessPending { max_messages } => {
            process_pending(participant, relay_url, max_messages, profile_path).await
        }

        Command::ShowParticipantState => Ok(CommandOutcome::message(format!(
            "participant={} remaining_prekeys={} address={:?}",
            participant.name,
            participant.remaining_prekeys(),
            participant.address,
        ))),

        Command::RemoveParticipants { participants } => Ok(CommandOutcome::new(
            format!(
                "participants {:?} deactivated; local sessions retained",
                participants
            ),
            CommandMetrics {
                participant_count: Some(participants.len()),
                ..Default::default()
            },
        )),
    }
}
