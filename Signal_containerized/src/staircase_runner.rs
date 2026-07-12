use std::{
    collections::{HashMap, HashSet, VecDeque},
    error::Error as StdError,
    fmt,
    fs::{self, File, OpenOptions},
    io::{self, BufRead, BufReader, Write},
    path::{Path, PathBuf},
    str::FromStr,
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::{anyhow, Context, Result};
use futures_util::stream::{self, StreamExt};
use rand::seq::SliceRandom;
use rand::Rng;
use serde::{Deserialize, Serialize};

use crate::http_retry::{
    is_connect_stage_reqwest_error, is_transient_reqwest_error, is_transient_status,
    retry_transient_http_async, RetryDecision,
};
use crate::signal_metrics::SignalProfileEvent;
use crate::worker_api::{
    BatchCommandItem, BatchCommandRequest, BatchCommandResponse, Command, CommandRequestEnvelope,
    CommandResponse,
};

const WORKER_COMMAND_MAX_ATTEMPTS: usize = 10;
const WORKER_COMMAND_INITIAL_DELAY: Duration = Duration::from_millis(100);
const WORKER_COMMAND_MAX_DELAY: Duration = Duration::from_secs(3);
const DEFAULT_HTTP_POOL_MAX_IDLE_PER_HOST: usize = 4;
const DEFAULT_MAX_FANOUT_PARALLELISM: usize = 32;
const DEFAULT_MIN_FANOUT_PARALLELISM: usize = 1;
const ADAPTIVE_FANOUT_START: usize = 16;
const FANOUT_LATENCY_SPIKE_P95_MS: u128 = 5_000;
const FANOUT_STABLE_INCREASE_AFTER: usize = 20;
const DEFAULT_FANOUT_ERROR_RATE_THRESHOLD: f64 = 0.02;
const DEFAULT_RUNNER_HTTP_CONNECT_TIMEOUT_MS: u64 = 2_000;
const DEFAULT_RUNNER_HTTP_REQUEST_TIMEOUT_MS: u64 = 60_000;
const DEFAULT_FANOUT_RETRY_PASSES: usize = 1;
const MAX_RANDOM_BATCH_SIZE: usize = 8;

static WORKER_REQUEST_COUNTER: AtomicU64 = AtomicU64::new(1);
static WORKFLOW_COUNTER: AtomicU64 = AtomicU64::new(1);

fn next_workflow_id() -> u64 {
    WORKFLOW_COUNTER.fetch_add(1, Ordering::Relaxed)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PlateauOrder {
    Staircase,
    Ascending,
    Randomized,
}

impl FromStr for PlateauOrder {
    type Err = String;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "staircase" => Ok(Self::Staircase),
            "ascending" | "asc" => Ok(Self::Ascending),
            "randomized" | "random" => Ok(Self::Randomized),
            other => Err(format!(
                "--plateau-order expected 'staircase', 'ascending', or 'randomized', got '{other}'"
            )),
        }
    }
}

impl fmt::Display for PlateauOrder {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Staircase => formatter.write_str("staircase"),
            Self::Ascending => formatter.write_str("ascending"),
            Self::Randomized => formatter.write_str("randomized"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct StaircaseConfig {
    pub preflight_only: bool,
    pub kr_url: String,
    pub relay_url: String,
    pub workers: Vec<WorkerSpec>,
    pub min_size: usize,
    pub max_size: Option<usize>,
    pub step_size: StepSize,
    pub step_size_switch_at: Option<usize>,
    pub step_size_after_switch: Option<usize>,
    pub plateau_order: PlateauOrder,
    pub roundtrips: usize,
    pub app_rounds: usize,
    pub max_app_samples_per_payload: usize,
    pub payload_sizes: PayloadSizes,
    pub run_id: String,
    pub scenario: String,
    pub output_dir: String,
    pub worker_health_timeout_seconds: u64,
    pub worker_health_poll_ms: u64,
    pub max_fanout_parallelism: usize,
    pub min_fanout_parallelism: usize,
    pub fanout_adaptive: Option<bool>,
    pub fanout_error_rate_threshold: f64,
    pub fanout_p95_threshold_ms: u128,
    pub http_pool_max_idle_per_host: usize,
    pub profile_only_singletons: bool,
    pub worker_layout: Option<WorkerLayout>,
    pub no_aggregate: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StepSize {
    Fixed(usize),
    UniformRange { min: usize, max: usize },
}

impl StepSize {
    fn sample<R: Rng + ?Sized>(&self, rng: &mut R) -> usize {
        match self {
            Self::Fixed(step_size) => *step_size,
            Self::UniformRange { min, max } => rng.random_range(*min..=*max),
        }
    }

    fn is_valid(&self) -> bool {
        match self {
            Self::Fixed(step_size) => *step_size > 0,
            Self::UniformRange { min, .. } => *min > 0,
        }
    }
}

impl FromStr for StepSize {
    type Err = String;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        if let Some((min, max)) = parse_uniform_range(value, "--step-size")? {
            if min == 0 {
                return Err("--step-size range minimum must be at least 1".to_string());
            }
            return Ok(Self::UniformRange { min, max });
        }

        let step_size = parse_usize(value, "--step-size")?;
        if step_size == 0 {
            return Err("--step-size must be at least 1".to_string());
        }
        Ok(Self::Fixed(step_size))
    }
}

impl fmt::Display for StepSize {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Fixed(step_size) => write!(formatter, "{step_size}"),
            Self::UniformRange { min, max } => write!(formatter, "[{min},{max}]"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PayloadSizes {
    Fixed(Vec<usize>),
    UniformRange { min: usize, max: usize },
}

impl PayloadSizes {
    fn is_empty(&self) -> bool {
        matches!(self, Self::Fixed(sizes) if sizes.is_empty())
    }

    fn source_count(&self) -> usize {
        match self {
            Self::Fixed(sizes) => sizes.len(),
            Self::UniformRange { .. } => 1,
        }
    }

    fn sources(&self) -> Vec<PayloadSizeSource> {
        match self {
            Self::Fixed(sizes) => sizes
                .iter()
                .copied()
                .map(PayloadSizeSource::Fixed)
                .collect(),
            Self::UniformRange { min, max } => vec![PayloadSizeSource::UniformRange {
                min: *min,
                max: *max,
            }],
        }
    }
}

impl FromStr for PayloadSizes {
    type Err = String;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        if let Some((min, max)) = parse_uniform_range(value, "--payload-sizes")? {
            return Ok(Self::UniformRange { min, max });
        }

        let sizes = value
            .split(',')
            .map(|size| parse_usize(size, "--payload-sizes"))
            .collect::<std::result::Result<Vec<_>, _>>()?;
        if sizes.is_empty() {
            return Err("--payload-sizes requires at least one size".to_string());
        }
        Ok(Self::Fixed(sizes))
    }
}

impl fmt::Display for PayloadSizes {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Fixed(sizes) => {
                let joined = sizes
                    .iter()
                    .map(usize::to_string)
                    .collect::<Vec<_>>()
                    .join(",");
                formatter.write_str(&joined)
            }
            Self::UniformRange { min, max } => write!(formatter, "[{min},{max}]"),
        }
    }
}

#[derive(Clone, Copy)]
enum PayloadSizeSource {
    Fixed(usize),
    UniformRange { min: usize, max: usize },
}

impl PayloadSizeSource {
    fn sample<R: Rng + ?Sized>(self, rng: &mut R) -> usize {
        match self {
            Self::Fixed(size) => size,
            Self::UniformRange { min, max } => rng.random_range(min..=max),
        }
    }

    fn phase_label(self) -> String {
        match self {
            Self::Fixed(size) => format!("payload {size} B"),
            Self::UniformRange { min, max } => format!("payload range [{min},{max}] B"),
        }
    }
}

fn parse_usize(value: &str, flag: &str) -> std::result::Result<usize, String> {
    let value = value.trim();
    if value.is_empty() {
        return Err(format!("{flag} contains an empty integer"));
    }
    value
        .parse::<usize>()
        .map_err(|_| format!("{flag} expected an integer, got '{value}'"))
}

fn parse_uniform_range(
    value: &str,
    flag: &str,
) -> std::result::Result<Option<(usize, usize)>, String> {
    let value = value.trim();
    if !value.contains('[') && !value.contains(']') {
        return Ok(None);
    }
    if !(value.starts_with('[') && value.ends_with(']')) {
        return Err(format!("{flag} range must use [min,max], got '{value}'"));
    }

    let bounds = value[1..value.len() - 1].split(',').collect::<Vec<_>>();
    if bounds.len() != 2 {
        return Err(format!(
            "{flag} range must contain exactly two integers, got '{value}'"
        ));
    }
    let min = parse_usize(bounds[0], flag)?;
    let max = parse_usize(bounds[1], flag)?;
    if min > max {
        return Err(format!("{flag} range minimum {min} exceeds maximum {max}"));
    }
    Ok(Some((min, max)))
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ContainerMode {
    Singleton,
    Packed,
}

#[derive(Debug, Clone)]
pub struct WorkerSpec {
    pub id: String,
    pub url: String,
    pub command_url: String,
    pub health_url: String,
    pub physical_worker_id: String,
    pub container_mode: ContainerMode,
    pub profile_enabled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerLayoutClient {
    pub client_id: String,
    pub physical_worker_id: String,
    pub container_mode: String,
    pub profile_enabled: bool,
    pub command_url: String,
    pub health_url: String,
    #[serde(default)]
    pub execution_backend: String,
    #[serde(default)]
    pub device_kind: String,
    #[serde(default)]
    pub transport: String,
    #[serde(default)]
    pub access_backend: String,
    #[serde(default)]
    pub arch: String,
    #[serde(default)]
    pub rust_target: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerLayoutPhysicalWorker {
    pub physical_worker_id: String,
    pub container_mode: String,
    pub client_ids: Vec<String>,
    pub base_url: String,
    pub profile_enabled_client_ids: Vec<String>,
    #[serde(default)]
    pub resource_limit_cpus: Option<f64>,
    #[serde(default)]
    pub resource_limit_memory: Option<String>,
    #[serde(default)]
    pub resource_limit_memory_bytes: Option<u64>,
    #[serde(default)]
    pub resource_limit_memory_swap: Option<String>,
    #[serde(default)]
    pub resource_limit_memory_swap_bytes: Option<u64>,
    #[serde(default)]
    pub resource_limit_pids: Option<u64>,
    #[serde(default)]
    pub resource_profile: String,
    #[serde(default)]
    pub execution_backend: String,
    #[serde(default)]
    pub device_kind: String,
    #[serde(default)]
    pub transport: String,
    #[serde(default)]
    pub access_backend: String,
    #[serde(default)]
    pub arch: String,
    #[serde(default)]
    pub rust_target: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerLayout {
    pub version: u32,
    pub logical_worker_count: usize,
    pub physical_worker_count: usize,
    pub layout_mode: String,
    #[serde(default)]
    pub singleton_min_count: usize,
    #[serde(default)]
    pub singleton_fraction: f64,
    #[serde(default)]
    pub packed_clients_per_container: usize,
    #[serde(default)]
    pub singleton_selection_seed: u64,
    pub profile_policy: String,
    pub clients: Vec<WorkerLayoutClient>,
    pub physical_workers: Vec<WorkerLayoutPhysicalWorker>,
    #[serde(default)]
    pub execution_backend: String,
    #[serde(default)]
    pub device_kind: String,
    #[serde(default)]
    pub transport: String,
    #[serde(default)]
    pub access_backend: String,
    #[serde(default)]
    pub arch: String,
    #[serde(default)]
    pub rust_target: String,
}

impl WorkerSpec {
    pub fn legacy(id: String, url: String) -> Self {
        Self {
            id: id.clone(),
            url: url.clone(),
            command_url: format!("{}/command", url.trim_end_matches('/')),
            health_url: format!("{}/health", url.trim_end_matches('/')),
            physical_worker_id: id,
            container_mode: ContainerMode::Singleton,
            profile_enabled: true,
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct OomEvidence {
    #[serde(default)]
    ts_unix_ns: Option<u128>,
    #[serde(default)]
    source: String,
    #[serde(default)]
    worker_id: Option<String>,
    #[serde(default)]
    physical_worker_id: Option<String>,
    #[serde(default)]
    detail: Option<String>,
}

impl OomEvidence {
    fn matches(&self, worker: &WorkerSpec) -> bool {
        self.worker_id.as_deref() == Some(worker.id.as_str())
            || self.physical_worker_id.as_deref() == Some(worker.physical_worker_id.as_str())
    }
}

#[derive(Debug, Clone)]
struct BenchmarkCursor {
    plateau_index: usize,
    target_size: usize,
    active_size: usize,
    phase: String,
    operation: String,
    operation_seq: Option<usize>,
    payload_size: Option<usize>,
}

impl BenchmarkCursor {
    fn new(
        plateau_index: usize,
        target_size: usize,
        active_size: usize,
        phase: &str,
        operation: &str,
    ) -> Self {
        Self {
            plateau_index,
            target_size,
            active_size,
            phase: phase.to_string(),
            operation: operation.to_string(),
            operation_seq: None,
            payload_size: None,
        }
    }

    fn at_operation(mut self, operation_seq: usize, payload_size: Option<usize>) -> Self {
        self.operation_seq = Some(operation_seq);
        self.payload_size = payload_size;
        self
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RunnerEvent {
    profile_schema_version: u32,
    ts_unix_ns: u128,
    event_kind: String,
    failed_worker_id: String,
    failed_physical_worker_id: String,
    failure_class: String,
    failure_detail: String,
    failure_evidence_source: Option<String>,
    failure_evidence_detail: Option<String>,
    failure_action: String,
    reassigned_to_worker_id: Option<String>,
    benchmark_plateau_index: usize,
    benchmark_target_size: usize,
    benchmark_active_size: usize,
    benchmark_phase: String,
    benchmark_operation: String,
    benchmark_operation_seq: Option<usize>,
    benchmark_payload_size: Option<usize>,
    benchmark_workflow_id: Option<u64>,
    workflow_pair_index: Option<u32>,
    workflow_pair_count: Option<u32>,
}

struct RunnerEventLog {
    path: PathBuf,
    oom_evidence_path: PathBuf,
    run_dir: PathBuf,
}

impl RunnerEventLog {
    fn new(run_dir: &Path) -> Self {
        Self {
            path: run_dir.join("runner-events.jsonl"),
            oom_evidence_path: run_dir.join("oom_events.jsonl"),
            run_dir: run_dir.to_path_buf(),
        }
    }

    async fn find_oom_evidence(&self, worker: &WorkerSpec) -> Option<OomEvidence> {
        // The Python monitors flush cgroup and dmesg evidence asynchronously.
        for attempt in 0..6 {
            if let Some(evidence) = latest_oom_evidence_for(&self.oom_evidence_path, worker) {
                return Some(evidence);
            }
            if attempt < 5 {
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        }
        None
    }

    fn record_oom_failure(
        &self,
        cursor: &BenchmarkCursor,
        worker: &WorkerSpec,
        error: &anyhow::Error,
        evidence: &OomEvidence,
        action: &str,
        reassigned_to: Option<&WorkerSpec>,
    ) -> Result<()> {
        let event = RunnerEvent {
            profile_schema_version: 3,
            ts_unix_ns: unix_time_ns(),
            event_kind: "worker_failure".to_string(),
            failed_worker_id: worker.id.clone(),
            failed_physical_worker_id: worker.physical_worker_id.clone(),
            failure_class: "oom_kill".to_string(),
            failure_detail: format!("{:#}", error),
            failure_evidence_source: non_empty_string(&evidence.source),
            failure_evidence_detail: evidence.detail.clone(),
            failure_action: action.to_string(),
            reassigned_to_worker_id: reassigned_to.map(|candidate| candidate.id.clone()),
            benchmark_plateau_index: cursor.plateau_index,
            benchmark_target_size: cursor.target_size,
            benchmark_active_size: cursor.active_size,
            benchmark_phase: cursor.phase.clone(),
            benchmark_operation: cursor.operation.clone(),
            benchmark_operation_seq: cursor.operation_seq,
            benchmark_payload_size: cursor.payload_size,
            benchmark_workflow_id: None,
            workflow_pair_index: None,
            workflow_pair_count: None,
        };
        let mut out = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .with_context(|| format!("Failed to open runner event log {}", self.path.display()))?;
        serde_json::to_writer(&mut out, &event)?;
        writeln!(out)?;
        let profile_path = self
            .run_dir
            .join(format!("participant-{}.jsonl", worker.id));
        if let Err(profile_error) =
            append_signal_profile_event(&profile_path, &event.to_profile_event())
        {
            eprintln!(
                "[oom-attrition] WARNING: failed to append duplicate profile row for worker {}: {:#}; runner journal {} remains authoritative",
                worker.id,
                profile_error,
                self.path.display()
            );
        }
        eprintln!(
            "[oom-attrition] worker={} physical_worker={} phase={} operation={} action={}",
            worker.id, worker.physical_worker_id, cursor.phase, cursor.operation, action
        );
        Ok(())
    }
}

impl RunnerEvent {
    fn to_profile_event(&self) -> SignalProfileEvent {
        let mut event = SignalProfileEvent {
            profile_schema_version: self.profile_schema_version,
            ts_unix_ns: self.ts_unix_ns,
            op: "benchmark.worker_failure".to_string(),
            span_layer: "benchmark_runner".to_string(),
            protocol_stack: "signal".to_string(),
            implementation: "benchmark_runner".to_string(),
            measurement_class: "runner_failure".to_string(),
            event_family: "worker_lifecycle".to_string(),
            event_subtype: "oom_kill".to_string(),
            error_class: Some(self.failure_class.clone()),
            runner_event_kind: Some(self.event_kind.clone()),
            failed_worker_id: Some(self.failed_worker_id.clone()),
            failed_physical_worker_id: Some(self.failed_physical_worker_id.clone()),
            failure_detail: Some(self.failure_detail.clone()),
            failure_evidence_source: self.failure_evidence_source.clone(),
            failure_evidence_detail: self.failure_evidence_detail.clone(),
            failure_action: Some(self.failure_action.clone()),
            reassigned_to_worker_id: self.reassigned_to_worker_id.clone(),
            benchmark_plateau_index: Some(self.benchmark_plateau_index),
            benchmark_target_size: Some(self.benchmark_target_size),
            benchmark_active_size: Some(self.benchmark_active_size),
            benchmark_phase: Some(self.benchmark_phase.clone()),
            benchmark_operation: Some(self.benchmark_operation.clone()),
            benchmark_operation_seq: self.benchmark_operation_seq,
            benchmark_payload_size: self.benchmark_payload_size,
            participant_id: Some(self.failed_worker_id.clone()),
            success: false,
            wall_ns: 0,
            pid: std::process::id(),
            thread_id: "benchmark-runner".to_string(),
            ..SignalProfileEvent::default()
        };
        apply_app_heap_budget_failure_fields(&mut event);
        event
    }
}

fn append_signal_profile_event(path: &Path, event: &SignalProfileEvent) -> Result<()> {
    let mut out = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .with_context(|| format!("Failed to open {}", path.display()))?;
    serde_json::to_writer(&mut out, event)?;
    writeln!(out)?;
    Ok(())
}

fn signal_profile_contains_runner_event(path: &Path, runner_event: &RunnerEvent) -> Result<bool> {
    if !path.exists() {
        return Ok(false);
    }

    let file = File::open(path).with_context(|| format!("Failed to open {}", path.display()))?;
    for line in BufReader::new(file).lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let event: SignalProfileEvent = serde_json::from_str(&line)
            .with_context(|| format!("Invalid json in {}", path.display()))?;
        if event.op == "benchmark.worker_failure"
            && event.ts_unix_ns == runner_event.ts_unix_ns
            && event.failed_worker_id.as_deref() == Some(runner_event.failed_worker_id.as_str())
        {
            return Ok(true);
        }
    }

    Ok(false)
}

fn materialize_runner_profile_events(run_dir: &Path) -> Result<()> {
    let path = run_dir.join("runner-events.jsonl");
    if !path.exists() {
        return Ok(());
    }

    let file = File::open(&path).with_context(|| format!("Failed to open {}", path.display()))?;
    for line in BufReader::new(file).lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let event: RunnerEvent = serde_json::from_str(&line)
            .with_context(|| format!("Invalid json in {}", path.display()))?;
        let profile_path = run_dir.join(format!("participant-{}.jsonl", event.failed_worker_id));
        if !signal_profile_contains_runner_event(&profile_path, &event)? {
            append_signal_profile_event(&profile_path, &event.to_profile_event())?;
        }
    }

    Ok(())
}

fn unix_time_ns() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
}

fn non_empty_string(value: &str) -> Option<String> {
    (!value.is_empty()).then(|| value.to_string())
}

fn classify_worker_error(error: &anyhow::Error) -> &'static str {
    if let Some(req_err) = error.downcast_ref::<reqwest::Error>() {
        if req_err.is_connect() {
            if let Some(src) = req_err.source() {
                let src_str = format!("{}", src);
                if src_str.contains("Connection refused") || src_str.contains("connection refused")
                {
                    return "container_exit";
                }
            }
            return "worker_unreachable";
        }
        if req_err.is_timeout() {
            return "cpu_starvation_timeout";
        }
        if req_err.is_body() || req_err.is_decode() {
            return "protocol_failure";
        }
    }
    let error_str = format!("{:#}", error).to_lowercase();
    if error_str.contains("app_heap_budget_exceeded") {
        return "app_heap_budget_exceeded";
    }
    if error_str.contains("app_heap_budget_allocator_abort") {
        return "app_heap_budget_allocator_abort";
    }
    if error_str.contains("embedded_budget_timeout") {
        return "embedded_budget_timeout";
    }
    if error_str.contains("connection refused") {
        return "container_exit";
    }
    if error_str.contains("timeout")
        || error_str.contains("deadline")
        || error_str.contains("timed out")
    {
        return "cpu_starvation_timeout";
    }
    if error_str.contains("connect") {
        return "worker_unreachable";
    }
    "infrastructure_failure"
}

fn app_heap_field(detail: &str, key: &str) -> Option<String> {
    for part in detail.split_whitespace() {
        if let Some((k, v)) = part.split_once('=') {
            if k == key {
                return Some(v.to_string());
            }
        }
    }
    None
}

fn apply_app_heap_budget_failure_fields(event: &mut SignalProfileEvent) {
    let Some(detail) = event.failure_detail.clone() else {
        return;
    };
    if !detail.contains("APP_HEAP_BUDGET_EXCEEDED") {
        return;
    }
    let detail = detail.as_str();

    event.memory_model = app_heap_field(detail, "memory_model");
    event.app_heap_budget = app_heap_field(detail, "app_heap_budget");
    event.app_heap_budget_bytes =
        app_heap_field(detail, "app_heap_budget_bytes").and_then(|v| v.parse::<u64>().ok());
    event.heap_current_live_bytes =
        app_heap_field(detail, "current_live_heap_bytes").and_then(|v| v.parse::<u64>().ok());
    event.heap_peak_live_bytes =
        app_heap_field(detail, "peak_live_heap_bytes").and_then(|v| v.parse::<u64>().ok());
    event.heap_operation_peak_live_bytes = app_heap_field(detail, "operation_peak_live_heap_bytes")
        .and_then(|v| v.parse::<u64>().ok());
    event.heap_total_allocated_bytes =
        app_heap_field(detail, "total_allocated_bytes").and_then(|v| v.parse::<u64>().ok());
    event.heap_allocation_count =
        app_heap_field(detail, "allocation_count").and_then(|v| v.parse::<u64>().ok());
    event.heap_deallocation_count =
        app_heap_field(detail, "deallocation_count").and_then(|v| v.parse::<u64>().ok());
    event.heap_failed_allocation_size_bytes =
        app_heap_field(detail, "failed_allocation_size_bytes").and_then(|v| v.parse::<u64>().ok());
    event.heap_failure_context = app_heap_field(detail, "heap_failure_context");

    if event.failure_operation.is_none() {
        event.failure_operation = app_heap_field(detail, "operation_family");
    }
    if let Some(member_count) =
        app_heap_field(detail, "member_count").and_then(|v| v.parse::<usize>().ok())
    {
        event.peer_count = Some(member_count);
    }
}

fn latest_oom_evidence_for(path: &Path, worker: &WorkerSpec) -> Option<OomEvidence> {
    let file = File::open(path).ok()?;
    let mut latest = None;
    for line in BufReader::new(file).lines().map_while(Result::ok) {
        if let Ok(evidence) = serde_json::from_str::<OomEvidence>(&line) {
            if evidence.matches(worker) {
                latest = Some(evidence);
            }
        }
    }
    latest
}

pub fn parse_worker_layout(path: &Path) -> Result<WorkerLayout> {
    let content = fs::read_to_string(path)
        .with_context(|| format!("Failed to read worker layout file '{}'", path.display()))?;
    let layout: WorkerLayout = serde_json::from_str(&content).with_context(|| {
        format!(
            "Failed to parse worker layout JSON from '{}'",
            path.display()
        )
    })?;
    Ok(layout)
}

pub fn workers_from_layout(layout: &WorkerLayout) -> Vec<WorkerSpec> {
    layout
        .clients
        .iter()
        .map(|c| {
            let container_mode = match c.container_mode.as_str() {
                "packed" => ContainerMode::Packed,
                _ => ContainerMode::Singleton,
            };
            WorkerSpec {
                id: c.client_id.clone(),
                url: c.command_url.clone(),
                command_url: c.command_url.clone(),
                health_url: c.health_url.clone(),
                physical_worker_id: c.physical_worker_id.clone(),
                container_mode,
                profile_enabled: c.profile_enabled,
            }
        })
        .collect()
}

pub fn measured_active_participants(active: &[WorkerSpec]) -> Vec<&WorkerSpec> {
    active.iter().filter(|w| w.profile_enabled).collect()
}

fn frontload_profile_enabled_singletons_in_idle(
    idle: VecDeque<WorkerSpec>,
) -> VecDeque<WorkerSpec> {
    let mut profiled_singletons = Vec::new();
    let mut other_workers = Vec::new();
    for worker in idle {
        if worker.profile_enabled && worker.container_mode == ContainerMode::Singleton {
            profiled_singletons.push(worker);
        } else {
            other_workers.push(worker);
        }
    }
    profiled_singletons
        .into_iter()
        .chain(other_workers)
        .collect()
}

pub fn physical_groups<'a>(
    workers: impl Iterator<Item = &'a WorkerSpec>,
) -> HashMap<String, Vec<&'a WorkerSpec>> {
    let mut groups: HashMap<String, Vec<&'a WorkerSpec>> = HashMap::new();
    for w in workers {
        groups
            .entry(w.physical_worker_id.clone())
            .or_default()
            .push(w);
    }
    groups
}

#[derive(Debug, Clone, Serialize)]
pub struct NetworkPhaseMetrics {
    pub phase: String,
    pub conversation_size: usize,
    pub operation: String,
    pub request_count: usize,
    pub recipient_count: usize,
    pub success_count: usize,
    pub failure_count: usize,
    pub timeout_count: usize,
    pub connect_error_count: usize,
    pub max_parallelism: usize,
    pub effective_parallelism: usize,
    pub wall_ms: u128,
    pub retry_count: usize,
    pub retry_sleep_ms: u128,
    pub retry_pass_count: usize,
    pub failures: usize,
    pub worker_latency_p50_ms: Option<u128>,
    pub worker_latency_p95_ms: Option<u128>,
    pub worker_latency_p99_ms: Option<u128>,
    pub worker_latency_max_ms: Option<u128>,
    pub slowest_worker_ids: Vec<String>,
    #[serde(default)]
    pub logical_request_count: usize,
    #[serde(default)]
    pub physical_request_count: usize,
    #[serde(default)]
    pub singleton_request_count: usize,
    #[serde(default)]
    pub packed_request_count: usize,
    #[serde(default)]
    pub packed_logical_client_count: usize,
    #[serde(default)]
    pub profile_enabled_recipient_count: usize,
}

struct Progress {
    total_units: usize,
    completed_units: usize,
    start: Instant,
}

impl Progress {
    fn new(total_units: usize) -> Self {
        Self {
            total_units: total_units.max(1),
            completed_units: 0,
            start: Instant::now(),
        }
    }

    fn tick(&mut self, label: &str) {
        self.tick_units(1, label);
    }

    fn tick_units(&mut self, units: usize, label: &str) {
        self.completed_units = self
            .completed_units
            .saturating_add(units)
            .min(self.total_units);
        self.render(label);
    }

    fn render(&self, label: &str) {
        let width = 32usize;
        let ratio = self.completed_units as f64 / self.total_units as f64;
        let filled = ((ratio * width as f64).round() as usize).min(width);

        let mut bar = String::with_capacity(width);
        for _ in 0..filled {
            bar.push('#');
        }
        for _ in filled..width {
            bar.push('-');
        }

        let elapsed = self.start.elapsed();
        let eta = if self.completed_units == 0 {
            None
        } else {
            let elapsed_secs = elapsed.as_secs_f64();
            let per_unit = elapsed_secs / self.completed_units as f64;
            let remaining = self.total_units.saturating_sub(self.completed_units) as f64;
            Some(Duration::from_secs_f64(per_unit * remaining))
        };

        let percent = ratio * 100.0;
        let eta_text = eta
            .map(format_hms)
            .unwrap_or_else(|| "--:--:--".to_string());

        eprint!(
            "\r[{}] {:6.2}% | {}/{} units | elapsed {} | ETA {} | {}",
            bar,
            percent,
            self.completed_units,
            self.total_units,
            format_hms(elapsed),
            eta_text,
            label
        );
        let _ = io::stderr().flush();
    }

    fn finish(&self) {
        eprintln!();
    }
}

#[derive(Debug)]
struct FanoutController {
    max_parallelism: usize,
    min_parallelism: usize,
    current_parallelism: usize,
    adaptive: bool,
    stable_successes: usize,
    error_rate_threshold: f64,
    p95_threshold_ms: u128,
}

impl FanoutController {
    fn new(
        max_parallelism: usize,
        min_parallelism: usize,
        adaptive: bool,
        error_rate_threshold: f64,
        p95_threshold_ms: u128,
    ) -> Self {
        let max_parallelism = max_parallelism.max(1);
        let min_parallelism = min_parallelism.clamp(1, max_parallelism);
        let current_parallelism = if adaptive {
            ADAPTIVE_FANOUT_START
                .min(max_parallelism)
                .max(min_parallelism)
        } else {
            max_parallelism
        };

        Self {
            max_parallelism,
            min_parallelism,
            current_parallelism,
            adaptive,
            stable_successes: 0,
            error_rate_threshold,
            p95_threshold_ms,
        }
    }

    fn parallelism(&self) -> usize {
        self.current_parallelism.max(1)
    }

    fn record(&mut self, phase: &str, operation: &str, summary: &FanoutSummary) {
        if !self.adaptive {
            return;
        }

        let p95 = summary.latency_p95_ms.unwrap_or(0);
        let error_rate = if summary.request_count == 0 {
            0.0
        } else {
            summary.failure_count as f64 / summary.request_count as f64
        };
        let latency_spike = p95 >= self.p95_threshold_ms;
        let error_spike = error_rate >= self.error_rate_threshold && summary.failure_count > 0;
        let should_reduce = latency_spike || error_spike;

        if should_reduce {
            let previous = self.current_parallelism;
            self.current_parallelism = (self.current_parallelism / 2).max(self.min_parallelism);
            self.stable_successes = 0;

            if self.current_parallelism != previous {
                eprintln!(
                    "[fanout-adaptive] phase={} operation={} reducing parallelism {} -> {} failures={} error_rate={:.4} p95_ms={}",
                    phase,
                    operation,
                    previous,
                    self.current_parallelism,
                    summary.failure_count,
                    error_rate,
                    p95,
                );
            }
            return;
        }

        self.stable_successes += 1;
        if self.stable_successes >= FANOUT_STABLE_INCREASE_AFTER
            && self.current_parallelism < self.max_parallelism
        {
            let previous = self.current_parallelism;
            self.current_parallelism = (self.current_parallelism + 4).min(self.max_parallelism);
            self.stable_successes = 0;

            eprintln!(
                "[fanout-adaptive] phase={} operation={} increasing parallelism {} -> {} p95_ms={} stable_successes={}",
                phase,
                operation,
                previous,
                self.current_parallelism,
                p95,
                FANOUT_STABLE_INCREASE_AFTER
            );
        }
    }
}

#[derive(Debug, Clone, Default)]
struct FanoutSummary {
    request_count: usize,
    recipient_count: usize,
    success_count: usize,
    failure_count: usize,
    timeout_count: usize,
    connect_error_count: usize,
    max_parallelism: usize,
    effective_parallelism: usize,
    retry_pass_count: usize,
    wall_ms: u128,
    latency_p50_ms: Option<u128>,
    latency_p95_ms: Option<u128>,
    latency_p99_ms: Option<u128>,
    latency_max_ms: Option<u128>,
    slowest_worker_ids: Vec<String>,
}

pub fn parse_worker_specs(raw_specs: &[String]) -> Result<Vec<WorkerSpec>> {
    let mut workers = Vec::with_capacity(raw_specs.len());

    for raw in raw_specs {
        let spec = parse_worker_spec(raw)?;
        if workers.iter().any(|w: &WorkerSpec| w.id == spec.id) {
            return Err(anyhow!("Duplicate worker id '{}'", spec.id));
        }
        workers.push(spec);
    }

    if workers.is_empty() {
        return Err(anyhow!("At least one worker must be provided"));
    }

    Ok(workers)
}

pub fn run_dir_for(output_dir: &str, run_id: &str) -> PathBuf {
    PathBuf::from(output_dir).join(run_id)
}

pub fn run_staircase_benchmark(config: StaircaseConfig) -> Result<()> {
    let worker_threads = std::thread::available_parallelism()
        .map(|threads| threads.get())
        .unwrap_or(4);

    eprintln!(
        "[runtime] benchmark runner using multi-thread Tokio runtime with {} worker threads",
        worker_threads
    );

    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(worker_threads)
        .enable_all()
        .build()
        .context("Failed to build benchmark runner Tokio runtime")?
        .block_on(run_staircase_benchmark_async(config))
}

async fn run_staircase_benchmark_async(config: StaircaseConfig) -> Result<()> {
    let max_size = validate_config(&config, config.workers.len())?;

    let run_dir = run_dir_for(&config.output_dir, &config.run_id);
    fs::create_dir_all(&run_dir)?;
    let runner_events = RunnerEventLog::new(&run_dir);

    let max_fanout_parallelism = effective_max_fanout_parallelism(config.max_fanout_parallelism);
    let min_fanout_parallelism =
        effective_min_fanout_parallelism(config.min_fanout_parallelism, max_fanout_parallelism);
    let fanout_adaptive = effective_fanout_adaptive(config.fanout_adaptive, config.workers.len());
    let fanout_error_rate_threshold =
        effective_fanout_error_rate_threshold(config.fanout_error_rate_threshold);
    let fanout_p95_threshold_ms = effective_fanout_p95_threshold_ms(config.fanout_p95_threshold_ms);
    let mut fanout = FanoutController::new(
        max_fanout_parallelism,
        min_fanout_parallelism,
        fanout_adaptive,
        fanout_error_rate_threshold,
        fanout_p95_threshold_ms,
    );
    let http_pool_max_idle_per_host =
        effective_http_pool_max_idle_per_host(config.http_pool_max_idle_per_host);
    let runner_http_connect_timeout = Duration::from_millis(runner_http_connect_timeout_ms());
    let runner_http_request_timeout = Duration::from_millis(runner_http_request_timeout_ms());

    eprintln!(
        "[network] runner http_pool_max_idle_per_host={} connect_timeout_ms={} request_timeout_ms={} max_fanout_parallelism={} min_fanout_parallelism={} fanout_adaptive={} initial_effective_fanout_parallelism={} fanout_error_rate_threshold={:.4}",
        http_pool_max_idle_per_host,
        runner_http_connect_timeout.as_millis(),
        runner_http_request_timeout.as_millis(),
        max_fanout_parallelism,
        min_fanout_parallelism,
        fanout_adaptive,
        fanout.parallelism(),
        fanout_error_rate_threshold,
    );

    let http = reqwest::Client::builder()
        .connect_timeout(runner_http_connect_timeout)
        .timeout(runner_http_request_timeout)
        .pool_max_idle_per_host(http_pool_max_idle_per_host)
        .pool_idle_timeout(Duration::from_secs(30))
        .tcp_keepalive(Some(Duration::from_secs(60)))
        .build()
        .context("Failed to build HTTP client")?;

    wait_for_health(&http, &config.kr_url, Duration::from_secs(10))
        .await
        .with_context(|| format!("Key repository at {} is not healthy", config.kr_url))?;

    wait_for_health(&http, &config.relay_url, Duration::from_secs(10))
        .await
        .with_context(|| format!("Message relay at {} is not healthy", config.relay_url))?;

    let worker_health_timeout = Duration::from_secs(config.worker_health_timeout_seconds);
    let worker_health_poll = Duration::from_millis(config.worker_health_poll_ms);

    eprintln!(
        "[preflight] waiting up to {} for {} workers to become healthy",
        format_hms(worker_health_timeout),
        config.workers.len()
    );

    wait_for_all_workers_healthy(
        &http,
        &config.workers,
        worker_health_timeout,
        worker_health_poll,
        max_fanout_parallelism,
    )
    .await?;

    if config.preflight_only {
        eprintln!("[preflight] preflight-only mode complete; skipping Signal benchmark logic");
        return Ok(());
    }

    let piecewise_step = match (config.step_size_switch_at, config.step_size_after_switch) {
        (None, None) => None,
        (Some(switch_at), Some(after_switch)) if switch_at > 0 && after_switch > 0 => {
            Some((switch_at, after_switch))
        }
        _ => return Err(anyhow!("--step-size-switch-at and --step-size-after-switch must be provided together and be greater than 0")),
    };

    let mut plateau_rng = rand::rng();
    let plateau_sequence = build_plateau_sequence_for_order(
        config.min_size,
        max_size,
        &config.step_size,
        config.roundtrips,
        config.plateau_order,
        piecewise_step,
        &mut plateau_rng,
    );

    let total_units = estimate_total_units(
        &plateau_sequence,
        config.app_rounds,
        config.max_app_samples_per_payload,
        config.payload_sizes.source_count(),
    );

    eprintln!(
        "Scenario plan: plateau_order={}, plateaus={:?}, step_size={}, payload_sizes={}, app_cap={}, total_units≈{}",
        config.plateau_order,
        plateau_sequence,
        config.step_size,
        config.payload_sizes,
        config.max_app_samples_per_payload,
        total_units
    );

    let kr_url = config.kr_url.clone();
    let relay_url = config.relay_url.clone();

    let mut progress = Progress::new(total_units);
    progress.render("starting");

    let mut active: Vec<WorkerSpec> = Vec::new();
    let mut idle: VecDeque<WorkerSpec> = config.workers.iter().cloned().collect();
    if config.profile_only_singletons {
        idle = frontload_profile_enabled_singletons_in_idle(idle);
        eprintln!(
            "[sampling] front-loaded profile-enabled singleton joiners for PQXDH initiator coverage; packed clients remain unprofiled"
        );
    }

    let run_result = {
        let mut plateau_result = Ok(());
        for (plateau_idx, &target_size) in plateau_sequence.iter().enumerate() {
            eprintln!(
                "\n=== Plateau {}/{} | target active participants = {} ===",
                plateau_idx + 1,
                plateau_sequence.len(),
                target_size
            );

            if let Err(e) = transition_to_size(
                &http,
                &kr_url,
                &relay_url,
                &mut active,
                &mut idle,
                target_size,
                &mut fanout,
                &mut progress,
                plateau_idx + 1,
                &runner_events,
                config.profile_only_singletons,
            )
            .await
            {
                plateau_result = Err(e);
                break;
            }

            eprintln!(
                "\n[plateau {}] active participants = {} established_sessions = {}",
                target_size,
                active.len(),
                active
                    .len()
                    .saturating_sub(1)
                    .saturating_mul(active.len())
                    .saturating_div(2),
            );

            if let Err(e) = run_application_phase(
                &http,
                &kr_url,
                &relay_url,
                &mut active,
                target_size,
                config.app_rounds,
                config.max_app_samples_per_payload,
                &config.payload_sizes,
                &mut fanout,
                &mut progress,
                plateau_idx + 1,
                &runner_events,
            )
            .await
            {
                plateau_result = Err(e);
                break;
            }

            eprintln!("\n=== Plateau {} complete ===", target_size);
        }
        plateau_result
    };

    progress.finish();

    if let Err(ref e) = run_result {
        eprintln!("[runner] benchmark terminated with error: {:#}", e);
    }

    let worker_ids: Vec<String> = config.workers.iter().map(|w| w.id.clone()).collect();
    if !config.no_aggregate {
        if let Err(e) = aggregate_csv(&run_dir, &worker_ids, &config.worker_layout) {
            eprintln!("[aggregate] CSV aggregation failed: {:#}", e);
        }
    } else {
        eprintln!("[aggregate] --no-aggregate set, skipping CSV aggregation");
    }

    // Always write sidecars, even on failure
    if let Err(e) = write_post_benchmark_sidecars(&run_dir, &config.run_id) {
        eprintln!(
            "[sidecars] failed to write post-benchmark sidecars: {:#}",
            e
        );
    }

    if run_result.is_ok() {
        println!(
            "Signal staircase benchmark finished. Output in {}",
            run_dir.display()
        );
    }

    run_result
}

fn effective_max_fanout_parallelism(configured: usize) -> usize {
    if configured > 0 {
        configured
    } else {
        DEFAULT_MAX_FANOUT_PARALLELISM
    }
}

fn effective_min_fanout_parallelism(configured: usize, max_parallelism: usize) -> usize {
    let value = if configured > 0 {
        configured
    } else {
        DEFAULT_MIN_FANOUT_PARALLELISM
    };
    value.clamp(1, max_parallelism.max(1))
}

fn effective_fanout_adaptive(configured: Option<bool>, worker_count: usize) -> bool {
    configured.unwrap_or(worker_count >= 256)
}

fn effective_fanout_error_rate_threshold(configured: f64) -> f64 {
    if configured.is_finite() && configured > 0.0 {
        configured
    } else {
        DEFAULT_FANOUT_ERROR_RATE_THRESHOLD
    }
}

fn effective_fanout_p95_threshold_ms(configured: u128) -> u128 {
    if configured > 0 {
        configured
    } else {
        FANOUT_LATENCY_SPIKE_P95_MS
    }
}

fn effective_http_pool_max_idle_per_host(configured: usize) -> usize {
    if configured > 0 {
        configured
    } else {
        DEFAULT_HTTP_POOL_MAX_IDLE_PER_HOST
    }
}

fn runner_http_connect_timeout_ms() -> u64 {
    std::env::var("SIGNAL_RUNNER_HTTP_CONNECT_TIMEOUT_MS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_RUNNER_HTTP_CONNECT_TIMEOUT_MS)
}

fn runner_http_request_timeout_ms() -> u64 {
    std::env::var("SIGNAL_RUNNER_HTTP_REQUEST_TIMEOUT_MS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_RUNNER_HTTP_REQUEST_TIMEOUT_MS)
}

fn format_hms(d: Duration) -> String {
    let total = d.as_secs();
    let h = total / 3600;
    let m = (total % 3600) / 60;
    let s = total % 60;
    format!("{h:02}:{m:02}:{s:02}")
}

fn parse_worker_spec(raw: &str) -> Result<WorkerSpec> {
    let (id, url) = raw
        .split_once('=')
        .ok_or_else(|| anyhow!("Invalid worker '{}', expected ID=URL", raw))?;

    let id = id.trim();
    let url = url.trim().trim_end_matches('/');

    if id.is_empty() {
        return Err(anyhow!("Worker id cannot be empty in '{}'", raw));
    }
    if url.is_empty() {
        return Err(anyhow!("Worker url cannot be empty in '{}'", raw));
    }

    Ok(WorkerSpec::legacy(id.to_string(), url.to_string()))
}

async fn wait_for_health(http: &reqwest::Client, base_url: &str, timeout: Duration) -> Result<()> {
    let url = format!("{}/health", base_url.trim_end_matches('/'));
    let per_request_timeout = timeout.min(Duration::from_secs(5));

    retry_transient_http_async("service.health", None, &url, || async {
        let response = match http.get(&url).timeout(per_request_timeout).send().await {
            Ok(response) => response,
            Err(err) if is_transient_reqwest_error(&err) => {
                return RetryDecision::Transient(err.to_string())
            }
            Err(err) => return RetryDecision::Fatal(anyhow!(err)),
        };

        let status = response.status();

        if status.is_success() {
            return RetryDecision::Success(());
        }

        let body = response.text().await.unwrap_or_default();

        if is_transient_status(status) {
            return RetryDecision::Transient(format!("HTTP {}: {}", status, body));
        }

        RetryDecision::Fatal(anyhow!(
            "Health check failed with status {}: {}",
            status,
            body
        ))
    })
    .await
}

async fn wait_for_all_workers_healthy(
    http: &reqwest::Client,
    workers: &[WorkerSpec],
    timeout: Duration,
    poll: Duration,
    max_parallelism: usize,
) -> Result<()> {
    let start = Instant::now();
    let mut remaining: Vec<usize> = (0..workers.len()).collect();
    let mut last_report = Instant::now();
    let max_parallelism = max_parallelism.max(1);

    while start.elapsed() < timeout {
        let mut still_unhealthy = Vec::new();
        let mut latencies = Vec::new();
        let in_flight = Arc::new(AtomicUsize::new(0));
        let max_in_flight = Arc::new(AtomicUsize::new(0));
        let remaining_snapshot = remaining.clone();

        let mut probes = stream::iter(remaining_snapshot.into_iter())
            .map(|idx| {
                let worker = &workers[idx];
                let in_flight = Arc::clone(&in_flight);
                let max_in_flight = Arc::clone(&max_in_flight);
                async move {
                    let command_started = Instant::now();
                    let current = in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                    update_atomic_max(&max_in_flight, current);
                    let url = format!("{}/health", worker.url.trim_end_matches('/'));
                    let healthy = matches!(
                        http.get(&url).send().await,
                        Ok(resp) if resp.status().is_success()
                    );
                    in_flight.fetch_sub(1, Ordering::SeqCst);
                    (idx, healthy, command_started.elapsed().as_millis())
                }
            })
            .buffer_unordered(max_parallelism);

        while let Some((idx, healthy, latency_ms)) = probes.next().await {
            latencies.push(latency_ms);
            if !healthy {
                still_unhealthy.push(idx);
            }
        }

        let healthy_count = workers.len().saturating_sub(still_unhealthy.len());

        if still_unhealthy.is_empty() {
            eprintln!(
                "[preflight] all {} workers are healthy after {}",
                workers.len(),
                format_hms(start.elapsed())
            );
            emit_network_metrics(NetworkPhaseMetrics {
                phase: "preflight".to_string(),
                conversation_size: workers.len(),
                operation: "worker_health".to_string(),
                request_count: workers.len(),
                recipient_count: workers.len(),
                success_count: workers.len(),
                failure_count: 0,
                timeout_count: 0,
                connect_error_count: 0,
                max_parallelism,
                effective_parallelism: max_in_flight.load(Ordering::SeqCst),
                wall_ms: start.elapsed().as_millis(),
                retry_count: 0,
                retry_sleep_ms: 0,
                retry_pass_count: 0,
                failures: 0,
                worker_latency_p50_ms: Some(0),
                worker_latency_p95_ms: Some(0),
                worker_latency_p99_ms: Some(0),
                worker_latency_max_ms: Some(0),
                slowest_worker_ids: Vec::new(),
                logical_request_count: workers.len(),
                physical_request_count: workers.len(),
                singleton_request_count: workers.len(),
                packed_request_count: 0,
                packed_logical_client_count: 0,
                profile_enabled_recipient_count: workers
                    .iter()
                    .filter(|w| w.profile_enabled)
                    .count(),
            });
            return Ok(());
        }

        if last_report.elapsed() >= Duration::from_secs(5) {
            let examples: Vec<String> = still_unhealthy
                .iter()
                .take(10)
                .map(|&idx| workers[idx].id.clone())
                .collect();

            eprintln!(
                "[preflight] {}/{} workers healthy; still waiting for {}. Examples: {:?}",
                healthy_count,
                workers.len(),
                still_unhealthy.len(),
                examples
            );
            last_report = Instant::now();
        }

        remaining = still_unhealthy;
        tokio::time::sleep(poll).await;
    }

    Err(anyhow!(
        "Timeout waiting for worker readiness after {}. {}/{} workers still unhealthy.",
        format_hms(timeout),
        remaining.len(),
        workers.len()
    ))
}

#[derive(Debug, Clone)]
struct WorkerCommandContext {
    request_id: String,
    phase: Option<String>,
    benchmark_plateau_index: Option<usize>,
    benchmark_target_size: Option<usize>,
    benchmark_active_size: Option<usize>,
    benchmark_phase: Option<String>,
    benchmark_operation: Option<String>,
    benchmark_operation_seq: Option<usize>,
    benchmark_payload_size: Option<usize>,
    benchmark_workflow_id: Option<u64>,
    workflow_pair_index: Option<u32>,
    workflow_pair_count: Option<u32>,
}

impl WorkerCommandContext {
    fn new(worker: &WorkerSpec, command: &Command) -> Self {
        Self::with_metadata(worker, command, None)
    }

    fn with_metadata(worker: &WorkerSpec, command: &Command, phase: Option<&str>) -> Self {
        Self::with_cursor(worker, command, phase, None)
    }

    fn with_cursor(
        worker: &WorkerSpec,
        command: &Command,
        phase: Option<&str>,
        cursor: Option<&BenchmarkCursor>,
    ) -> Self {
        let seq = WORKER_REQUEST_COUNTER.fetch_add(1, Ordering::Relaxed);
        let request_id = format!(
            "runner-{}-{}-{}-{}",
            std::process::id(),
            worker.id,
            command.kind(),
            seq
        );

        Self {
            request_id,
            phase: phase.map(ToOwned::to_owned),
            benchmark_plateau_index: cursor.map(|c| c.plateau_index),
            benchmark_target_size: cursor.map(|c| c.target_size),
            benchmark_active_size: cursor.map(|c| c.active_size),
            benchmark_phase: cursor.as_ref().map(|c| c.phase.clone()),
            benchmark_operation: cursor.as_ref().map(|c| c.operation.clone()),
            benchmark_operation_seq: cursor.and_then(|c| c.operation_seq),
            benchmark_payload_size: cursor.and_then(|c| c.payload_size),
            benchmark_workflow_id: None,
            workflow_pair_index: None,
            workflow_pair_count: None,
        }
    }

    fn with_workflow(mut self, workflow_id: u64, pair_index: usize, pair_count: usize) -> Self {
        self.benchmark_workflow_id = Some(workflow_id);
        self.workflow_pair_index = Some(pair_index as u32);
        self.workflow_pair_count = Some(pair_count as u32);
        self
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WorkerCommandErrorClass {
    TransportRetryable,
    FatalHttpStatus,
    FatalDecode,
}

impl WorkerCommandErrorClass {
    fn as_str(self) -> &'static str {
        match self {
            WorkerCommandErrorClass::TransportRetryable => "transport-retryable",
            WorkerCommandErrorClass::FatalHttpStatus => "fatal-http-status",
            WorkerCommandErrorClass::FatalDecode => "fatal-decode",
        }
    }
}

#[derive(Debug)]
struct WorkerCommandError {
    worker_id: String,
    command: &'static str,
    url: String,
    request_id: String,
    attempts: usize,
    classification: WorkerCommandErrorClass,
    last_error: String,
    diagnostic: Option<String>,
}

impl std::fmt::Display for WorkerCommandError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "runner.worker_command failed: worker={} command={} url={} request_id={} attempts={} classification={} last_error={}",
            self.worker_id, self.command, self.url, self.request_id, self.attempts, self.classification.as_str(), self.last_error
        )?;
        if let Some(diagnostic) = &self.diagnostic {
            write!(f, " diagnostic={}", diagnostic)?;
        }
        Ok(())
    }
}

impl StdError for WorkerCommandError {}

async fn record_worker_oom_if_evidenced(
    runner_events: &RunnerEventLog,
    cursor: &BenchmarkCursor,
    worker: &WorkerSpec,
    error: &anyhow::Error,
    action: &str,
    reassigned_to: Option<&WorkerSpec>,
) -> Result<bool> {
    let Some(evidence) = runner_events.find_oom_evidence(worker).await else {
        return Ok(false);
    };
    runner_events.record_oom_failure(cursor, worker, error, &evidence, action, reassigned_to)?;
    Ok(true)
}

fn worker_reqwest_error_diagnostic(err: &reqwest::Error) -> String {
    let mut parts = Vec::new();
    parts.push(format!("top_level={}", err));
    parts.push(format!("is_connect={}", err.is_connect()));
    parts.push(format!("is_timeout={}", err.is_timeout()));
    parts.push(format!("is_request={}", err.is_request()));
    parts.push(format!("is_body={}", err.is_body()));
    parts.push(format!(
        "status={}",
        err.status()
            .map(|s| s.to_string())
            .unwrap_or_else(|| "-".to_string())
    ));
    let inferred_stage = if err.is_connect() {
        "connect"
    } else if err.is_timeout() {
        "timeout"
    } else if err.is_body() {
        "reading response body"
    } else if err.is_request() {
        "writing request"
    } else {
        "unknown"
    };
    parts.push(format!("inferred_stage={}", inferred_stage));
    let mut source = err.source();
    let mut idx = 0usize;
    while let Some(err) = source {
        parts.push(format!("source[{}]={}", idx, err));
        source = err.source();
        idx += 1;
    }
    parts.join("; ")
}

async fn retry_worker_command_sleep(
    worker: &WorkerSpec,
    command_name: &str,
    attempt: usize,
    delay: &mut Duration,
    url: &str,
    err_text: &str,
) {
    let sleep_for = worker_command_with_jitter(*delay);
    eprintln!(
        "[retry] op=runner.worker_command worker={} command={} attempt={}/{} delay_ms={} url={} error={}",
        worker.id, command_name, attempt, WORKER_COMMAND_MAX_ATTEMPTS, sleep_for.as_millis(), url, err_text
    );
    tokio::time::sleep(sleep_for).await;
    *delay = worker_command_next_delay(*delay);
}

async fn send_command_with_context(
    http: &reqwest::Client,
    worker: &WorkerSpec,
    command: &Command,
    context: &WorkerCommandContext,
) -> Result<CommandResponse> {
    let url = format!("{}/command", worker.url);
    let command_name = command.kind();
    let mut delay = WORKER_COMMAND_INITIAL_DELAY;
    let request = CommandRequestEnvelope {
        request_id: context.request_id.clone(),
        command: command.clone(),
        phase: context.phase.clone(),
        benchmark_plateau_index: context.benchmark_plateau_index,
        benchmark_target_size: context.benchmark_target_size,
        benchmark_active_size: context.benchmark_active_size,
        benchmark_phase: context.benchmark_phase.clone(),
        benchmark_operation: context.benchmark_operation.clone(),
        benchmark_operation_seq: context.benchmark_operation_seq,
        benchmark_payload_size: context.benchmark_payload_size,
        benchmark_workflow_id: context.benchmark_workflow_id,
        workflow_pair_index: context.workflow_pair_index,
        workflow_pair_count: context.workflow_pair_count,
    };

    for attempt in 1..=WORKER_COMMAND_MAX_ATTEMPTS {
        let response = match http.post(&url).json(&request).send().await {
            Ok(response) => response,
            Err(err)
                if is_transient_reqwest_error(&err) || is_connect_stage_reqwest_error(&err) =>
            {
                let err_text = err.to_string();
                let diagnostic = worker_reqwest_error_diagnostic(&err);
                if attempt == WORKER_COMMAND_MAX_ATTEMPTS {
                    return Err(WorkerCommandError {
                        worker_id: worker.id.clone(),
                        command: command_name,
                        url: url.clone(),
                        request_id: context.request_id.clone(),
                        attempts: attempt,
                        classification: WorkerCommandErrorClass::TransportRetryable,
                        last_error: err_text,
                        diagnostic: Some(diagnostic),
                    }
                    .into());
                }
                retry_worker_command_sleep(
                    worker,
                    command_name,
                    attempt,
                    &mut delay,
                    &url,
                    &format!("{} ({})", err_text, diagnostic),
                )
                .await;
                continue;
            }
            Err(err) => {
                return Err(WorkerCommandError {
                    worker_id: worker.id.clone(),
                    command: command_name,
                    url,
                    request_id: context.request_id.clone(),
                    attempts: attempt,
                    classification: WorkerCommandErrorClass::FatalDecode,
                    last_error: err.to_string(),
                    diagnostic: Some(worker_reqwest_error_diagnostic(&err)),
                }
                .into());
            }
        };

        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            let last_error = format!("HTTP {}: {}", status, body);
            if is_transient_status(status) && attempt < WORKER_COMMAND_MAX_ATTEMPTS {
                retry_worker_command_sleep(
                    worker,
                    command_name,
                    attempt,
                    &mut delay,
                    &url,
                    &last_error,
                )
                .await;
                continue;
            }
            return Err(WorkerCommandError {
                worker_id: worker.id.clone(),
                command: command_name,
                url,
                request_id: context.request_id.clone(),
                attempts: attempt,
                classification: WorkerCommandErrorClass::FatalHttpStatus,
                last_error,
                diagnostic: None,
            }
            .into());
        }

        match response.json::<CommandResponse>().await {
            Ok(parsed) => return Ok(parsed),
            Err(err) if is_transient_reqwest_error(&err) => {
                let err_text = err.to_string();
                let diagnostic = worker_reqwest_error_diagnostic(&err);
                if attempt == WORKER_COMMAND_MAX_ATTEMPTS {
                    return Err(WorkerCommandError {
                        worker_id: worker.id.clone(),
                        command: command_name,
                        url,
                        request_id: context.request_id.clone(),
                        attempts: attempt,
                        classification: WorkerCommandErrorClass::TransportRetryable,
                        last_error: err_text,
                        diagnostic: Some(diagnostic),
                    }
                    .into());
                }
                retry_worker_command_sleep(
                    worker,
                    command_name,
                    attempt,
                    &mut delay,
                    &url,
                    &format!("{} ({})", err_text, diagnostic),
                )
                .await;
                continue;
            }
            Err(err) => {
                return Err(WorkerCommandError {
                    worker_id: worker.id.clone(),
                    command: command_name,
                    url,
                    request_id: context.request_id.clone(),
                    attempts: attempt,
                    classification: WorkerCommandErrorClass::FatalDecode,
                    last_error: err.to_string(),
                    diagnostic: Some(worker_reqwest_error_diagnostic(&err)),
                }
                .into());
            }
        }
    }

    unreachable!("worker command retry loop always returns")
}

fn worker_command_next_delay(delay: Duration) -> Duration {
    let doubled_ms = delay.as_millis().saturating_mul(2);
    let max_ms = WORKER_COMMAND_MAX_DELAY.as_millis();
    Duration::from_millis(doubled_ms.min(max_ms) as u64)
}

fn worker_command_with_jitter(delay: Duration) -> Duration {
    let base_ms = delay.as_millis() as u64;
    let jitter_cap_ms = (base_ms / 10).clamp(1, 100);
    let jitter_ms = rand::rng().random_range(0..=jitter_cap_ms);
    Duration::from_millis(base_ms + jitter_ms)
}

async fn send_cmd_expect_ok_fragment(
    http: &reqwest::Client,
    worker: &WorkerSpec,
    command: &Command,
    ok_fragment: &str,
) -> Result<String> {
    let context = WorkerCommandContext::new(worker, command);
    send_cmd_expect_ok_fragment_with_context(http, worker, command, ok_fragment, &context).await
}

async fn send_cmd_expect_ok_fragment_with_context(
    http: &reqwest::Client,
    worker: &WorkerSpec,
    command: &Command,
    ok_fragment: &str,
    context: &WorkerCommandContext,
) -> Result<String> {
    let response = send_command_with_context(http, worker, command, context).await?;

    match response.status.as_str() {
        "ok" if response.message.contains(ok_fragment) => Ok(response.message),
        "ok" => Err(anyhow!(
            "Worker {} returned unexpected ok message: {}",
            worker.id,
            response.message
        )),
        "error" => Err(anyhow!("Worker {} error: {}", worker.id, response.message)),
        other => Err(anyhow!(
            "Worker {} returned unknown status '{}': {}",
            worker.id,
            other,
            response.message
        )),
    }
}

// ── Participant setup ─────────────────────────────────────────────────────────

async fn register_participant(http: &reqwest::Client, worker: &WorkerSpec) -> Result<()> {
    send_cmd_expect_ok_fragment(http, worker, &Command::RegisterParticipant, "registered").await?;
    Ok(())
}

async fn generate_publish_prekey_bundle(http: &reqwest::Client, worker: &WorkerSpec) -> Result<()> {
    send_cmd_expect_ok_fragment(
        http,
        worker,
        &Command::PublishPrekeyBundle,
        "prekeys stored locally",
    )
    .await?;
    send_cmd_expect_ok_fragment(
        http,
        worker,
        &Command::GeneratePrekeyBundle,
        "prekey bundle generated and published",
    )
    .await?;
    Ok(())
}

async fn establish_sessions(
    http: &reqwest::Client,
    actor: &WorkerSpec,
    existing_participants: &[WorkerSpec],
    _fanout: &mut FanoutController,
    conversation_size: usize,
    cursor: &BenchmarkCursor,
    decrypt_on_peer: bool,
) -> Result<()> {
    if existing_participants.is_empty() {
        return Ok(());
    }

    let workflow_id = next_workflow_id();
    let pair_count = existing_participants.len();
    for (pair_index, peer) in existing_participants.iter().enumerate() {
        let establish_command = Command::EstablishSessions {
            participants: vec![peer.id.clone()],
            conversation_size: Some(conversation_size),
        };
        let establish_context = WorkerCommandContext::with_cursor(
            actor,
            &establish_command,
            Some("handshake.initiator_process_bundle"),
            Some(cursor),
        )
        .with_workflow(workflow_id, pair_index, pair_count);
        send_cmd_expect_ok_fragment_with_context(
            http,
            actor,
            &establish_command,
            "session establishment",
            &establish_context,
        )
        .await?;

        let initial_message = format!("signal-initial-handshake:{}->{}", actor.id, peer.id);
        let encrypt_command = Command::EncryptMessage {
            recipient: peer.id.clone(),
            message: initial_message,
            conversation_size: Some(2),
        };
        let encrypt_context = WorkerCommandContext::with_metadata(
            actor,
            &encrypt_command,
            Some("handshake.initial_message_encrypt"),
        );
        send_cmd_expect_ok_fragment_with_context(
            http,
            actor,
            &encrypt_command,
            "encrypted and sent",
            &encrypt_context,
        )
        .await?;

        if decrypt_on_peer {
            let decrypt_command = Command::DecryptMessage {
                sender: actor.id.clone(),
                profile: true,
                conversation_size: Some(2),
            };
            let decrypt_context = WorkerCommandContext::with_metadata(
                peer,
                &decrypt_command,
                Some("handshake.initial_message_decrypt"),
            );
            send_cmd_expect_ok_fragment_with_context(
                http,
                peer,
                &decrypt_command,
                "pairwise message received",
                &decrypt_context,
            )
            .await?;

            let update_opks_command = Command::UpdateOneTimePrekeys;
            let update_opks_context = WorkerCommandContext::with_metadata(
                peer,
                &update_opks_command,
                Some("prekey.maintenance_after_handshake"),
            );
            send_cmd_expect_ok_fragment_with_context(
                http,
                peer,
                &update_opks_command,
                "one-time prekey stock",
                &update_opks_context,
            )
            .await?;
        }
    }

    Ok(())
}

async fn broadcast_message(
    http: &reqwest::Client,
    sender: &WorkerSpec,
    recipients: &[WorkerSpec],
    payload: &str,
    fanout: &mut FanoutController,
    profiling_recipient_id: Option<&str>,
    cursor: &BenchmarkCursor,
) -> Result<()> {
    let conversation_size = recipients.len() + 1;

    let send_workflow_id = next_workflow_id();
    let pair_count = recipients.len();
    for (pair_index, recipient) in recipients.iter().enumerate() {
        let ctx = WorkerCommandContext::with_cursor(
            sender,
            &Command::EncryptMessage {
                recipient: recipient.id.clone(),
                message: payload.to_string(),
                conversation_size: Some(conversation_size),
            },
            None,
            Some(cursor),
        )
        .with_workflow(send_workflow_id, pair_index, pair_count);
        send_cmd_expect_ok_fragment_with_context(
            http,
            sender,
            &Command::EncryptMessage {
                recipient: recipient.id.clone(),
                message: payload.to_string(),
                conversation_size: Some(conversation_size),
            },
            "encrypted and sent",
            &ctx,
        )
        .await?;
    }

    // Receive at each recipient
    let commands_by_physical = build_batch_commands(recipients, |worker| {
        let is_profiled = profiling_recipient_id == Some(worker.id.as_str());
        let receive_workflow_id = is_profiled.then(next_workflow_id);
        BatchFanoutCommand {
            participant_id: worker.id.clone(),
            request_id: None,
            command: Command::DecryptMessage {
                sender: sender.id.clone(),
                profile: is_profiled,
                conversation_size: Some(conversation_size),
            },
            phase: Some("application.fanout_receive_message".to_string()),
            profile: is_profiled.then_some(true),
            benchmark_plateau_index: is_profiled.then_some(cursor.plateau_index),
            benchmark_target_size: is_profiled.then_some(cursor.target_size),
            benchmark_active_size: is_profiled.then_some(cursor.active_size),
            benchmark_phase: is_profiled.then(|| cursor.phase.clone()),
            benchmark_operation: is_profiled.then(|| "receive_message_delivery".to_string()),
            benchmark_operation_seq: is_profiled.then_some(cursor.operation_seq).flatten(),
            benchmark_payload_size: is_profiled.then_some(cursor.payload_size).flatten(),
            benchmark_workflow_id: receive_workflow_id,
            workflow_pair_index: is_profiled.then_some(0),
            workflow_pair_count: is_profiled.then_some(1),
        }
    });

    batch_fanout_workers(
        http,
        "application",
        recipients.len() + 1,
        "receive_message",
        recipients,
        fanout,
        &commands_by_physical,
    )
    .await?;

    Ok(())
}

// ── Transition helpers ────────────────────────────────────────────────────────

async fn enroll_participants(
    http: &reqwest::Client,
    _kr_url: &str,
    _relay_url: &str,
    active: &mut Vec<WorkerSpec>,
    idle: &mut VecDeque<WorkerSpec>,
    batch_size: usize,
    fanout: &mut FanoutController,
    progress: &mut Progress,
    plateau_index: usize,
    target_size: usize,
    runner_events: &RunnerEventLog,
    profile_only_singletons: bool,
) -> Result<()> {
    if batch_size == 0 {
        return Err(anyhow!("Cannot enroll zero participants"));
    }
    if idle.len() < batch_size {
        return Err(anyhow!(
            "Requested enroll batch of {} participants, but only {} idle workers available",
            batch_size,
            idle.len()
        ));
    }

    let mut enrollments = Vec::with_capacity(batch_size);
    for _ in 0..batch_size {
        let participant = idle
            .pop_front()
            .ok_or_else(|| anyhow!("No idle worker available"))?;
        enrollments.push(participant);
    }

    let mut prepared = Vec::with_capacity(enrollments.len());
    for participant in enrollments {
        let cursor = BenchmarkCursor::new(
            plateau_index,
            target_size,
            active.len(),
            "enrollment",
            "prepare_participant",
        );
        if let Err(error) = register_participant(http, &participant).await {
            if record_worker_oom_if_evidenced(
                runner_events,
                &cursor,
                &participant,
                &error,
                "drop_idle_participant",
                None,
            )
            .await?
            {
                continue;
            }
            return Err(error);
        }
        if let Err(error) = generate_publish_prekey_bundle(http, &participant).await {
            if record_worker_oom_if_evidenced(
                runner_events,
                &cursor,
                &participant,
                &error,
                "drop_idle_participant",
                None,
            )
            .await?
            {
                continue;
            }
            return Err(error);
        }
        prepared.push(participant);
    }

    if prepared.is_empty() {
        return Ok(());
    }

    // Establish pairwise sessions from each new participant to every participant that
    // will be active after this enrollment batch. This includes peers enrolled in the
    // same batch; otherwise the first plateau in a batched ascent lacks new-new sessions.
    let existing_ids: Vec<WorkerSpec> = active.clone();
    if profile_only_singletons {
        // When only singletons carry profiling, prioritise profile-enabled
        // initiators so every (singleton→peer) session establishment runs
        // PQXDH while no responder-side session exists yet. Without this
        // ordering the bidirectional handshake creates a session on the
        // singleton in loop 1 (via DecryptMessage), and loop 2's
        // EstablishSessions hits has_session_with=true → skips all PQXDH
        // crypto → no profiled initiator data.

        // Phase A: existing profile-enabled → new non-profiled
        //          decrypt_on_peer = true (packed workers need the session)
        let profile_enabled_existing: Vec<_> = existing_ids
            .iter()
            .filter(|w| w.profile_enabled)
            .cloned()
            .collect();
        let non_profile_prepared: Vec<_> = prepared
            .iter()
            .filter(|w| !w.profile_enabled)
            .cloned()
            .collect();
        for existing in &profile_enabled_existing {
            let est_cursor = BenchmarkCursor::new(
                plateau_index,
                target_size,
                active.len(),
                "enrollment",
                "establish_session_existing_profiled",
            );
            if !non_profile_prepared.is_empty() {
                establish_sessions(
                    http,
                    existing,
                    &non_profile_prepared,
                    fanout,
                    target_size,
                    &est_cursor,
                    true,
                )
                .await?;
            }
        }

        // Phase B: existing profile-enabled → new profile-enabled
        //          decrypt_on_peer = false (the other singleton gets its
        //          own profiled initiator turn in Phase C)
        let profile_prepared: Vec<_> = prepared
            .iter()
            .filter(|w| w.profile_enabled)
            .cloned()
            .collect();
        for existing in &profile_enabled_existing {
            if profile_prepared.is_empty() {
                break;
            }
            let est_cursor = BenchmarkCursor::new(
                plateau_index,
                target_size,
                active.len(),
                "enrollment",
                "establish_session_existing_to_new_profiled",
            );
            establish_sessions(
                http,
                existing,
                &profile_prepared,
                fanout,
                target_size,
                &est_cursor,
                false,
            )
            .await?;
        }

        // Phase C: new profile-enabled → existing + peer-new
        for p in &profile_prepared {
            let mut peers: Vec<WorkerSpec> = existing_ids.clone();
            peers.extend(prepared.iter().filter(|peer| peer.id != p.id).cloned());
            let est_cursor = BenchmarkCursor::new(
                plateau_index,
                target_size,
                active.len(),
                "enrollment",
                "establish_session_new_profiled",
            );
            establish_sessions(http, p, &peers, fanout, target_size, &est_cursor, true).await?;
        }

        // Phase D: existing non-profiled → prepared
        let non_profile_existing: Vec<_> = existing_ids
            .iter()
            .filter(|w| !w.profile_enabled)
            .cloned()
            .collect();
        for existing in &non_profile_existing {
            let est_cursor = BenchmarkCursor::new(
                plateau_index,
                target_size,
                active.len(),
                "enrollment",
                "establish_session_existing_nonprofiled",
            );
            establish_sessions(
                http,
                existing,
                &prepared,
                fanout,
                target_size,
                &est_cursor,
                true,
            )
            .await?;
        }

        // Phase E: new non-profiled → existing + peer-new
        for participant in &non_profile_prepared {
            let mut peers = existing_ids.clone();
            peers.extend(
                prepared
                    .iter()
                    .filter(|peer| peer.id != participant.id)
                    .cloned(),
            );
            let est_cursor = BenchmarkCursor::new(
                plateau_index,
                target_size,
                active.len(),
                "enrollment",
                "establish_session_new_nonprofiled",
            );
            establish_sessions(
                http,
                participant,
                &peers,
                fanout,
                target_size,
                &est_cursor,
                true,
            )
            .await?;
        }
    } else {
        for (idx, participant) in prepared.iter().enumerate() {
            let mut peers = existing_ids.clone();
            peers.extend(
                prepared
                    .iter()
                    .enumerate()
                    .filter(|(peer_idx, _)| *peer_idx != idx)
                    .map(|(_, peer)| peer.clone()),
            );
            let est_cursor = BenchmarkCursor::new(
                plateau_index,
                target_size,
                active.len(),
                "enrollment",
                "establish_session",
            );
            establish_sessions(
                http,
                participant,
                &peers,
                fanout,
                target_size,
                &est_cursor,
                true,
            )
            .await?;
        }

        // Also establish sessions from existing participants to the new ones.
        for existing in &existing_ids {
            let enroll_cursor = BenchmarkCursor::new(
                plateau_index,
                target_size,
                active.len(),
                "enrollment",
                "establish_session_reverse",
            );
            establish_sessions(
                http,
                existing,
                &prepared,
                fanout,
                target_size,
                &enroll_cursor,
                true,
            )
            .await?;
        }
    }

    let enrolled_count = prepared.len();
    active.extend(prepared);
    let new_ids: Vec<String> = active.iter().map(|w| w.id.clone()).collect();
    progress.tick_units(
        enrolled_count,
        &format!(
            "enrolled {} participants {:?} total={}",
            enrolled_count,
            new_ids,
            active.len()
        ),
    );
    Ok(())
}

async fn deactivate_participants(
    http: &reqwest::Client,
    active: &mut Vec<WorkerSpec>,
    idle: &mut VecDeque<WorkerSpec>,
    batch_size: usize,
    _fanout: &mut FanoutController,
    progress: &mut Progress,
) -> Result<()> {
    if active.len() <= 1 {
        return Err(anyhow!("Cannot deactivate the last remaining participant"));
    }
    if batch_size == 0 {
        return Err(anyhow!("Cannot deactivate zero participants"));
    }
    if batch_size >= active.len() {
        return Err(anyhow!(
            "Cannot deactivate {} participants from {} active",
            batch_size,
            active.len()
        ));
    }

    let removed: Vec<WorkerSpec> = (0..batch_size)
        .map(|_| {
            let idx = rand::rng().random_range(0..active.len());
            active.remove(idx)
        })
        .collect();
    let removed_ids: Vec<String> = removed.iter().map(|w| w.id.clone()).collect();

    if let Some(notifier) = active.first() {
        let _ = send_cmd_expect_ok_fragment(
            http,
            notifier,
            &Command::RemoveParticipants {
                participants: removed_ids.clone(),
            },
            "deactivated",
        )
        .await;
    }

    idle.extend(removed);

    progress.tick_units(
        batch_size,
        &format!("deactivated {} participants", batch_size),
    );
    Ok(())
}

async fn transition_to_size(
    http: &reqwest::Client,
    kr_url: &str,
    relay_url: &str,
    active: &mut Vec<WorkerSpec>,
    idle: &mut VecDeque<WorkerSpec>,
    target_size: usize,
    fanout: &mut FanoutController,
    progress: &mut Progress,
    plateau_index: usize,
    runner_events: &RunnerEventLog,
    profile_only_singletons: bool,
) -> Result<()> {
    while active.len() < target_size {
        if idle.is_empty() {
            eprintln!(
                "[oom-attrition] plateau target {} cannot be reached; active={} no idle workers remain",
                target_size,
                active.len()
            );
            break;
        }
        let remaining = target_size - active.len();
        let batch_size = remaining.min(idle.len()).min(MAX_RANDOM_BATCH_SIZE).max(1);
        enroll_participants(
            http,
            kr_url,
            relay_url,
            active,
            idle,
            batch_size,
            fanout,
            progress,
            plateau_index,
            target_size,
            runner_events,
            profile_only_singletons,
        )
        .await?;
    }

    while active.len() > target_size {
        let remaining = active.len() - target_size;
        let batch_size = remaining
            .min(active.len().saturating_sub(1))
            .min(MAX_RANDOM_BATCH_SIZE)
            .max(1);
        deactivate_participants(http, active, idle, batch_size, fanout, progress).await?;
    }

    Ok(())
}

// ── Application phase ─────────────────────────────────────────────────────────

async fn run_application_phase(
    http: &reqwest::Client,
    _kr_url: &str,
    _relay_url: &str,
    active: &mut Vec<WorkerSpec>,
    plateau_size: usize,
    app_rounds: usize,
    max_app_samples_per_payload: usize,
    payload_sizes: &PayloadSizes,
    fanout: &mut FanoutController,
    progress: &mut Progress,
    plateau_index: usize,
    runner_events: &RunnerEventLog,
) -> Result<()> {
    if active.len() < 2 {
        eprintln!(
            "\n[plateau {}] application phase skipped: fewer than 2 active participants",
            plateau_size
        );
        return Ok(());
    }

    let per_payload_count =
        app_sends_per_plateau(plateau_size, app_rounds, max_app_samples_per_payload);
    if per_payload_count == 0 {
        return Ok(());
    }

    let mut payload_rng = rand::rng();
    for payload_source in payload_sizes.sources() {
        eprintln!(
            "\n[plateau {}] application phase: {} sends at {}",
            plateau_size,
            per_payload_count,
            payload_source.phase_label()
        );

        let mut seq_no = 0usize;
        while seq_no < per_payload_count {
            if active.len() < 2 {
                eprintln!(
                    "\n[plateau {}] application phase stopped after OOM attrition: fewer than 2 active participants",
                    plateau_size
                );
                return Ok(());
            }
            let mut profiled_indices: Vec<usize> = active
                .iter()
                .enumerate()
                .filter_map(|(idx, worker)| worker.profile_enabled.then_some(idx))
                .collect();
            if profiled_indices.is_empty() {
                profiled_indices = (0..active.len()).collect();
            }
            let actor_selection_idx =
                sampled_participant_index(profiled_indices.len(), per_payload_count, seq_no);
            let actor_idx = profiled_indices[actor_selection_idx];
            let actor = active[actor_idx].clone();
            let payload_size = payload_source.sample(&mut payload_rng);
            let payload =
                deterministic_payload(payload_size, plateau_size, payload_size, seq_no, &actor.id);

            let recipient_indices: Vec<usize> =
                (0..active.len()).filter(|&j| j != actor_idx).collect();
            let recipient_workers: Vec<WorkerSpec> = recipient_indices
                .iter()
                .map(|&i| active[i].clone())
                .collect();

            let profiling_recipient_id = if profiled_indices.len() > 1 {
                let profiled_recipient_indices: Vec<usize> = profiled_indices
                    .iter()
                    .copied()
                    .filter(|&i| i != actor_idx)
                    .collect();
                let sampled_pos = sampled_participant_index(
                    profiled_recipient_indices.len(),
                    per_payload_count,
                    seq_no,
                );
                Some(active[profiled_recipient_indices[sampled_pos]].id.clone())
            } else {
                None
            };

            let cursor = BenchmarkCursor::new(
                plateau_index,
                plateau_size,
                active.len(),
                "application",
                "broadcast_message",
            )
            .at_operation(seq_no + 1, Some(payload_size));
            let broadcast = broadcast_message(
                http,
                &actor,
                &recipient_workers,
                &payload,
                fanout,
                profiling_recipient_id.as_deref(),
                &cursor,
            )
            .await;

            if let Err(error) = broadcast {
                if record_worker_oom_if_evidenced(
                    runner_events,
                    &cursor,
                    &actor,
                    &error,
                    "remove_active_actor_and_retry",
                    active.iter().find(|worker| worker.id != actor.id),
                )
                .await?
                {
                    active.retain(|worker| worker.id != actor.id);
                    continue;
                }

                if let Some(batch_error) = error.downcast_ref::<BatchFanoutError>() {
                    let mut oom_ids = HashSet::new();
                    for failure in &batch_error.failures {
                        if record_worker_oom_if_evidenced(
                            runner_events,
                            &cursor,
                            &failure.worker,
                            &failure.error,
                            "remove_active_recipient_and_retry",
                            Some(&actor),
                        )
                        .await?
                        {
                            oom_ids.insert(failure.worker.id.clone());
                        } else if failure.worker.profile_enabled
                            && failure.worker.container_mode == ContainerMode::Singleton
                        {
                            let failure_class = classify_worker_error(&failure.error);
                            if matches!(
                                failure_class,
                                "app_heap_budget_exceeded"
                                    | "app_heap_budget_allocator_abort"
                                    | "embedded_budget_timeout"
                                    | "cpu_starvation_timeout"
                            ) {
                                oom_ids.insert(failure.worker.id.clone());
                            }
                        }
                    }
                    if oom_ids.len() == batch_error.failures.len() {
                        active.retain(|worker| !oom_ids.contains(&worker.id));
                        continue;
                    }
                }

                return Err(error);
            }

            progress.tick(&format!(
                "plateau {} app payload={} {}/{} actor={} recipients={}",
                plateau_size,
                payload_size,
                seq_no + 1,
                per_payload_count,
                actor.id,
                recipient_workers.len()
            ));
            seq_no += 1;
        }
    }

    Ok(())
}

fn sampled_participant_index(member_count: usize, sample_count: usize, seq_no: usize) -> usize {
    assert!(member_count > 0);
    assert!(sample_count > 0);
    if sample_count >= member_count {
        return seq_no % member_count;
    }
    let sample_no = seq_no % sample_count;
    let one_based_index =
        ((sample_no + 1) as u128 * member_count as u128 / sample_count as u128) as usize;
    one_based_index.saturating_sub(1)
}

fn deterministic_payload(
    len: usize,
    plateau_size: usize,
    payload_size: usize,
    seq_no: usize,
    actor_id: &str,
) -> String {
    if len == 0 {
        return String::new();
    }
    let seed = format!(
        "plateau={};payload={};seq={};actor={};",
        plateau_size, payload_size, seq_no, actor_id
    );
    let mut out = String::with_capacity(len);
    while out.len() < len {
        out.push_str(&seed);
    }
    out.truncate(len);
    out
}

fn app_sends_per_plateau(size: usize, app_rounds: usize, max_app_samples: usize) -> usize {
    if size < 2 {
        0
    } else {
        cap_count(app_rounds.saturating_mul(size), max_app_samples)
    }
}

fn app_ops_for_plateau(
    size: usize,
    app_rounds: usize,
    max_app_samples: usize,
    payload_count: usize,
) -> usize {
    app_sends_per_plateau(size, app_rounds, max_app_samples).saturating_mul(payload_count)
}

fn estimate_total_units(
    plateau_sequence: &[usize],
    app_rounds: usize,
    max_app_samples: usize,
    payload_count: usize,
) -> usize {
    let mut total = 0usize;
    let mut current = 0usize;
    for &target in plateau_sequence {
        total = total.saturating_add(current.abs_diff(target));
        total = total.saturating_add(app_ops_for_plateau(
            target,
            app_rounds,
            max_app_samples,
            payload_count,
        ));
        current = target;
    }
    total
}

fn cap_count(raw: usize, cap: usize) -> usize {
    if cap == 0 {
        0
    } else {
        raw.min(cap)
    }
}

fn building_plateau_sequence(
    min_size: usize,
    max_size: usize,
    step_size: usize,
    _roundtrips: usize,
    piecewise: Option<(usize, usize)>,
) -> Vec<usize> {
    building_plateau_sequence_with_piecewise(min_size, max_size, step_size, piecewise)
}

fn building_plateau_sequence_with_piecewise(
    min_size: usize,
    max_size: usize,
    step_size: usize,
    piecewise: Option<(usize, usize)>,
) -> Vec<usize> {
    let mut sizes = Vec::new();
    let mut current = min_size;
    sizes.push(current);
    while current < max_size {
        let next = if let Some((switch_at, after_switch)) = piecewise {
            if current < switch_at {
                current
                    .saturating_add(step_size)
                    .min(switch_at)
                    .min(max_size)
            } else {
                current.saturating_add(after_switch).min(max_size)
            }
        } else {
            current.saturating_add(step_size).min(max_size)
        };
        if sizes.last().copied() != Some(next) {
            sizes.push(next);
        }
        current = next;
    }
    sizes
}

pub fn build_plateau_sequence(
    min_size: usize,
    max_size: usize,
    step_size: usize,
    roundtrips: usize,
) -> Vec<usize> {
    let ascent = building_plateau_sequence(min_size, max_size, step_size, roundtrips, None);
    let mut sequence = Vec::new();
    for _ in 0..roundtrips {
        for &size in &ascent {
            if sequence.last().copied() != Some(size) {
                sequence.push(size);
            }
        }
        for &size in ascent.iter().rev().skip(1) {
            if sequence.last().copied() != Some(size) {
                sequence.push(size);
            }
        }
    }
    sequence
}

fn build_plateau_sequence_for_step_size<R: Rng + ?Sized>(
    min_size: usize,
    max_size: usize,
    step_size: &StepSize,
    roundtrips: usize,
    piecewise: Option<(usize, usize)>,
    rng: &mut R,
) -> Vec<usize> {
    if let StepSize::Fixed(step_size) = step_size {
        let ascent =
            building_plateau_sequence(min_size, max_size, *step_size, roundtrips, piecewise);
        let mut sequence = Vec::new();
        for _ in 0..roundtrips {
            for &size in &ascent {
                if sequence.last().copied() != Some(size) {
                    sequence.push(size);
                }
            }
            for &size in ascent.iter().rev().skip(1) {
                if sequence.last().copied() != Some(size) {
                    sequence.push(size);
                }
            }
        }
        return sequence;
    }

    let mut sequence = Vec::new();
    let mut current = min_size;
    for _ in 0..roundtrips {
        if sequence.last().copied() != Some(current) {
            sequence.push(current);
        }
        while current < max_size {
            current = current.saturating_add(step_size.sample(rng)).min(max_size);
            if sequence.last().copied() != Some(current) {
                sequence.push(current);
            }
        }
        while current > min_size {
            current = current.saturating_sub(step_size.sample(rng)).max(min_size);
            if sequence.last().copied() != Some(current) {
                sequence.push(current);
            }
        }
    }

    sequence
}

fn build_ascending_plateau_sequence_for_step_size<R: Rng + ?Sized>(
    min_size: usize,
    max_size: usize,
    step_size: &StepSize,
    piecewise: Option<(usize, usize)>,
    rng: &mut R,
) -> Vec<usize> {
    if let StepSize::Fixed(step_size) = step_size {
        return building_plateau_sequence(min_size, max_size, *step_size, 0, piecewise);
    }

    let mut sequence = Vec::new();
    let mut current = min_size;
    if sequence.last().copied() != Some(current) {
        sequence.push(current);
    }
    while current < max_size {
        current = current.saturating_add(step_size.sample(rng)).min(max_size);
        if sequence.last().copied() != Some(current) {
            sequence.push(current);
        }
    }
    sequence
}

fn build_plateau_sequence_for_order<R: Rng + ?Sized>(
    min_size: usize,
    max_size: usize,
    step_size: &StepSize,
    roundtrips: usize,
    plateau_order: PlateauOrder,
    piecewise: Option<(usize, usize)>,
    rng: &mut R,
) -> Vec<usize> {
    if plateau_order == PlateauOrder::Staircase {
        return build_plateau_sequence_for_step_size(
            min_size, max_size, step_size, roundtrips, piecewise, rng,
        );
    }
    if plateau_order == PlateauOrder::Ascending {
        return build_ascending_plateau_sequence_for_step_size(
            min_size, max_size, step_size, piecewise, rng,
        );
    }

    // Randomized
    let mut candidates = vec![min_size];
    let mut current = min_size;
    while current < max_size {
        current = current.saturating_add(step_size.sample(rng)).min(max_size);
        if candidates.last().copied() != Some(current) {
            candidates.push(current);
        }
    }

    let mut sequence = Vec::new();
    for _ in 0..roundtrips {
        let mut randomized = candidates.clone();
        randomized.shuffle(rng);
        if randomized.len() > 1 && sequence.last() == randomized.first() {
            randomized.rotate_left(1);
        }
        for size in randomized {
            if sequence.last().copied() != Some(size) {
                sequence.push(size);
            }
        }
    }
    sequence
}

fn validate_config(config: &StaircaseConfig, worker_count: usize) -> Result<usize> {
    if config.min_size == 0 {
        return Err(anyhow!("--min-size must be at least 1"));
    }
    if !config.step_size.is_valid() {
        return Err(anyhow!("--step-size must be at least 1"));
    }
    if config.roundtrips == 0 {
        return Err(anyhow!("--roundtrips must be at least 1"));
    }
    if config.payload_sizes.is_empty() {
        return Err(anyhow!("At least one payload size is required"));
    }

    let max_size = config.max_size.unwrap_or(worker_count);
    if max_size == 0 {
        return Err(anyhow!("--max-size must be at least 1"));
    }
    if max_size > worker_count {
        return Err(anyhow!(
            "--max-size {} exceeds number of supplied workers {}",
            max_size,
            worker_count
        ));
    }
    if config.min_size > max_size {
        return Err(anyhow!(
            "--min-size {} cannot exceed --max-size {}",
            config.min_size,
            max_size
        ));
    }
    Ok(max_size)
}

#[cfg(test)]
mod random_input_tests {
    use std::collections::HashSet;

    use rand::{rngs::StdRng, SeedableRng};

    use super::{
        build_plateau_sequence, build_plateau_sequence_for_step_size,
        building_plateau_sequence_with_piecewise, PayloadSizeSource, PayloadSizes, StepSize,
    };

    #[test]
    fn fixed_and_range_flag_values_parse() {
        assert_eq!("8".parse::<StepSize>(), Ok(StepSize::Fixed(8)));
        assert_eq!(
            "[2,16]".parse::<StepSize>(),
            Ok(StepSize::UniformRange { min: 2, max: 16 })
        );
        assert_eq!(
            "32,256".parse::<PayloadSizes>(),
            Ok(PayloadSizes::Fixed(vec![32, 256]))
        );
        assert_eq!(
            "[32,4096]".parse::<PayloadSizes>(),
            Ok(PayloadSizes::UniformRange { min: 32, max: 4096 })
        );
        assert!("[0,16]".parse::<StepSize>().is_err());
        assert!("[4096,32]".parse::<PayloadSizes>().is_err());
    }

    #[test]
    fn range_steps_and_payloads_sample_independently() {
        let mut rng = StdRng::seed_from_u64(7);
        let step_size = StepSize::UniformRange { min: 2, max: 16 };
        let step_samples = (0..32)
            .map(|_| step_size.sample(&mut rng))
            .collect::<Vec<_>>();
        assert!(step_samples.iter().all(|sample| (2..=16).contains(sample)));
        assert!(step_samples.iter().collect::<HashSet<_>>().len() > 1);

        let fixed_sequence =
            build_plateau_sequence_for_step_size(2, 32, &StepSize::Fixed(8), 1, None, &mut rng);
        assert_eq!(fixed_sequence, build_plateau_sequence(2, 32, 8, 1));

        let sequence = build_plateau_sequence_for_step_size(2, 32, &step_size, 2, None, &mut rng);
        assert_eq!(sequence.first(), Some(&2));
        assert_eq!(sequence.last(), Some(&2));
        assert!(sequence.contains(&32));
        assert!(sequence.windows(2).all(|pair| {
            let delta = pair[0].abs_diff(pair[1]);
            delta > 0 && delta <= 16
        }));

        let payload_source = PayloadSizeSource::UniformRange { min: 32, max: 4096 };
        let payload_samples = (0..32)
            .map(|_| payload_source.sample(&mut rng))
            .collect::<Vec<_>>();
        assert!(payload_samples
            .iter()
            .all(|sample| (32..=4096).contains(sample)));
        assert!(payload_samples.iter().collect::<HashSet<_>>().len() > 1);
    }

    #[test]
    fn piecewise_fixed_step_snaps_to_switch_point() {
        assert_eq!(
            building_plateau_sequence_with_piecewise(2, 1024, 64, Some((256, 128))),
            vec![2, 66, 130, 194, 256, 384, 512, 640, 768, 896, 1024]
        );
        assert_eq!(
            building_plateau_sequence_with_piecewise(2, 1024, 64, Some((256, 196))),
            vec![2, 66, 130, 194, 256, 452, 648, 844, 1024]
        );
        assert_eq!(
            building_plateau_sequence_with_piecewise(2, 194, 64, Some((256, 128))),
            vec![2, 66, 130, 194]
        );
    }
}

// ── Fanout infrastructure ─────────────────────────────────────────────────────

fn latency_percentiles(
    mut latencies: Vec<u128>,
) -> (Option<u128>, Option<u128>, Option<u128>, Option<u128>) {
    if latencies.is_empty() {
        return (None, None, None, None);
    }
    latencies.sort_unstable();
    let max = latencies.last().copied();
    let percentile = |pct: usize| -> Option<u128> {
        let len = latencies.len();
        let idx = ((len.saturating_sub(1)) * pct).div_ceil(100);
        latencies.get(idx).copied()
    };
    (percentile(50), percentile(95), percentile(99), max)
}

pub fn build_batch_commands<F>(
    workers: &[WorkerSpec],
    mut command_for: F,
) -> Vec<(String, Vec<BatchFanoutCommand>)>
where
    F: FnMut(&WorkerSpec) -> BatchFanoutCommand,
{
    let groups = physical_groups(workers.iter());
    let mut result = Vec::new();
    for (physical_id, group) in groups {
        let cmds: Vec<BatchFanoutCommand> = group
            .iter()
            .map(|w| {
                let mut command = command_for(*w);
                if command.request_id.is_none() {
                    command.request_id = Some(batch_request_id(*w, &command));
                }
                command
            })
            .collect();
        result.push((physical_id, cmds));
    }
    result
}

fn batch_request_id(worker: &WorkerSpec, command: &BatchFanoutCommand) -> String {
    WorkerCommandContext::with_metadata(worker, &command.command, command.phase.as_deref())
        .request_id
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchFanoutCommand {
    pub participant_id: String,
    #[serde(default)]
    pub request_id: Option<String>,
    pub command: Command,
    pub phase: Option<String>,
    pub profile: Option<bool>,
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

#[derive(Debug)]
struct FanoutFailure {
    worker: WorkerSpec,
    error: anyhow::Error,
}

#[derive(Debug)]
struct BatchFanoutError {
    phase: String,
    operation: String,
    failures: Vec<FanoutFailure>,
}

impl std::fmt::Display for BatchFanoutError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let sample_errors = self
            .failures
            .iter()
            .take(5)
            .map(|failure| format!("{}: {}", failure.worker.id, failure.error))
            .collect::<Vec<_>>()
            .join("; ");
        write!(
            formatter,
            "batch_fanout phase={} operation={} failed_workers={} sample_errors=[{}]",
            self.phase,
            self.operation,
            self.failures.len(),
            sample_errors
        )
    }
}

impl StdError for BatchFanoutError {}

async fn batch_fanout_workers(
    http: &reqwest::Client,
    phase: &str,
    conversation_size: usize,
    operation: &str,
    workers: &[WorkerSpec],
    fanout: &mut FanoutController,
    commands_by_physical: &[(String, Vec<BatchFanoutCommand>)],
) -> Result<()> {
    let max_parallelism = fanout.parallelism();
    let started = Instant::now();

    let mut all_successes = Vec::new();
    let mut all_failures = Vec::new();
    let mut all_latencies = Vec::new();
    let mut request_count = 0usize;
    let mut retry_pass_count = 0usize;

    let mut retry_commands: Vec<(String, Vec<BatchFanoutCommand>)> =
        commands_by_physical.iter().cloned().collect();
    for retry_pass in 0..=DEFAULT_FANOUT_RETRY_PASSES {
        if retry_commands.is_empty() {
            break;
        }

        let current_commands = std::mem::take(&mut retry_commands);
        if retry_pass > 0 {
            eprintln!(
                "[batch-fanout-retry] phase={} operation={} pass={} retry_physical_workers={}",
                phase,
                operation,
                retry_pass,
                current_commands.len()
            );
            retry_pass_count += 1;
        }

        let workers_by_id: HashMap<String, WorkerSpec> =
            workers.iter().map(|w| (w.id.clone(), w.clone())).collect();
        let in_flight = Arc::new(AtomicUsize::new(0));
        let max_in_flight = Arc::new(AtomicUsize::new(0));

        let mut attempts = stream::iter(current_commands.iter().cloned())
            .map(|(physical_id, cmds)| {
                let http = http.clone();
                let workers_by_id = Arc::new(workers_by_id.clone());
                let in_flight = Arc::clone(&in_flight);
                let max_in_flight = Arc::clone(&max_in_flight);
                async move {
                    let current = in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                    update_atomic_max(&max_in_flight, current);
                    let attempt =
                        batch_physical_request(&http, &workers_by_id, physical_id, cmds).await;
                    in_flight.fetch_sub(1, Ordering::SeqCst);
                    attempt
                }
            })
            .buffer_unordered(max_parallelism.max(1));

        let mut pass_successes = Vec::new();
        let mut pass_failures = Vec::new();
        let mut pass_latencies = Vec::new();
        let mut pass_request_count = 0usize;

        while let Some(attempt) = attempts.next().await {
            pass_request_count += attempt.request_count;
            pass_latencies.push((attempt.physical_id, attempt.latency_ms));
            pass_successes.extend(attempt.successes);
            pass_failures.extend(attempt.failures);
        }

        request_count += pass_request_count;
        all_latencies.extend(pass_latencies);
        all_successes.extend(pass_successes);
        all_failures = pass_failures;
        retry_commands = retry_batch_commands_for_failures(commands_by_physical, &all_failures);
    }

    let latency_values: Vec<u128> = all_latencies.iter().map(|(_, lat)| *lat).collect();
    let (p50, p95, p99, max_lat) = latency_percentiles(latency_values);
    let mut sorted_lat = all_latencies.clone();
    sorted_lat.sort_by(|a, b| b.1.cmp(&a.1));
    let slowest_worker_ids: Vec<String> = sorted_lat
        .iter()
        .take(5)
        .map(|(id, lat)| format!("{}:{}ms", id, lat))
        .collect();

    let summary = FanoutSummary {
        request_count,
        recipient_count: workers.len(),
        success_count: all_successes.len(),
        failure_count: all_failures.len(),
        timeout_count: 0,
        connect_error_count: 0,
        max_parallelism,
        effective_parallelism: max_parallelism,
        retry_pass_count,
        wall_ms: started.elapsed().as_millis(),
        latency_p50_ms: p50,
        latency_p95_ms: p95,
        latency_p99_ms: p99,
        latency_max_ms: max_lat,
        slowest_worker_ids,
    };

    emit_fanout_metrics(phase, conversation_size, operation, &summary);
    fanout.record(phase, operation, &summary);

    if !all_failures.is_empty() {
        return Err(BatchFanoutError {
            phase: phase.to_string(),
            operation: operation.to_string(),
            failures: all_failures,
        }
        .into());
    }
    Ok(())
}

struct BatchPhysicalAttempt {
    physical_id: String,
    successes: Vec<(WorkerSpec, ())>,
    failures: Vec<FanoutFailure>,
    latency_ms: u128,
    request_count: usize,
}

async fn batch_physical_request(
    http: &reqwest::Client,
    workers_by_id: &HashMap<String, WorkerSpec>,
    physical_id: String,
    cmds: Vec<BatchFanoutCommand>,
) -> BatchPhysicalAttempt {
    let physical_url = batch_physical_base_url(&physical_id, &cmds, workers_by_id);
    let batch_url = format!("{}/batch-command", physical_url.trim_end_matches('/'));

    let batch_items: Vec<BatchCommandItem> = cmds
        .iter()
        .map(|c| BatchCommandItem {
            participant_id: c.participant_id.clone(),
            request_id: c.request_id.clone(),
            command: c.command.clone(),
            phase: c.phase.clone(),
            profile: c.profile,
            benchmark_plateau_index: c.benchmark_plateau_index,
            benchmark_target_size: c.benchmark_target_size,
            benchmark_active_size: c.benchmark_active_size,
            benchmark_phase: c.benchmark_phase.clone(),
            benchmark_operation: c.benchmark_operation.clone(),
            benchmark_operation_seq: c.benchmark_operation_seq,
            benchmark_payload_size: c.benchmark_payload_size,
            benchmark_workflow_id: c.benchmark_workflow_id,
            workflow_pair_index: c.workflow_pair_index,
            workflow_pair_count: c.workflow_pair_count,
        })
        .collect();

    let batch_req = BatchCommandRequest { items: batch_items };
    let attempt_start = Instant::now();
    let result = http.post(&batch_url).json(&batch_req).send().await;
    let latency_ms = attempt_start.elapsed().as_millis();

    let mut failures = Vec::new();
    let mut successes = Vec::new();

    match result {
        Ok(response) => {
            if response.status().is_success() {
                if let Ok(batch_resp) = response.json::<BatchCommandResponse>().await {
                    for item_result in &batch_resp.items {
                        if item_result.response.status == "ok" {
                            if let Some(w) = workers_by_id.get(&item_result.participant_id) {
                                successes.push((w.clone(), ()));
                            }
                        } else if let Some(w) = workers_by_id.get(&item_result.participant_id) {
                            failures.push(FanoutFailure {
                                worker: w.clone(),
                                error: anyhow!(
                                    "participant {} batch error: {}",
                                    item_result.participant_id,
                                    item_result.response.message
                                ),
                            });
                        }
                    }
                } else {
                    for c in &cmds {
                        if let Some(w) = workers_by_id.get(&c.participant_id) {
                            failures.push(FanoutFailure {
                                worker: w.clone(),
                                error: anyhow!("batch response parse error"),
                            });
                        }
                    }
                }
            } else {
                let status = response.status();
                let body = response.text().await.unwrap_or_default();
                for c in &cmds {
                    if let Some(w) = workers_by_id.get(&c.participant_id) {
                        failures.push(FanoutFailure {
                            worker: w.clone(),
                            error: anyhow!("batch HTTP error: {} {}", status, body),
                        });
                    }
                }
            }
        }
        Err(err) => {
            for c in &cmds {
                if let Some(w) = workers_by_id.get(&c.participant_id) {
                    failures.push(FanoutFailure {
                        worker: w.clone(),
                        error: anyhow!("batch request error: {}", err),
                    });
                }
            }
        }
    }

    BatchPhysicalAttempt {
        physical_id,
        successes,
        failures,
        latency_ms,
        request_count: 1,
    }
}

fn batch_physical_base_url(
    physical_id: &str,
    cmds: &[BatchFanoutCommand],
    workers_by_id: &HashMap<String, WorkerSpec>,
) -> String {
    if let Some(worker) = cmds
        .iter()
        .find_map(|cmd| workers_by_id.get(&cmd.participant_id))
    {
        if let Some((base, _)) = worker.url.split_once("/participant/") {
            return base.trim_end_matches('/').to_string();
        }
        return worker.url.trim_end_matches('/').to_string();
    }
    format!("http://{}:8080", physical_id)
}

fn retry_batch_commands_for_failures(
    commands_by_physical: &[(String, Vec<BatchFanoutCommand>)],
    failures: &[FanoutFailure],
) -> Vec<(String, Vec<BatchFanoutCommand>)> {
    let failed_ids: std::collections::HashSet<&str> =
        failures.iter().map(|f| f.worker.id.as_str()).collect();
    commands_by_physical
        .iter()
        .filter_map(|(physical_id, cmds)| {
            let retry_cmds: Vec<BatchFanoutCommand> = cmds
                .iter()
                .filter(|cmd| failed_ids.contains(cmd.participant_id.as_str()))
                .cloned()
                .collect();
            if retry_cmds.is_empty() {
                None
            } else {
                Some((physical_id.clone(), retry_cmds))
            }
        })
        .collect()
}

fn update_atomic_max(max: &AtomicUsize, value: usize) {
    let mut current = max.load(Ordering::SeqCst);
    while value > current {
        match max.compare_exchange(current, value, Ordering::SeqCst, Ordering::SeqCst) {
            Ok(_) => break,
            Err(observed) => current = observed,
        }
    }
}

fn emit_fanout_metrics(
    phase: &str,
    conversation_size: usize,
    operation: &str,
    summary: &FanoutSummary,
) {
    emit_network_metrics(NetworkPhaseMetrics {
        phase: phase.to_string(),
        conversation_size,
        operation: operation.to_string(),
        request_count: summary.request_count,
        recipient_count: summary.recipient_count,
        success_count: summary.success_count,
        failure_count: summary.failure_count,
        timeout_count: summary.timeout_count,
        connect_error_count: summary.connect_error_count,
        max_parallelism: summary.max_parallelism,
        effective_parallelism: summary.effective_parallelism,
        wall_ms: summary.wall_ms,
        retry_count: 0,
        retry_sleep_ms: 0,
        retry_pass_count: summary.retry_pass_count,
        failures: summary.failure_count,
        worker_latency_p50_ms: summary.latency_p50_ms,
        worker_latency_p95_ms: summary.latency_p95_ms,
        worker_latency_p99_ms: summary.latency_p99_ms,
        worker_latency_max_ms: summary.latency_max_ms,
        slowest_worker_ids: summary.slowest_worker_ids.clone(),
        logical_request_count: summary.recipient_count,
        physical_request_count: summary.request_count,
        singleton_request_count: 0,
        packed_request_count: 0,
        packed_logical_client_count: 0,
        profile_enabled_recipient_count: 0,
    });
}

fn emit_network_metrics(metrics: NetworkPhaseMetrics) {
    match serde_json::to_string(&metrics) {
        Ok(json) => eprintln!("[network-metrics] {}", json),
        Err(err) => eprintln!("[network-metrics] serialization_error={}", err),
    }
}

// ── CSV aggregation ───────────────────────────────────────────────────────────

pub fn aggregate_csv(
    run_dir: &Path,
    worker_ids: &[String],
    provided_layout: &Option<WorkerLayout>,
) -> Result<()> {
    materialize_runner_profile_events(run_dir)?;

    let csv_path = run_dir.join("events.csv");
    let mut wtr = csv::Writer::from_path(&csv_path)?;

    let profile_enabled_ids: std::collections::HashSet<&str> = if let Some(ref l) = provided_layout
    {
        l.clients
            .iter()
            .filter(|c| c.profile_enabled)
            .map(|c| c.client_id.as_str())
            .collect()
    } else {
        worker_ids.iter().map(|s| s.as_str()).collect()
    };

    let logical_worker_count = provided_layout
        .as_ref()
        .map(|l| l.logical_worker_count)
        .unwrap_or(worker_ids.len());
    let physical_worker_count = provided_layout
        .as_ref()
        .map(|l| l.physical_worker_count)
        .unwrap_or(worker_ids.len());
    let singleton_count = provided_layout
        .as_ref()
        .map(|l| l.clients.iter().filter(|c| c.profile_enabled).count())
        .unwrap_or(worker_ids.len());
    let packed_clients_per_container = provided_layout
        .as_ref()
        .map(|l| l.packed_clients_per_container)
        .unwrap_or(1);
    let layout_mode = provided_layout
        .as_ref()
        .map(|l| l.layout_mode.as_str())
        .unwrap_or("one-container-per-client");

    let mut client_meta: std::collections::HashMap<&str, &WorkerLayoutClient> =
        std::collections::HashMap::new();
    let mut physical_meta: std::collections::HashMap<&str, &WorkerLayoutPhysicalWorker> =
        std::collections::HashMap::new();
    if let Some(ref l) = provided_layout {
        for c in &l.clients {
            client_meta.insert(c.client_id.as_str(), c);
        }
        for p in &l.physical_workers {
            physical_meta.insert(p.physical_worker_id.as_str(), p);
        }
    }

    fn non_empty_or<'a>(value: Option<&'a str>, default: &'a str) -> &'a str {
        match value {
            Some(value) if !value.is_empty() => value,
            _ => default,
        }
    }

    for worker_id in worker_ids {
        let path = run_dir.join(format!("participant-{worker_id}.jsonl"));
        if !profile_enabled_ids.contains(worker_id.as_str()) {
            if path.exists() {
                let _ = std::fs::remove_file(&path);
            }
            continue;
        }
        if !path.exists() {
            eprintln!(
                "[csv] WARNING: profile_enabled participant {} JSONL not found: {}",
                worker_id,
                path.display()
            );
            continue;
        }

        let meta = client_meta.get(worker_id.as_str()).copied();
        let file = File::open(&path)?;
        let reader = BufReader::new(file);

        for line in reader.lines() {
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }

            let event: SignalProfileEvent = serde_json::from_str(&line)
                .with_context(|| format!("Invalid json in {}", path.display()))?;
            let physical_worker_id =
                non_empty_or(meta.map(|m| m.physical_worker_id.as_str()), worker_id);
            let phys = physical_meta.get(physical_worker_id).copied();

            #[derive(Serialize)]
            struct CsvRow<'a> {
                participant_id: String,
                physical_worker_id: &'a str,
                container_mode: &'a str,
                execution_backend: &'a str,
                device_kind: &'a str,
                transport: &'a str,
                access_backend: &'a str,
                arch: &'a str,
                rust_target: &'a str,
                profile_schema_version: u32,
                ts_unix_ns: u128,
                op: String,
                span_name: String,
                span_layer: String,
                protocol_stack: String,
                implementation: String,
                measurement_class: String,
                event_family: String,
                event_subtype: String,
                success: bool,
                error_class: Option<String>,
                runner_event_kind: Option<String>,
                failed_worker_id: Option<String>,
                failed_physical_worker_id: Option<String>,
                failure_detail: Option<String>,
                failure_evidence_source: Option<String>,
                failure_evidence_detail: Option<String>,
                failure_action: Option<String>,
                reassigned_to_worker_id: Option<String>,
                benchmark_plateau_index: Option<usize>,
                benchmark_target_size: Option<usize>,
                benchmark_active_size: Option<usize>,
                benchmark_phase: Option<String>,
                benchmark_operation: Option<String>,
                benchmark_operation_seq: Option<usize>,
                benchmark_payload_size: Option<usize>,
                benchmark_workflow_id: Option<u64>,
                workflow_pair_index: Option<u32>,
                workflow_pair_count: Option<u32>,
                new_session_established: Option<bool>,
                span_id: Option<u64>,
                parent_span_id: Option<u64>,
                parent_operation: Option<String>,
                span_kind: Option<String>,
                measurement_plane: Option<String>,
                span_inclusive: Option<bool>,
                global_span_id: Option<String>,
                parent_global_span_id: Option<String>,
                profiling_worker_id: Option<String>,
                request_id: Option<String>,
                participant_device_id: Option<u32>,
                role: Option<String>,
                peer_id: Option<String>,
                peer_device_id: Option<u32>,
                pair_id: Option<String>,
                peer_count: Option<usize>,
                event_side: Option<String>,
                direction: Option<String>,
                phase: Option<String>,
                wall_ns: u128,
                cpu_thread_ns: Option<u128>,
                cpu_process_ns: Option<u128>,
                cpu_envelope_utilization: Option<f64>,
                cpu_throttled_time_ratio: Option<f64>,
                alloc_bytes: Option<u64>,
                alloc_count: Option<u64>,
                alloc_measurement_scope: Option<String>,
                l1d_cache_accesses: Option<u64>,
                l1d_cache_misses: Option<u64>,
                ram_rss_delta_bytes: Option<i64>,
                ram_rss_utilization: Option<f64>,
                artifact_size_bytes: Option<usize>,
                participant_count: Option<usize>,
                conversation_size: Option<usize>,
                prekey_bundle_count: Option<usize>,
                prekey_stock_before: Option<usize>,
                prekey_stock_after: Option<usize>,
                prekey_refill_count: Option<usize>,
                prekey_refill_trigger: Option<String>,
                session_count: Option<usize>,
                ratchet_step_count: Option<usize>,
                ciphertext_bytes: Option<usize>,
                plaintext_bytes: Option<usize>,
                handshake_protocol: Option<String>,
                handshake_side: Option<String>,
                classical_one_time_prekey_present: Option<bool>,
                classical_one_time_prekey_id: Option<u32>,
                signed_prekey_id: Option<u32>,
                pq_prekey_id: Option<u32>,
                pq_prekey_type: Option<String>,
                pq_prekey_signature_present: Option<bool>,
                ciphertext_message_type: Option<String>,
                message_counter: Option<u32>,
                previous_counter: Option<u32>,
                sender_ratchet_key_fingerprint: Option<String>,
                receiver_chain_matched: Option<bool>,
                dh_ratchet_performed: Option<bool>,
                root_chain_updated: Option<bool>,
                send_chain_index_before: Option<u32>,
                send_chain_index_after: Option<u32>,
                receive_chain_index_before: Option<u32>,
                receive_chain_index_after: Option<u32>,
                skipped_message_keys_used: Option<u32>,
                skipped_message_keys_stored: Option<u32>,
                spqr_step_performed: Option<bool>,
                ratchet_progression_kind: Option<String>,
                ratchet_progression_value: Option<u64>,
                pid: u32,
                thread_id: String,
                run_id: Option<String>,
                scenario: Option<String>,
                scenario_seed: Option<u64>,
                node_name: Option<String>,
                pod_name: Option<String>,
                logical_worker_count: usize,
                physical_worker_count: usize,
                singleton_count: usize,
                packed_clients_per_container: usize,
                layout_mode: &'a str,
                resource_limit_cpus: Option<f64>,
                resource_limit_memory: Option<&'a str>,
                resource_limit_memory_bytes: Option<u64>,
                resource_limit_memory_swap: Option<&'a str>,
                resource_limit_memory_swap_bytes: Option<u64>,
                resource_limit_pids: Option<u64>,
                resource_profile: &'a str,
                cpu_model: Option<String>,
                requested_cpu_fraction: Option<f64>,
                applied_cpu_fraction: Option<f64>,
                cpu_period_us: Option<u64>,
                cpu_quota_us: Option<u64>,
                cgroup_cpu_max: Option<String>,
                cpuset_cpus_requested: Option<String>,
                cpuset_cpus_effective: Option<String>,
                cpu_nr_periods: Option<u64>,
                cpu_nr_throttled: Option<u64>,
                cpu_throttled_usec: Option<u64>,
                memory_model: Option<String>,
                requested_memory_limit: Option<String>,
                requested_memory_limit_bytes: Option<u64>,
                applied_memory_limit_bytes: Option<u64>,
                memory_current_bytes: Option<u64>,
                memory_peak_bytes: Option<u64>,
                memory_events_max: Option<u64>,
                memory_events_oom: Option<u64>,
                memory_events_oom_kill: Option<u64>,
                app_heap_budget: Option<String>,
                app_heap_budget_bytes: Option<u64>,
                heap_current_live_bytes: Option<u64>,
                heap_peak_live_bytes: Option<u64>,
                heap_operation_peak_live_bytes: Option<u64>,
                heap_total_allocated_bytes: Option<u64>,
                heap_allocation_count: Option<u64>,
                heap_deallocation_count: Option<u64>,
                heap_failed_allocation_size_bytes: Option<u64>,
                heap_failure_context: Option<String>,
                resource_profile_id: Option<String>,
                resource_profile_index: Option<i32>,
                failure_class: Option<String>,
                failure_operation: Option<String>,
                failure_span: Option<String>,
                failure_phase: Option<String>,
                container_exit_code: Option<i32>,
                oom_killed: Option<bool>,
            }

            let row = CsvRow {
                participant_id: event
                    .participant_id
                    .clone()
                    .unwrap_or_else(|| worker_id.to_string()),
                physical_worker_id,
                container_mode: non_empty_or(meta.map(|m| m.container_mode.as_str()), "singleton"),
                execution_backend: non_empty_or(
                    meta.map(|m| m.execution_backend.as_str()),
                    "docker_container",
                ),
                device_kind: non_empty_or(
                    meta.map(|m| m.device_kind.as_str()),
                    "scratch_container",
                ),
                transport: non_empty_or(meta.map(|m| m.transport.as_str()), "docker_bridge"),
                access_backend: non_empty_or(meta.map(|m| m.access_backend.as_str()), "docker"),
                arch: non_empty_or(meta.map(|m| m.arch.as_str()), "x86_64"),
                rust_target: non_empty_or(
                    meta.map(|m| m.rust_target.as_str()),
                    "x86_64-unknown-linux-musl",
                ),
                profile_schema_version: event.profile_schema_version,
                ts_unix_ns: event.ts_unix_ns,
                op: event.op.clone(),
                span_name: event.op,
                span_layer: event.span_layer,
                protocol_stack: event.protocol_stack,
                implementation: event.implementation,
                measurement_class: event.measurement_class,
                event_family: event.event_family,
                event_subtype: event.event_subtype,
                success: event.success,
                error_class: event.error_class,
                runner_event_kind: event.runner_event_kind,
                failed_worker_id: event.failed_worker_id,
                failed_physical_worker_id: event.failed_physical_worker_id,
                failure_detail: event.failure_detail,
                failure_evidence_source: event.failure_evidence_source,
                failure_evidence_detail: event.failure_evidence_detail,
                failure_action: event.failure_action,
                reassigned_to_worker_id: event.reassigned_to_worker_id,
                benchmark_plateau_index: event.benchmark_plateau_index,
                benchmark_target_size: event.benchmark_target_size,
                benchmark_active_size: event.benchmark_active_size,
                benchmark_phase: event.benchmark_phase,
                benchmark_operation: event.benchmark_operation,
                benchmark_operation_seq: event.benchmark_operation_seq,
                benchmark_payload_size: event.benchmark_payload_size,
                benchmark_workflow_id: event.benchmark_workflow_id,
                workflow_pair_index: event.workflow_pair_index,
                workflow_pair_count: event.workflow_pair_count,
                new_session_established: event.new_session_established,
                span_id: event.span_id,
                parent_span_id: event.parent_span_id,
                parent_operation: event.parent_operation,
                span_kind: event.span_kind,
                measurement_plane: event.measurement_plane,
                span_inclusive: event.span_inclusive,
                global_span_id: event.global_span_id,
                parent_global_span_id: event.parent_global_span_id,
                profiling_worker_id: event.worker_id,
                request_id: event.request_id,
                participant_device_id: event.participant_device_id,
                role: event.role,
                peer_id: event.peer_id,
                peer_device_id: event.peer_device_id,
                pair_id: event.pair_id,
                peer_count: event.peer_count,
                event_side: event.event_side,
                direction: event.direction,
                phase: event.phase,
                wall_ns: event.wall_ns,
                cpu_thread_ns: event.cpu_thread_ns,
                cpu_process_ns: event.cpu_process_ns,
                cpu_envelope_utilization: event.cpu_envelope_utilization,
                cpu_throttled_time_ratio: event.cpu_throttled_time_ratio,
                alloc_bytes: event.alloc_bytes,
                alloc_count: event.alloc_count,
                alloc_measurement_scope: event.alloc_measurement_scope.clone(),
                l1d_cache_accesses: event.l1d_cache_accesses,
                l1d_cache_misses: event.l1d_cache_misses,
                ram_rss_delta_bytes: event.ram_rss_delta_bytes,
                ram_rss_utilization: event.ram_rss_utilization,
                artifact_size_bytes: event.artifact_size_bytes,
                participant_count: event.participant_count,
                conversation_size: event.conversation_size,
                prekey_bundle_count: event.prekey_bundle_count,
                prekey_stock_before: event.prekey_stock_before,
                prekey_stock_after: event.prekey_stock_after,
                prekey_refill_count: event.prekey_refill_count,
                prekey_refill_trigger: event.prekey_refill_trigger,
                session_count: event.session_count,
                ratchet_step_count: event.ratchet_step_count,
                ciphertext_bytes: event.ciphertext_bytes,
                plaintext_bytes: event.plaintext_bytes,
                handshake_protocol: event.handshake_protocol,
                handshake_side: event.handshake_side,
                classical_one_time_prekey_present: event.classical_one_time_prekey_present,
                classical_one_time_prekey_id: event.classical_one_time_prekey_id,
                signed_prekey_id: event.signed_prekey_id,
                pq_prekey_id: event.pq_prekey_id,
                pq_prekey_type: event.pq_prekey_type,
                pq_prekey_signature_present: event.pq_prekey_signature_present,
                ciphertext_message_type: event.ciphertext_message_type,
                message_counter: event.message_counter,
                previous_counter: event.previous_counter,
                sender_ratchet_key_fingerprint: event.sender_ratchet_key_fingerprint,
                receiver_chain_matched: event.receiver_chain_matched,
                dh_ratchet_performed: event.dh_ratchet_performed,
                root_chain_updated: event.root_chain_updated,
                send_chain_index_before: event.send_chain_index_before,
                send_chain_index_after: event.send_chain_index_after,
                receive_chain_index_before: event.receive_chain_index_before,
                receive_chain_index_after: event.receive_chain_index_after,
                skipped_message_keys_used: event.skipped_message_keys_used,
                skipped_message_keys_stored: event.skipped_message_keys_stored,
                spqr_step_performed: event.spqr_step_performed,
                ratchet_progression_kind: event.ratchet_progression_kind,
                ratchet_progression_value: event.ratchet_progression_value,
                pid: event.pid,
                thread_id: event.thread_id,
                run_id: event.run_id,
                scenario: event.scenario,
                scenario_seed: event.scenario_seed,
                node_name: event.node_name,
                pod_name: event.pod_name,
                logical_worker_count,
                physical_worker_count,
                singleton_count,
                packed_clients_per_container,
                layout_mode,
                resource_limit_cpus: phys.and_then(|m| m.resource_limit_cpus),
                resource_limit_memory: phys.and_then(|m| m.resource_limit_memory.as_deref()),
                resource_limit_memory_bytes: phys.and_then(|m| m.resource_limit_memory_bytes),
                resource_limit_memory_swap: phys
                    .and_then(|m| m.resource_limit_memory_swap.as_deref()),
                resource_limit_memory_swap_bytes: phys
                    .and_then(|m| m.resource_limit_memory_swap_bytes),
                resource_limit_pids: phys.and_then(|m| m.resource_limit_pids),
                resource_profile: non_empty_or(phys.map(|m| m.resource_profile.as_str()), ""),
                cpu_model: event.cpu_model,
                requested_cpu_fraction: event.requested_cpu_fraction,
                applied_cpu_fraction: event.applied_cpu_fraction,
                cpu_period_us: event.cpu_period_us,
                cpu_quota_us: event.cpu_quota_us,
                cgroup_cpu_max: event.cgroup_cpu_max,
                cpuset_cpus_requested: event.cpuset_cpus_requested,
                cpuset_cpus_effective: event.cpuset_cpus_effective,
                cpu_nr_periods: event.cpu_nr_periods,
                cpu_nr_throttled: event.cpu_nr_throttled,
                cpu_throttled_usec: event.cpu_throttled_usec,
                memory_model: event.memory_model,
                requested_memory_limit: event.requested_memory_limit,
                requested_memory_limit_bytes: event.requested_memory_limit_bytes,
                applied_memory_limit_bytes: event.applied_memory_limit_bytes,
                memory_current_bytes: event.memory_current_bytes,
                memory_peak_bytes: event.memory_peak_bytes,
                memory_events_max: event.memory_events_max,
                memory_events_oom: event.memory_events_oom,
                memory_events_oom_kill: event.memory_events_oom_kill,
                app_heap_budget: event.app_heap_budget,
                app_heap_budget_bytes: event.app_heap_budget_bytes,
                heap_current_live_bytes: event.heap_current_live_bytes,
                heap_peak_live_bytes: event.heap_peak_live_bytes,
                heap_operation_peak_live_bytes: event.heap_operation_peak_live_bytes,
                heap_total_allocated_bytes: event.heap_total_allocated_bytes,
                heap_allocation_count: event.heap_allocation_count,
                heap_deallocation_count: event.heap_deallocation_count,
                heap_failed_allocation_size_bytes: event.heap_failed_allocation_size_bytes,
                heap_failure_context: event.heap_failure_context.clone(),
                resource_profile_id: event.resource_profile_id,
                resource_profile_index: event.resource_profile_index,
                failure_class: event.failure_class,
                failure_operation: event.failure_operation,
                failure_span: event.failure_span,
                failure_phase: event.failure_phase,
                container_exit_code: event.container_exit_code,
                oom_killed: event.oom_killed,
            };

            wtr.serialize(row)?;
        }
    }

    wtr.flush()?;
    Ok(())
}

// ── Post-benchmark sidecars ────────────────────────────────────────────────

pub fn write_worker_failures_csv(run_dir: &Path, run_id: &str) -> Result<()> {
    let path = run_dir.join("worker_failures.csv");
    let mut wtr = csv::Writer::from_path(&path)?;
    wtr.write_record(&[
        "run_id",
        "worker_id",
        "physical_worker_id",
        "participant_id",
        "resource_profile_id",
        "failure_class",
        "failure_detail",
        "failure_operation",
        "failure_span",
        "failure_phase",
        "benchmark_target_size",
        "benchmark_active_size",
        "container_exit_code",
        "oom_killed",
        "action_taken",
    ])?;

    let events_path = run_dir.join("runner-events.jsonl");
    if events_path.exists() {
        let file = File::open(&events_path)?;
        for line in BufReader::new(file).lines() {
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }
            let event: RunnerEvent = serde_json::from_str(&line)?;
            let target_size = event.benchmark_target_size.to_string();
            let active_size = event.benchmark_active_size.to_string();
            let oom = event.failure_class == "oom_kill";
            wtr.write_record(&[
                run_id,
                &event.failed_worker_id,
                &event.failed_physical_worker_id,
                &event.failed_worker_id,
                "",
                &event.failure_class,
                &event.failure_detail,
                &event.benchmark_operation,
                "",
                &event.benchmark_phase,
                &target_size,
                &active_size,
                "",
                if oom { "true" } else { "false" },
                &event.failure_action,
            ])?;
        }
    }

    wtr.flush()?;
    Ok(())
}

pub fn write_run_status_csv(run_dir: &Path, run_id: &str, events_csv_exists: bool) -> Result<()> {
    let path = run_dir.join("run_status.csv");
    let mut wtr = csv::Writer::from_path(&path)?;
    wtr.write_record(&["run_id", "run_status", "events_csv_generated"])?;
    let status = if events_csv_exists {
        "completed"
    } else {
        "failed"
    };
    wtr.write_record(&[
        run_id,
        status,
        if events_csv_exists { "true" } else { "false" },
    ])?;
    wtr.flush()?;
    Ok(())
}

pub fn write_benchmark_outcome_json(
    run_dir: &Path,
    run_id: &str,
    events_csv_exists: bool,
    has_failures: bool,
) -> Result<()> {
    let path = run_dir.join("benchmark_outcome.json");
    let outcome = serde_json::json!({
        "run_id": run_id,
        "success": events_csv_exists && !has_failures,
        "events_csv_generated": events_csv_exists,
        "failures_detected": has_failures,
        "timestamp_ns": unix_time_ns(),
    });
    fs::write(&path, serde_json::to_string_pretty(&outcome)?)?;
    Ok(())
}

pub fn write_validation_report_json(
    run_dir: &Path,
    run_id: &str,
    events_csv_exists: bool,
    events_csv_non_empty: bool,
) -> Result<()> {
    let path = run_dir.join("validation_report.json");
    let csv_path = run_dir.join("events.csv");
    let mut has_protocol_core = false;
    if csv_path.exists() {
        if let Ok(mut reader) = csv::Reader::from_path(&csv_path) {
            let span_layer_idx = reader
                .headers()
                .ok()
                .and_then(|h| h.iter().position(|s| s == "span_layer"));
            let measurement_class_idx = reader
                .headers()
                .ok()
                .and_then(|h| h.iter().position(|s| s == "measurement_class"));
            for record in reader.records().flatten() {
                if let Some(idx) = span_layer_idx {
                    if record
                        .get(idx)
                        .map(|s| s == "protocol_core" || s == "libsignal_main")
                        .unwrap_or(false)
                    {
                        has_protocol_core = true;
                        break;
                    }
                }
                if let Some(idx) = measurement_class_idx {
                    if record.get(idx).map(|s| s == "protocol").unwrap_or(false) {
                        has_protocol_core = true;
                        break;
                    }
                }
            }
        }
    }
    let failures_path = run_dir.join("worker_failures.csv");
    let failures_csv_exists = failures_path.exists();
    let report = serde_json::json!({
        "run_id": run_id,
        "timestamp_ns": unix_time_ns(),
        "checks": {
            "events_csv_exists": events_csv_exists,
            "events_csv_non_empty": events_csv_non_empty,
            "events_csv_has_protocol_core_subspans": has_protocol_core,
            "worker_failures_csv_exists": failures_csv_exists,
            "run_status_csv_exists": run_dir.join("run_status.csv").exists(),
            "benchmark_outcome_json_exists": run_dir.join("benchmark_outcome.json").exists(),
        },
        "valid": events_csv_exists && events_csv_non_empty,
    });
    fs::write(&path, serde_json::to_string_pretty(&report)?)?;
    Ok(())
}

pub fn write_post_benchmark_sidecars(run_dir: &Path, run_id: &str) -> Result<()> {
    let events_csv_path = run_dir.join("events.csv");

    write_worker_failures_csv(run_dir, run_id)?;

    let events_csv_exists = events_csv_path.exists();
    let events_csv_non_empty = if events_csv_exists {
        fs::metadata(&events_csv_path)
            .map(|m| m.len() > 0)
            .unwrap_or(false)
    } else {
        false
    };

    // If no events.csv exists (failure before aggregation), write a minimal one
    if !events_csv_exists {
        let mut wtr = csv::Writer::from_path(&events_csv_path)?;
        wtr.write_record(&[
            "run_id",
            "scenario",
            "protocol_stack",
            "implementation",
            "profile_schema_version",
            "client_id",
            "participant_id",
            "worker_id",
            "physical_worker_id",
            "op",
            "span_name",
            "span_layer",
            "measurement_class",
            "event_family",
            "event_subtype",
            "success",
            "error_class",
            "failure_class",
            "failure_detail",
            "wall_ns",
            "cpu_thread_ns",
        ])?;
        wtr.flush()?;
    }

    let events_csv_exists = events_csv_path.exists();
    let events_csv_non_empty = if events_csv_exists {
        fs::metadata(&events_csv_path)
            .map(|m| m.len() > 0)
            .unwrap_or(false)
    } else {
        false
    };

    let runner_events_path = run_dir.join("runner-events.jsonl");
    let has_failures = runner_events_path.exists()
        && fs::metadata(&runner_events_path)
            .map(|m| m.len() > 0)
            .unwrap_or(false);

    write_run_status_csv(run_dir, run_id, events_csv_non_empty)?;
    write_benchmark_outcome_json(run_dir, run_id, events_csv_non_empty, has_failures)?;
    write_validation_report_json(run_dir, run_id, events_csv_non_empty, events_csv_non_empty)?;

    Ok(())
}

#[cfg(test)]
mod aggregate_csv_runner_event_tests {
    use super::*;

    #[test]
    fn aggregate_csv_materializes_runner_event_without_profile_append() {
        let unique = format!(
            "signal-runner-event-test-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system time")
                .as_nanos()
        );
        let run_dir = std::env::temp_dir().join(unique);
        std::fs::create_dir_all(&run_dir).expect("create run dir");

        let runner_event = serde_json::json!({
            "profile_schema_version": 3,
            "ts_unix_ns": 7u128,
            "event_kind": "worker_failure",
            "failed_worker_id": "00001",
            "failed_physical_worker_id": "worker-00001",
            "failure_class": "oom_kill",
            "failure_detail": "worker OOMed",
            "failure_evidence_source": "docker_state",
            "failure_evidence_detail": "State.OOMKilled",
            "failure_action": "drop_idle_participant",
            "reassigned_to_worker_id": null,
            "benchmark_plateau_index": 1,
            "benchmark_target_size": 2,
            "benchmark_active_size": 0,
            "benchmark_phase": "enrollment",
            "benchmark_operation": "prepare_participant",
            "benchmark_operation_seq": null,
            "benchmark_payload_size": null
        });
        std::fs::write(
            run_dir.join("runner-events.jsonl"),
            serde_json::to_string(&runner_event).expect("runner event json") + "\n",
        )
        .expect("write runner event");

        aggregate_csv(&run_dir, &["00001".to_string()], &None).expect("aggregate csv");

        let mut reader = csv::Reader::from_path(run_dir.join("events.csv")).expect("open csv");
        let headers = reader.headers().expect("headers").clone();
        let record = reader
            .records()
            .next()
            .expect("runner row")
            .expect("valid row");
        let value = |name: &str| {
            let idx = headers
                .iter()
                .position(|header| header == name)
                .expect("header exists");
            record.get(idx).expect("value").to_string()
        };

        assert_eq!(value("op"), "benchmark.worker_failure");
        assert_eq!(value("failed_worker_id"), "00001");
        assert_eq!(value("failure_evidence_source"), "docker_state");
        assert_eq!(value("benchmark_operation"), "prepare_participant");
        assert!(run_dir.join("participant-00001.jsonl").exists());

        let _ = std::fs::remove_dir_all(&run_dir);
    }
}

/// Validate a run ID string for safe filesystem usage.
pub fn validate_run_id(run_id: &str) -> Result<()> {
    if run_id.is_empty() {
        return Err(anyhow!("Run ID must not be empty"));
    }
    if run_id == "/" || run_id == "." || run_id == ".." {
        return Err(anyhow!("Run ID must not be '{}'", run_id));
    }
    if run_id.contains('/') {
        return Err(anyhow!("Run ID must not contain '/'"));
    }
    if !run_id
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '.' || c == '_' || c == '-')
    {
        return Err(anyhow!(
            "Run ID must only contain [A-Za-z0-9._-], got '{}'",
            run_id
        ));
    }
    Ok(())
}
