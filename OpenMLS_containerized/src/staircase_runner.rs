use std::{
    collections::{HashMap, HashSet, VecDeque},
    error::Error as StdError,
    fmt,
    fs::{self, File, OpenOptions},
    future::Future,
    io::{self, BufRead, BufReader, Write},
    path::{Path, PathBuf},
    str::FromStr,
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicU8, AtomicUsize, Ordering},
        Arc, Mutex, OnceLock,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::{anyhow, Context, Result};
use futures_util::stream::{self, StreamExt};
use rand::{rngs::StdRng, seq::SliceRandom, thread_rng, Rng, SeedableRng};
use serde::{Deserialize, Serialize};

use crate::http_retry::{
    is_connect_stage_reqwest_error, is_transient_reqwest_error, is_transient_status,
    retry_transient_http_async, RetryDecision,
};
use crate::worker_api::PendingKind;
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
const DEFAULT_RUNNER_HTTP_REQUEST_TIMEOUT_MS: u64 = 200_000;
const DEFAULT_FANOUT_RETRY_PASSES: usize = 1;
const MAX_RANDOM_MEMBERSHIP_BATCH_SIZE: usize = 8;
const ADD_BATCH_SEED_DOMAIN: u64 = 0x4144_4442_4154_4348;
const REMOVE_BATCH_SEED_DOMAIN: u64 = 0x524d_5642_4154_4348;
const EXTERNAL_BATCH_SEED_DOMAIN: u64 = 0x4558_5442_4154_4348;

static WORKER_COMMAND_REQUEST_COUNTER: AtomicU64 = AtomicU64::new(1);

static FAILURE_EXPERIMENT_MODE: AtomicBool = AtomicBool::new(false);
static PROFILED_FAILURE_POLICY: AtomicU8 =
    AtomicU8::new(ProfiledFailurePolicy::StopOnProfiledFailure as u8);
static PROFILED_OPERATION_JOURNAL: OnceLock<Mutex<Option<PathBuf>>> = OnceLock::new();

fn profiled_operation_journal() -> &'static Mutex<Option<PathBuf>> {
    PROFILED_OPERATION_JOURNAL.get_or_init(|| Mutex::new(None))
}

#[derive(Debug, Clone)]
pub struct StaircaseConfig {
    pub preflight_only: bool,
    pub ds_url: String,
    pub workers: Vec<WorkerSpec>,
    pub min_size: usize,
    pub max_size: Option<usize>,
    pub step_size: StepSize,
    pub plateau_sizes: Vec<usize>,
    pub plateau_order: PlateauOrder,
    pub roundtrips: usize,
    pub update_rounds: usize,
    pub app_rounds: usize,
    pub max_update_samples_per_plateau: usize,
    pub max_app_samples_per_payload: usize,
    pub min_profiled_samples_per_operation: usize,
    pub max_commit_receive_samples_per_plateau: usize,
    pub commit_receive_sampling_seed: u64,
    pub payload_sizes: PayloadSizes,
    pub scenario_seed: u64,
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
    pub process_pending_fanout: bool,
    pub profile_only_singletons: bool,
    pub external_coverage_lane: bool,
    pub worker_layout: Option<WorkerLayout>,
    pub no_aggregate: bool,
    pub failure_experiment: bool,
    pub profiled_failure_policy: ProfiledFailurePolicy,
    pub remove_rejoin: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ProfiledFailurePolicy {
    StopOnProfiledFailure = 0,
    RemoveAndContinue = 1,
}

impl Default for ProfiledFailurePolicy {
    fn default() -> Self {
        Self::StopOnProfiledFailure
    }
}

impl FromStr for ProfiledFailurePolicy {
    type Err = String;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        match value {
            "stop-on-profiled-failure" => Ok(Self::StopOnProfiledFailure),
            "remove-and-continue" => Ok(Self::RemoveAndContinue),
            other => Err(format!(
                "invalid profiled failure policy '{}'; expected stop-on-profiled-failure or remove-and-continue",
                other
            )),
        }
    }
}

fn store_profiled_failure_policy(policy: ProfiledFailurePolicy) {
    PROFILED_FAILURE_POLICY.store(policy as u8, Ordering::Relaxed);
}

fn profiled_failure_policy() -> ProfiledFailurePolicy {
    match PROFILED_FAILURE_POLICY.load(Ordering::Relaxed) {
        value if value == ProfiledFailurePolicy::RemoveAndContinue as u8 => {
            ProfiledFailurePolicy::RemoveAndContinue
        }
        _ => ProfiledFailurePolicy::StopOnProfiledFailure,
    }
}

fn should_continue_after_profiled_failure() -> bool {
    FAILURE_EXPERIMENT_MODE.load(Ordering::Relaxed)
        || profiled_failure_policy() == ProfiledFailurePolicy::RemoveAndContinue
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StepSize {
    Fixed(usize),
    UniformRange { min: usize, max: usize },
}

impl StepSize {
    fn sample<R: Rng + ?Sized>(&self, rng: &mut R) -> usize {
        match self {
            Self::Fixed(step_size) => *step_size,
            Self::UniformRange { min, max } => rng.gen_range(*min..=*max),
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

    fn shuffled_sources<R: Rng + ?Sized>(&self, rng: &mut R) -> Vec<PayloadSizeSource> {
        match self {
            Self::Fixed(sizes) => {
                let mut sizes = sizes.clone();
                sizes.shuffle(rng);
                sizes.into_iter().map(PayloadSizeSource::Fixed).collect()
            }
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
            Self::UniformRange { min, max } => rng.gen_range(min..=max),
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
    pub device_kind: String,
}

#[derive(Debug, Deserialize)]
struct ProfilingCapabilities {
    #[serde(default)]
    client_exists: Option<bool>,
    #[serde(default)]
    profiling_enabled: bool,
    #[serde(default = "default_true")]
    l1d_cache_profiling_enabled: bool,
    #[serde(default)]
    l1d_cache_counters_available: bool,
}

fn default_true() -> bool {
    true
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
    pub resource_profile_id: String,
    #[serde(default)]
    pub resource_experiment_type: String,
    #[serde(default)]
    pub memory_model: String,
    #[serde(default)]
    pub docker_memory_limit: String,
    #[serde(default)]
    pub app_heap_budget: String,
    #[serde(default)]
    pub app_heap_budget_bytes: Option<u64>,
    #[serde(default)]
    pub cpu_capacity_fraction: Option<f64>,
    #[serde(default)]
    pub assigned_core_count: Option<u32>,
    #[serde(default)]
    pub cpuset: Option<String>,
    #[serde(default)]
    pub profiled_singleton: bool,
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
    #[serde(default)]
    pub failure_experiment: Option<FailureExperimentConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FailureExperimentConfig {
    pub mode: String,
    pub seed: u64,
    pub cpu_caps: Vec<f64>,
    pub ram_caps: Vec<String>,
    pub grid_cells: usize,
    pub swap_equals_ram: bool,
    #[serde(default)]
    pub interpretation: String,
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
            device_kind: String::new(),
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
    membership_batch_requested: Option<usize>,
    membership_batch_effective: Option<usize>,
    membership_batch_group_cap: Option<usize>,
    membership_batch_transition_cap: Option<usize>,
    membership_batch_source: Option<String>,
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
            membership_batch_requested: None,
            membership_batch_effective: None,
            membership_batch_group_cap: None,
            membership_batch_transition_cap: None,
            membership_batch_source: None,
        }
    }

    fn at_operation(mut self, operation_seq: usize, payload_size: Option<usize>) -> Self {
        self.operation_seq = Some(operation_seq);
        self.payload_size = payload_size;
        self
    }

    fn with_membership_batch(mut self, decision: &MembershipBatchDecision) -> Self {
        self.membership_batch_requested = Some(decision.requested);
        self.membership_batch_effective = Some(decision.effective);
        self.membership_batch_group_cap = Some(decision.group_cap);
        self.membership_batch_transition_cap = Some(decision.transition_cap);
        self.membership_batch_source = Some(decision.source.to_string());
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
    membership_batch_requested: Option<usize>,
    membership_batch_effective: Option<usize>,
    membership_batch_group_cap: Option<usize>,
    membership_batch_transition_cap: Option<usize>,
    membership_batch_source: Option<String>,
    configured_payload_label: Option<String>,
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
            profile_schema_version: 11,
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
            membership_batch_requested: cursor.membership_batch_requested,
            membership_batch_effective: cursor.membership_batch_effective,
            membership_batch_group_cap: cursor.membership_batch_group_cap,
            membership_batch_transition_cap: cursor.membership_batch_transition_cap,
            membership_batch_source: cursor.membership_batch_source.clone(),
            configured_payload_label: cursor.payload_size.map(|s| s.to_string()),
        };
        let mut out = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .with_context(|| format!("Failed to open runner event log {}", self.path.display()))?;
        serde_json::to_writer(&mut out, &event)?;
        writeln!(out)?;
        let profile_path = self.run_dir.join(format!("client-{}.jsonl", worker.id));
        if let Err(profile_error) = append_profile_event(&profile_path, &event.to_profile_event()) {
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

    fn record_failure(
        &self,
        cursor: &BenchmarkCursor,
        worker: &WorkerSpec,
        error: &anyhow::Error,
        failure_class: &str,
        failure_evidence_source: Option<&str>,
        failure_evidence_detail: Option<&str>,
        action: &str,
        reassigned_to: Option<&WorkerSpec>,
    ) -> Result<()> {
        let event = RunnerEvent {
            profile_schema_version: 11,
            ts_unix_ns: unix_time_ns(),
            event_kind: "worker_failure".to_string(),
            failed_worker_id: worker.id.clone(),
            failed_physical_worker_id: worker.physical_worker_id.clone(),
            failure_class: failure_class.to_string(),
            failure_detail: format!("{:#}", error),
            failure_evidence_source: failure_evidence_source.map(|s| s.to_string()),
            failure_evidence_detail: failure_evidence_detail.map(|s| s.to_string()),
            failure_action: action.to_string(),
            reassigned_to_worker_id: reassigned_to.map(|candidate| candidate.id.clone()),
            benchmark_plateau_index: cursor.plateau_index,
            benchmark_target_size: cursor.target_size,
            benchmark_active_size: cursor.active_size,
            benchmark_phase: cursor.phase.clone(),
            benchmark_operation: cursor.operation.clone(),
            benchmark_operation_seq: cursor.operation_seq,
            benchmark_payload_size: cursor.payload_size,
            membership_batch_requested: cursor.membership_batch_requested,
            membership_batch_effective: cursor.membership_batch_effective,
            membership_batch_group_cap: cursor.membership_batch_group_cap,
            membership_batch_transition_cap: cursor.membership_batch_transition_cap,
            membership_batch_source: cursor.membership_batch_source.clone(),
            configured_payload_label: cursor.payload_size.map(|s| s.to_string()),
        };
        let mut out = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .with_context(|| format!("Failed to open runner event log {}", self.path.display()))?;
        serde_json::to_writer(&mut out, &event)?;
        writeln!(out)?;
        let profile_path = self.run_dir.join(format!("client-{}.jsonl", worker.id));
        if let Err(profile_error) = append_profile_event(&profile_path, &event.to_profile_event()) {
            eprintln!(
                "[failure-attrition] WARNING: failed to append duplicate profile row for worker {}: {:#}; runner journal {} remains authoritative",
                worker.id,
                profile_error,
                self.path.display()
            );
        }
        eprintln!(
            "[failure-attrition] worker={} physical_worker={} class={} phase={} operation={} action={}",
            worker.id, worker.physical_worker_id, failure_class, cursor.phase, cursor.operation, action
        );
        Ok(())
    }
}

impl RunnerEvent {
    fn to_profile_event(&self) -> ProfileEvent {
        let mut event = ProfileEvent {
            profile_schema_version: Some(self.profile_schema_version),
            ts_unix_ns: self.ts_unix_ns,
            op: "benchmark.worker_failure".to_string(),
            measurement_class: Some("runner_failure".to_string()),
            runner_event_kind: Some(self.event_kind.clone()),
            failed_worker_id: Some(self.failed_worker_id.clone()),
            failed_physical_worker_id: Some(self.failed_physical_worker_id.clone()),
            failure_class: Some(self.failure_class.clone()),
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
            configured_payload_label: self.configured_payload_label.clone(),
            worker_id: Some(self.failed_worker_id.clone()),
            global_span_id: None,
            parent_global_span_id: None,
            implementation: "benchmark_runner".to_string(),
            thread_id: "benchmark-runner".to_string(),
            pid: std::process::id(),
            ..ProfileEvent::default()
        };
        apply_app_heap_budget_failure_fields(&mut event);
        event
    }
}

fn apply_app_heap_budget_failure_fields(event: &mut ProfileEvent) {
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
        app_heap_field(detail, "app_heap_budget_bytes").and_then(|value| value.parse::<u64>().ok());
    event.heap_current_live_bytes = app_heap_field(detail, "current_live_heap_bytes")
        .and_then(|value| value.parse::<u64>().ok());
    event.heap_peak_live_bytes =
        app_heap_field(detail, "peak_live_heap_bytes").and_then(|value| value.parse::<u64>().ok());
    event.heap_operation_peak_live_bytes = app_heap_field(detail, "operation_peak_live_heap_bytes")
        .and_then(|value| value.parse::<u64>().ok());
    event.heap_total_allocated_bytes =
        app_heap_field(detail, "total_allocated_bytes").and_then(|value| value.parse::<u64>().ok());
    event.heap_allocation_count =
        app_heap_field(detail, "allocation_count").and_then(|value| value.parse::<u64>().ok());
    event.heap_deallocation_count =
        app_heap_field(detail, "deallocation_count").and_then(|value| value.parse::<u64>().ok());
    event.heap_failed_allocation_size_bytes =
        app_heap_field(detail, "failed_allocation_size_bytes")
            .and_then(|value| value.parse::<u64>().ok());
    event.heap_failure_context = app_heap_field(detail, "heap_failure_context");
    event.span_id =
        app_heap_field(detail, "failure_span_id").and_then(|value| value.parse::<u64>().ok());

    if event.operation_family.is_none() {
        event.operation_family = app_heap_field(detail, "operation_family");
    }
    if let Some(member_count) =
        app_heap_field(detail, "member_count").and_then(|value| value.parse::<usize>().ok())
    {
        event.member_count = Some(member_count);
        event.member_count_before.get_or_insert(member_count);
    }
    if event.group_epoch.is_none() {
        event.group_epoch =
            app_heap_field(detail, "epoch").and_then(|value| value.parse::<u64>().ok());
    }
}

fn app_heap_field(detail: &str, key: &str) -> Option<String> {
    let prefix = format!("{key}=");
    for token in detail.split_whitespace() {
        if let Some(value) = token.strip_prefix(&prefix) {
            return Some(value.trim_matches('"').to_string());
        }
    }
    None
}

fn append_profile_event(path: &Path, event: &ProfileEvent) -> Result<()> {
    let mut out = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .with_context(|| format!("Failed to open {}", path.display()))?;
    serde_json::to_writer(&mut out, event)?;
    writeln!(out)?;
    Ok(())
}

fn profile_contains_runner_event(path: &Path, runner_event: &RunnerEvent) -> Result<bool> {
    if !path.exists() {
        return Ok(false);
    }

    let file = File::open(path).with_context(|| format!("Failed to open {}", path.display()))?;
    for line in BufReader::new(file).lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let event: ProfileEvent = serde_json::from_str(&line)
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
        let profile_path = run_dir.join(format!("client-{}.jsonl", event.failed_worker_id));
        if !profile_contains_runner_event(&profile_path, &event)? {
            append_profile_event(&profile_path, &event.to_profile_event())?;
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
                device_kind: c.device_kind.clone(),
            }
        })
        .collect()
}

pub fn measured_active_clients(active: &[WorkerSpec]) -> Vec<&WorkerSpec> {
    active.iter().filter(|w| w.profile_enabled).collect()
}

fn frontload_profile_enabled_singletons_in_idle(
    idle: VecDeque<WorkerSpec>,
) -> VecDeque<WorkerSpec> {
    let mut external_devices = Vec::new();
    let mut profiled_singletons = Vec::new();
    let mut other_workers = Vec::new();

    for worker in idle {
        if is_external_device(&worker) {
            external_devices.push(worker);
        } else if worker.profile_enabled && worker.container_mode == ContainerMode::Singleton {
            profiled_singletons.push(worker);
        } else {
            other_workers.push(worker);
        }
    }

    external_devices
        .into_iter()
        .chain(profiled_singletons)
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

#[derive(Debug, Clone)]
struct GroupStateSnapshot {
    group_id: String,
    epoch: u64,
    members: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct NetworkPhaseMetrics {
    pub phase: String,
    pub group_size: usize,
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

#[derive(Debug, Default, Deserialize, Serialize)]
struct ProfileEvent {
    #[serde(default)]
    profile_schema_version: Option<u32>,
    ts_unix_ns: u128,
    op: String,
    #[serde(default)]
    measurement_class: Option<String>,
    #[serde(default)]
    measurement_plane: Option<String>,
    #[serde(default)]
    span_kind: Option<String>,
    #[serde(default)]
    span_name: Option<String>,
    #[serde(default)]
    span_id: Option<u64>,
    #[serde(default)]
    parent_span_id: Option<u64>,
    #[serde(default)]
    parent_operation: Option<String>,
    #[serde(default)]
    span_inclusive: Option<bool>,
    #[serde(default)]
    runner_event_kind: Option<String>,
    #[serde(default)]
    failed_worker_id: Option<String>,
    #[serde(default)]
    failed_physical_worker_id: Option<String>,
    #[serde(default)]
    failure_class: Option<String>,
    #[serde(default)]
    failure_detail: Option<String>,
    #[serde(default)]
    failure_evidence_source: Option<String>,
    #[serde(default)]
    failure_evidence_detail: Option<String>,
    #[serde(default)]
    failure_action: Option<String>,
    #[serde(default)]
    reassigned_to_worker_id: Option<String>,
    #[serde(default)]
    memory_model: Option<String>,
    #[serde(default)]
    app_heap_budget: Option<String>,
    #[serde(default)]
    app_heap_budget_bytes: Option<u64>,
    #[serde(default)]
    heap_current_live_bytes: Option<u64>,
    #[serde(default)]
    heap_peak_live_bytes: Option<u64>,
    #[serde(default)]
    heap_operation_peak_live_bytes: Option<u64>,
    #[serde(default)]
    heap_total_allocated_bytes: Option<u64>,
    #[serde(default)]
    heap_allocation_count: Option<u64>,
    #[serde(default)]
    heap_deallocation_count: Option<u64>,
    #[serde(default)]
    heap_failed_allocation_size_bytes: Option<u64>,
    #[serde(default)]
    heap_failure_context: Option<String>,
    #[serde(default)]
    benchmark_plateau_index: Option<usize>,
    #[serde(default)]
    benchmark_target_size: Option<usize>,
    #[serde(default)]
    benchmark_active_size: Option<usize>,
    #[serde(default)]
    benchmark_phase: Option<String>,
    #[serde(default)]
    benchmark_operation: Option<String>,
    #[serde(default)]
    benchmark_operation_seq: Option<usize>,
    #[serde(default)]
    benchmark_payload_size: Option<usize>,
    #[serde(default)]
    membership_batch_requested: Option<usize>,
    #[serde(default)]
    membership_batch_effective: Option<usize>,
    #[serde(default)]
    membership_batch_group_cap: Option<usize>,
    #[serde(default)]
    membership_batch_transition_cap: Option<usize>,
    #[serde(default)]
    membership_batch_source: Option<String>,
    #[serde(default)]
    configured_payload_label: Option<String>,
    implementation: String,
    wall_ns: u128,
    cpu_thread_ns: Option<u128>,
    #[serde(default)]
    cpu_process_ns: u128,
    #[serde(default)]
    cpu_envelope_utilization: Option<f64>,
    #[serde(default)]
    cpu_throttled_time_ratio: Option<f64>,
    #[serde(default)]
    cpu_nr_periods_delta: Option<u64>,
    #[serde(default)]
    cpu_nr_throttled_delta: Option<u64>,
    #[serde(default)]
    cpu_throttled_usec_delta: Option<u128>,
    #[serde(default)]
    cpu_throttled_period_fraction: Option<f64>,
    #[serde(default)]
    cpu_nr_periods_cumulative: Option<u64>,
    #[serde(default)]
    cpu_nr_throttled_cumulative: Option<u64>,
    #[serde(default)]
    cpu_throttled_usec_cumulative: Option<u128>,
    #[serde(default)]
    cpu_throttled_period_fraction_cumulative: Option<f64>,
    #[serde(default)]
    cpu_throttled_period_threshold: Option<f64>,
    #[serde(default)]
    cpu_throttled_period_threshold_crossing: Option<bool>,
    alloc_bytes: Option<u64>,
    alloc_count: Option<u64>,
    #[serde(default)]
    alloc_measurement_scope: Option<String>,
    #[serde(default)]
    l1d_cache_accesses: Option<u64>,
    #[serde(default)]
    l1d_cache_misses: Option<u64>,
    #[serde(default)]
    l1d_measurement_scope: Option<String>,
    #[serde(default)]
    l1d_cache_status: Option<String>,
    #[serde(default)]
    l1d_measured_thread_count: Option<usize>,
    #[serde(default)]
    l1d_discovered_thread_count: Option<usize>,
    #[serde(default)]
    l1d_multiplexed_thread_count: Option<usize>,
    #[serde(default)]
    ram_rss_delta_bytes: Option<i64>,
    #[serde(default)]
    ram_rss_utilization: Option<f64>,
    artifact_size_bytes: Option<usize>,
    #[serde(default)]
    welcome_bytes: Option<usize>,
    #[serde(default)]
    ratchet_tree_bytes: Option<usize>,
    #[serde(default)]
    welcome_plus_ratchet_tree_bytes: Option<usize>,
    #[serde(default)]
    group_info_bytes: Option<usize>,
    #[serde(default)]
    group_info_plaintext_bytes: Option<usize>,
    #[serde(default)]
    group_info_ciphertext_bytes: Option<usize>,
    encrypted_group_info_bytes: Option<usize>,
    encrypted_secrets_count: Option<usize>,
    group_epoch: Option<u64>,
    tree_size: Option<u32>,
    #[serde(default)]
    tree_height: Option<u32>,
    #[serde(default)]
    tree_leaf_count: Option<u32>,
    #[serde(default)]
    tree_node_count: Option<u32>,
    #[serde(default)]
    operation_family: Option<String>,
    member_count: Option<usize>,
    #[serde(default)]
    member_count_before: Option<usize>,
    #[serde(default)]
    member_count_after: Option<usize>,
    invitee_count: Option<isize>,
    #[serde(default)]
    added_members_count: Option<usize>,
    #[serde(default)]
    removed_members_count: Option<usize>,
    #[serde(default)]
    removed_leaf_indices: Option<Vec<u32>>,
    #[serde(default)]
    removed_right_edge_count: Option<usize>,
    #[serde(default)]
    rightmost_removed_leaf: Option<u32>,
    #[serde(default)]
    removed_right_edge_suffix_count: Option<usize>,
    #[serde(default)]
    right_edge_suffix_fully_removed: Option<bool>,
    #[serde(default)]
    tree_truncated: Option<bool>,
    #[serde(default)]
    truncated_levels_count: Option<usize>,
    #[serde(default)]
    tree_size_before: Option<u32>,
    #[serde(default)]
    tree_size_after: Option<u32>,
    #[serde(default)]
    tree_leaf_count_before: Option<u32>,
    #[serde(default)]
    tree_leaf_count_after: Option<u32>,
    #[serde(default)]
    tree_node_count_before: Option<u32>,
    #[serde(default)]
    tree_node_count_after: Option<u32>,
    #[serde(default)]
    add_commit_mode: Option<String>,
    #[serde(default)]
    remove_commit_mode: Option<String>,
    #[serde(default)]
    commit_path_policy: Option<String>,
    #[serde(default)]
    force_self_update: Option<bool>,
    #[serde(default)]
    update_path_present: Option<bool>,
    ciphersuite: Option<String>,
    #[serde(default)]
    committer_leaf_index: Option<u32>,
    #[serde(default)]
    joiner_leaf_index: Option<u32>,
    #[serde(default)]
    direct_path_len: Option<usize>,
    #[serde(default)]
    filtered_direct_path_len: Option<usize>,
    #[serde(default)]
    copath_len: Option<usize>,
    #[serde(default)]
    update_path_nodes_count: Option<usize>,
    #[serde(default)]
    encrypted_path_secret_count: Option<usize>,
    #[serde(default)]
    sum_copath_resolution_sizes: Option<usize>,
    #[serde(default)]
    max_copath_resolution_size: Option<usize>,
    #[serde(default)]
    path_secret_derivation_count: Option<u64>,
    #[serde(default)]
    node_secret_derivation_count: Option<u64>,
    #[serde(default)]
    hpke_encrypt_count: Option<u64>,
    #[serde(default)]
    hpke_decrypt_count: Option<u64>,
    #[serde(default)]
    tree_hash_nodes_touched: Option<u64>,
    #[serde(default)]
    parent_hash_nodes_touched: Option<u64>,
    #[serde(default)]
    commit_size_bytes: Option<usize>,
    #[serde(default)]
    commit_message_size_bytes: Option<usize>,
    #[serde(default)]
    commit_kind: Option<String>,
    #[serde(default)]
    commit_create_op: Option<String>,
    #[serde(default)]
    commit_semantics: Option<String>,
    #[serde(default)]
    add_semantics: Option<String>,
    #[serde(default)]
    commit_id: Option<String>,
    #[serde(default)]
    commit_has_path: Option<bool>,
    #[serde(default)]
    commit_is_external: Option<bool>,
    #[serde(default)]
    update_path_size_bytes: Option<usize>,
    #[serde(default)]
    welcome_recipient_count: Option<usize>,
    #[serde(default)]
    ratchet_tree_included: Option<bool>,
    #[serde(default)]
    ratchet_tree_delivery_mode: Option<String>,
    app_msg_plaintext_bytes: Option<usize>,
    app_msg_padding_bytes: Option<usize>,
    app_msg_ciphertext_bytes: Option<usize>,
    aad_bytes: Option<usize>,
    sender_leaf_index: Option<u32>,
    sender_generation: Option<u64>,
    first_message_in_epoch: Option<bool>,
    receiver_leaf_index: Option<u32>,
    #[serde(default)]
    receiver_member_index: Option<u32>,
    #[serde(default)]
    receiver_is_committer: Option<bool>,
    #[serde(default)]
    commit_receive_sampled: Option<bool>,
    #[serde(default)]
    commit_receive_sampling_policy: Option<String>,
    #[serde(default)]
    commit_receive_sampling_seed: Option<u64>,
    #[serde(default)]
    commit_receive_sample_index: Option<usize>,
    #[serde(default)]
    commit_receive_sample_count: Option<usize>,
    #[serde(default)]
    commit_receive_population_size: Option<usize>,
    #[serde(default)]
    selected_encrypted_path_secret_index: Option<usize>,
    #[serde(default)]
    path_secret_decryption_count: Option<u64>,
    #[serde(default)]
    confirmation_tag_verified: Option<bool>,
    #[serde(default)]
    proposal_count: Option<usize>,
    #[serde(default)]
    inline_proposal_count: Option<usize>,
    #[serde(default)]
    proposal_ref_count: Option<usize>,
    #[serde(default)]
    add_proposal_count: Option<usize>,
    #[serde(default)]
    update_proposal_count: Option<usize>,
    #[serde(default)]
    remove_proposal_count: Option<usize>,
    first_receive_from_sender: Option<bool>,
    generation_gap: Option<u64>,
    out_of_order_message: Option<bool>,
    aead_decrypt_count: Option<u64>,
    sender_data_decrypt_count: Option<u64>,
    signature_verify_count: Option<u64>,
    pid: u32,
    thread_id: String,
    #[serde(default)]
    worker_id: Option<String>,
    #[serde(default)]
    global_span_id: Option<String>,
    #[serde(default)]
    parent_global_span_id: Option<String>,
    run_id: Option<String>,
    scenario: Option<String>,
    #[serde(default)]
    scenario_seed: Option<u64>,
    node_name: Option<String>,
    pod_name: Option<String>,
    #[serde(default)]
    device_kind: Option<String>,
    #[serde(default)]
    execution_backend: Option<String>,
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
                    "[fanout-adaptive] phase={} operation={} reducing parallelism {} -> {} failures={} error_rate={:.4} p95_ms={} reason={}",
                    phase,
                    operation,
                    previous,
                    self.current_parallelism,
                    summary.failure_count,
                    error_rate,
                    p95,
                    if error_spike { "error_rate" } else { "p95_latency" }
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

#[derive(Debug, Clone, PartialEq, Eq)]
struct MembershipBatchDecision {
    requested: usize,
    effective: usize,
    group_cap: usize,
    transition_cap: usize,
    source: &'static str,
}

#[derive(Debug)]
struct MembershipBatchPlanner {
    rng: StdRng,
    cycle: VecDeque<usize>,
    cycle_cap: usize,
}

impl MembershipBatchPlanner {
    fn new(seed: u64) -> Self {
        Self {
            rng: StdRng::seed_from_u64(seed),
            cycle: VecDeque::new(),
            cycle_cap: 0,
        }
    }

    fn next_batch(
        &mut self,
        current_group_size: usize,
        max_allowed: usize,
        source: &'static str,
    ) -> MembershipBatchDecision {
        if max_allowed == 0 {
            return MembershipBatchDecision {
                requested: 0,
                effective: 0,
                group_cap: 0,
                transition_cap: 0,
                source,
            };
        }

        let group_cap = membership_batch_group_cap(current_group_size);
        let feasible_cap = group_cap.min(max_allowed);
        if self.cycle.is_empty() || self.cycle_cap != feasible_cap {
            let mut values = (1..=feasible_cap).collect::<Vec<_>>();
            values.shuffle(&mut self.rng);
            if feasible_cap > 1 {
                let max_pos = values
                    .iter()
                    .position(|value| *value == feasible_cap)
                    .unwrap_or(0);
                values.swap(0, max_pos);
            }
            self.cycle = values.into();
            self.cycle_cap = feasible_cap;
        }
        let requested = self.cycle.pop_front().unwrap_or(1);
        let effective = requested;

        MembershipBatchDecision {
            requested,
            effective,
            group_cap,
            transition_cap: max_allowed,
            source,
        }
    }
}

#[derive(Debug)]
struct MembershipBatchPlans {
    regular: MembershipBatchPlanner,
    external: HashMap<String, MembershipBatchPlanner>,
    external_seed: u64,
}

impl MembershipBatchPlans {
    fn new(seed: u64) -> Self {
        Self {
            regular: MembershipBatchPlanner::new(seed),
            external: HashMap::new(),
            external_seed: seed ^ EXTERNAL_BATCH_SEED_DOMAIN,
        }
    }

    fn next_batch(
        &mut self,
        current_group_size: usize,
        max_allowed: usize,
        external_actor_id: Option<&str>,
    ) -> MembershipBatchDecision {
        if let Some(actor_id) = external_actor_id {
            let actor_seed = self.external_seed ^ stable_string_seed(actor_id);
            self.external
                .entry(actor_id.to_string())
                .or_insert_with(|| MembershipBatchPlanner::new(actor_seed))
                .next_batch(current_group_size, max_allowed, "balanced_seeded_external")
        } else {
            self.regular
                .next_batch(current_group_size, max_allowed, "balanced_seeded_regular")
        }
    }
}

fn stable_string_seed(value: &str) -> u64 {
    value
        .as_bytes()
        .iter()
        .fold(0xcbf2_9ce4_8422_2325, |hash, byte| {
            (hash ^ u64::from(*byte)).wrapping_mul(0x0000_0100_0000_01b3)
        })
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

#[derive(Debug, Clone)]
struct ExpectedGroupState {
    group_id: String,
    epoch: u64,
    members: Vec<String>,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
enum ExpectedReceiveCommitState {
    Group(ExpectedGroupState),
    Removed { expected_epoch: u64 },
}

impl From<GroupStateSnapshot> for ExpectedGroupState {
    fn from(snapshot: GroupStateSnapshot) -> Self {
        Self {
            group_id: snapshot.group_id,
            epoch: snapshot.epoch,
            members: snapshot.members,
        }
    }
}

impl ExpectedGroupState {
    fn matches(&self, snapshot: &GroupStateSnapshot) -> bool {
        snapshot.group_id == self.group_id
            && snapshot.epoch == self.epoch
            && snapshot.members == self.members
    }
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
    FAILURE_EXPERIMENT_MODE.store(config.failure_experiment, Ordering::Relaxed);
    store_profiled_failure_policy(config.profiled_failure_policy);
    if config.failure_experiment {
        eprintln!(
            "[failure-experiment] enabled: profiled singleton failures will be recorded but the run will continue"
        );
    }
    if should_continue_after_profiled_failure() && !config.failure_experiment {
        eprintln!(
            "[resource-failure-policy] remove-and-continue enabled: profiled singleton failures will be recorded and evicted when possible"
        );
    }
    let max_size = validate_config(&config, config.workers.len())?;

    let run_dir = run_dir_for(&config.output_dir, &config.run_id);
    fs::create_dir_all(&run_dir)?;
    configure_profiled_operation_journal(&run_dir);
    let runner_events = RunnerEventLog::new(&run_dir);

    for worker in &config.workers {
        if worker.profile_enabled {
            let profile_path = run_dir.join(format!("client-{}.jsonl", worker.id));
            if !profile_path.exists() {
                let _ = OpenOptions::new()
                    .create(true)
                    .write(true)
                    .truncate(false)
                    .open(&profile_path);
            }
        }
    }

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
        "[network] runner http_pool_max_idle_per_host={} connect_timeout_ms={} request_timeout_ms={} max_fanout_parallelism={} min_fanout_parallelism={} fanout_adaptive={} initial_effective_fanout_parallelism={} fanout_error_rate_threshold={:.4} fanout_p95_threshold_ms={} process_pending_fanout={}",
        http_pool_max_idle_per_host,
        runner_http_connect_timeout.as_millis(),
        runner_http_request_timeout.as_millis(),
        max_fanout_parallelism,
        min_fanout_parallelism,
        fanout_adaptive,
        fanout.parallelism(),
        fanout_error_rate_threshold,
        fanout_p95_threshold_ms,
        config.process_pending_fanout
    );

    let pool_idle_secs = runner_http_pool_idle_timeout_secs();
    let http = reqwest::Client::builder()
        .connect_timeout(runner_http_connect_timeout)
        .timeout(runner_http_request_timeout)
        .pool_max_idle_per_host(http_pool_max_idle_per_host)
        .pool_idle_timeout(Duration::from_secs(pool_idle_secs))
        .tcp_keepalive(Some(Duration::from_secs((pool_idle_secs / 2).max(5))))
        .build()
        .context("Failed to build HTTP client")?;

    wait_for_health(&http, &config.ds_url, Duration::from_secs(10))
        .await
        .with_context(|| format!("DS at {} is not healthy", config.ds_url))?;

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
        eprintln!("[preflight] preflight-only mode complete; skipping MLS benchmark logic");
        return Ok(());
    }

    let mut plateau_rng = StdRng::seed_from_u64(config.scenario_seed);
    let protected_floor = protected_member_floor(
        &config.workers,
        config.profile_only_singletons,
        config.external_coverage_lane,
    );
    let effective_min_size = config.min_size.max(protected_floor);
    if effective_min_size > max_size {
        return Err(anyhow!(
            "protected calibration members require min-size {}, but max-size is {}",
            effective_min_size,
            max_size
        ));
    }
    if effective_min_size > config.min_size {
        eprintln!(
            "[sampling] raising effective min-size {} -> {} to keep protected calibration members active",
            config.min_size, effective_min_size
        );
    }

    if config
        .plateau_sizes
        .first()
        .is_some_and(|first| *first < effective_min_size)
    {
        return Err(anyhow!(
            "explicit --plateau-sizes starts below protected minimum {}; got {:?}",
            effective_min_size,
            config.plateau_sizes
        ));
    }
    let plateau_sequence = if config.plateau_sizes.is_empty() {
        build_plateau_sequence_for_order(
            effective_min_size,
            max_size,
            &config.step_size,
            config.roundtrips,
            config.plateau_order,
            &mut plateau_rng,
        )
    } else {
        config.plateau_sizes.clone()
    };

    #[derive(Serialize)]
    struct ScenarioPlan<'a> {
        run_id: &'a str,
        scenario_seed: u64,
        plateau_order: PlateauOrder,
        plateau_sequence: &'a [usize],
        min_size: usize,
        max_size: usize,
        step_size: String,
        plateau_sizes: Option<&'a [usize]>,
        roundtrips: usize,
        payload_sizes: String,
        min_external_samples_per_operation: usize,
        randomized_membership_batches: bool,
        randomized_actor_selection: bool,
        randomized_payload_order: bool,
    }

    let scenario_plan = ScenarioPlan {
        run_id: &config.run_id,
        scenario_seed: config.scenario_seed,
        plateau_order: config.plateau_order,
        plateau_sequence: &plateau_sequence,
        min_size: effective_min_size,
        max_size,
        step_size: config.step_size.to_string(),
        plateau_sizes: (!config.plateau_sizes.is_empty()).then_some(&config.plateau_sizes),
        roundtrips: config.roundtrips,
        payload_sizes: config.payload_sizes.to_string(),
        min_external_samples_per_operation: config.min_profiled_samples_per_operation,
        randomized_membership_batches: true,
        randomized_actor_selection: true,
        randomized_payload_order: true,
    };
    fs::write(
        run_dir.join("scenario_plan.json"),
        serde_json::to_vec_pretty(&scenario_plan)?,
    )?;

    let total_units = estimate_total_units(
        &plateau_sequence,
        config.update_rounds,
        config.app_rounds,
        config.max_update_samples_per_plateau,
        config.max_app_samples_per_payload,
        config.payload_sizes.source_count(),
        config
            .workers
            .iter()
            .filter(|worker| is_external_device(worker))
            .count(),
        config.external_coverage_lane,
        config.min_profiled_samples_per_operation,
    );

    eprintln!(
        "Scenario plan: plateau_order={}, plateaus={:?}, step_size={}, payload_sizes={}, scenario_seed={}, update_cap={}, app_cap={}, min_external_samples={}, total_units≈{}",
        config.plateau_order,
        plateau_sequence,
        config.step_size,
        config.payload_sizes,
        config.scenario_seed,
        config.max_update_samples_per_plateau,
        config.max_app_samples_per_payload,
        config.min_profiled_samples_per_operation,
        total_units
    );

    let mut scenario_rng = StdRng::seed_from_u64(config.scenario_seed);
    let mut progress = Progress::new(total_units);
    progress.render("starting");

    let leader = config.workers[0].clone();
    let mut active = vec![leader.clone()];
    let mut idle: VecDeque<WorkerSpec> = config.workers.iter().skip(1).cloned().collect();
    if config.profile_only_singletons {
        idle = frontload_profile_enabled_singletons_in_idle(idle);
        eprintln!(
            "[sampling] front-loaded profile-enabled singleton joiners for AddCommit coverage; packed clients remain unprofiled"
        );
    }
    if config.external_coverage_lane {
        eprintln!(
            "[sampling] external coverage lane enabled; active external devices are protected from random removal and scheduled as app/update/add/remove actors"
        );
    }
    let mut add_membership_batches =
        MembershipBatchPlans::new(config.scenario_seed ^ ADD_BATCH_SEED_DOMAIN);
    let mut remove_membership_batches =
        MembershipBatchPlans::new(config.scenario_seed ^ REMOVE_BATCH_SEED_DOMAIN);
    let mut profiled_add_actor_seen = HashSet::new();

    create_group(&http, &leader, &mut progress).await?;
    let active_ids: Vec<String> = active.iter().map(|w| w.id.clone()).collect();
    let initial_state =
        ensure_converged(&http, &active, &active_ids, max_fanout_parallelism).await?;
    eprintln!(
        "\nInitial convergence: group_id={}, epoch={}, members={:?}",
        initial_state.group_id, initial_state.epoch, initial_state.members
    );

    for (plateau_idx, &target_size) in plateau_sequence.iter().enumerate() {
        eprintln!(
            "\n=== Plateau {}/{} | target active members = {} ===",
            plateau_idx + 1,
            plateau_sequence.len(),
            target_size
        );
        transition_to_size(
            &http,
            &mut active,
            &mut idle,
            target_size,
            &mut fanout,
            &mut add_membership_batches,
            &mut remove_membership_batches,
            &mut progress,
            config.process_pending_fanout,
            config.external_coverage_lane,
            config.profile_only_singletons,
            &mut profiled_add_actor_seen,
            config.max_commit_receive_samples_per_plateau,
            config.commit_receive_sampling_seed,
            &mut scenario_rng,
            plateau_idx + 1,
            &runner_events,
        )
        .await?;

        let state = ensure_converged_with_attrition(
            &http,
            &mut active,
            &mut fanout,
            config.process_pending_fanout,
            target_size,
            config.max_commit_receive_samples_per_plateau,
            config.commit_receive_sampling_seed,
            max_fanout_parallelism,
            plateau_idx + 1,
            &runner_events,
        )
        .await?;
        eprintln!(
            "\n[plateau {}] converged at epoch {} with members {:?}",
            target_size, state.epoch, state.members
        );

        if config.remove_rejoin {
            run_remove_rejoin_phase(
                &http,
                &mut active,
                &mut idle,
                &mut fanout,
                &mut progress,
                config.process_pending_fanout,
                config.external_coverage_lane,
                config.min_profiled_samples_per_operation,
                target_size,
                config.max_commit_receive_samples_per_plateau,
                config.commit_receive_sampling_seed,
                &mut scenario_rng,
                plateau_idx + 1,
                &runner_events,
            )
            .await?;
        }

        if !config.remove_rejoin {
            run_external_add_density_phase(
                &http,
                &mut active,
                &mut idle,
                &mut fanout,
                &mut progress,
                config.process_pending_fanout,
                config.external_coverage_lane,
                config.min_profiled_samples_per_operation,
                target_size,
                config.max_commit_receive_samples_per_plateau,
                config.commit_receive_sampling_seed,
                &mut scenario_rng,
                plateau_idx + 1,
                &runner_events,
            )
            .await?;

            run_update_phase(
                &http,
                &mut active,
                target_size,
                config.update_rounds,
                config.max_update_samples_per_plateau,
                config.max_commit_receive_samples_per_plateau,
                config.commit_receive_sampling_seed,
                &mut fanout,
                &mut progress,
                config.process_pending_fanout,
                config.external_coverage_lane,
                config.min_profiled_samples_per_operation,
                &mut scenario_rng,
                plateau_idx + 1,
                &runner_events,
            )
            .await?;

            let state_after_updates = ensure_converged_with_attrition(
                &http,
                &mut active,
                &mut fanout,
                config.process_pending_fanout,
                target_size,
                config.max_commit_receive_samples_per_plateau,
                config.commit_receive_sampling_seed,
                max_fanout_parallelism,
                plateau_idx + 1,
                &runner_events,
            )
            .await?;
            eprintln!(
                "\n[plateau {}] post-update convergence at epoch {}",
                target_size, state_after_updates.epoch
            );

            run_application_phase(
                &http,
                &mut active,
                target_size,
                config.app_rounds,
                config.max_app_samples_per_payload,
                config.max_commit_receive_samples_per_plateau,
                config.commit_receive_sampling_seed,
                &config.payload_sizes,
                &mut fanout,
                &mut progress,
                config.external_coverage_lane,
                config.min_profiled_samples_per_operation,
                &mut scenario_rng,
                plateau_idx + 1,
                &runner_events,
            )
            .await?;
        }

        eprintln!("\n=== Plateau {} complete ===", target_size);
    }

    progress.finish();

    let worker_ids: Vec<String> = config.workers.iter().map(|w| w.id.clone()).collect();
    if !config.no_aggregate {
        aggregate_csv(&run_dir, &worker_ids, &config.worker_layout)?;
    } else {
        eprintln!("[aggregate] --no-aggregate set, skipping CSV aggregation");
    }

    println!(
        "HTTP staircase benchmark finished. Output in {}",
        run_dir.display()
    );
    Ok(())
}

fn effective_max_fanout_parallelism(configured: usize) -> usize {
    if configured > 0 {
        return configured;
    }

    DEFAULT_MAX_FANOUT_PARALLELISM
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

fn runner_http_pool_idle_timeout_secs() -> u64 {
    std::env::var("OPENMLS_RUNNER_HTTP_POOL_IDLE_TIMEOUT_SECS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(30)
}

fn runner_http_connect_timeout_ms() -> u64 {
    std::env::var("OPENMLS_RUNNER_HTTP_CONNECT_TIMEOUT_MS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(DEFAULT_RUNNER_HTTP_CONNECT_TIMEOUT_MS)
}

fn runner_http_request_timeout_ms() -> u64 {
    std::env::var("OPENMLS_RUNNER_HTTP_REQUEST_TIMEOUT_MS")
        .ok()
        .and_then(|value| value.parse().ok())
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

    retry_transient_http_async("ds.health", None, &url, || async {
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
            verify_publication_profiling_capabilities(http, workers, max_parallelism).await?;
            let (p50, p95, p99, max) = latency_percentiles(latencies);
            eprintln!(
                "[preflight] all {} workers are healthy after {}",
                workers.len(),
                format_hms(start.elapsed())
            );
            emit_network_metrics(NetworkPhaseMetrics {
                phase: "preflight".to_string(),
                group_size: workers.len(),
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
                worker_latency_p50_ms: p50,
                worker_latency_p95_ms: p95,
                worker_latency_p99_ms: p99,
                worker_latency_max_ms: max,
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

    let examples: Vec<String> = remaining
        .iter()
        .take(25)
        .map(|&idx| {
            let worker = &workers[idx];
            format!("{}={}", worker.id, worker.url)
        })
        .collect();

    emit_network_metrics(NetworkPhaseMetrics {
        phase: "preflight".to_string(),
        group_size: workers.len(),
        operation: "worker_health".to_string(),
        request_count: workers.len(),
        recipient_count: workers.len(),
        success_count: workers.len().saturating_sub(remaining.len()),
        failure_count: remaining.len(),
        timeout_count: 0,
        connect_error_count: 0,
        max_parallelism,
        effective_parallelism: max_parallelism.min(workers.len()),
        wall_ms: start.elapsed().as_millis(),
        retry_count: 0,
        retry_sleep_ms: 0,
        retry_pass_count: 0,
        failures: remaining.len(),
        worker_latency_p50_ms: None,
        worker_latency_p95_ms: None,
        worker_latency_p99_ms: None,
        worker_latency_max_ms: None,
        slowest_worker_ids: remaining
            .iter()
            .take(5)
            .map(|&idx| workers[idx].id.clone())
            .collect(),
        logical_request_count: workers.len(),
        physical_request_count: workers.len(),
        singleton_request_count: workers.iter().filter(|w| w.profile_enabled).count(),
        packed_request_count: workers.iter().filter(|w| !w.profile_enabled).count(),
        packed_logical_client_count: workers.iter().filter(|w| !w.profile_enabled).count(),
        profile_enabled_recipient_count: workers.iter().filter(|w| w.profile_enabled).count(),
    });

    Err(anyhow!(
        "Timeout waiting for worker readiness after {}. {}/{} workers still unhealthy. Examples: {:?}",
        format_hms(timeout),
        remaining.len(),
        workers.len(),
        examples
    ))
}

async fn verify_publication_profiling_capabilities(
    http: &reqwest::Client,
    workers: &[WorkerSpec],
    max_parallelism: usize,
) -> Result<()> {
    let publication_workers = workers
        .iter()
        .filter(|worker| worker.profile_enabled && !worker.device_kind.is_empty())
        .collect::<Vec<_>>();
    if publication_workers.is_empty() {
        return Ok(());
    }

    let mut probes = stream::iter(publication_workers.into_iter())
        .map(|worker| async move {
            let url = format!(
                "{}/profiling-capabilities",
                worker.url.trim_end_matches('/')
            );
            let response = http.get(&url).send().await.with_context(|| {
                format!("profiling capability request failed for {}", worker.id)
            })?;
            let status = response.status();
            if !status.is_success() {
                return Err(anyhow!(
                    "profiling capability endpoint for {} returned {}",
                    worker.id,
                    status
                ));
            }
            let capabilities = response
                .json::<ProfilingCapabilities>()
                .await
                .with_context(|| {
                    format!("invalid profiling capability response for {}", worker.id)
                })?;
            if capabilities.client_exists == Some(false) {
                return Err(anyhow!(
                    "profiling capability endpoint did not find client {}",
                    worker.id
                ));
            }
            if !capabilities.profiling_enabled {
                return Err(anyhow!(
                    "profiling is disabled for publication worker {}",
                    worker.id
                ));
            }
            if !capabilities.l1d_cache_profiling_enabled {
                eprintln!(
                    "[preflight] L1D profiling is disabled for publication worker {}; continuing without L1D metrics",
                    worker.id
                );
            } else if !capabilities.l1d_cache_counters_available {
                if worker.device_kind.to_lowercase().contains("luckfox") {
                    eprintln!(
                        "[preflight] WARNING: L1D hardware counters are unavailable for publication worker {} ({}); proceeding without L1D metrics for this device",
                        worker.id,
                        worker.device_kind
                    );
                } else {
                    return Err(anyhow!(
                        "L1D hardware counters are unavailable for publication worker {} ({})",
                        worker.id,
                        worker.device_kind
                    ));
                }
            }
            Ok::<_, anyhow::Error>(worker.id.clone())
        })
        .buffer_unordered(max_parallelism.max(1));

    let mut verified = 0usize;
    while let Some(result) = probes.next().await {
        result?;
        verified += 1;
    }
    eprintln!(
        "[preflight] verified profiling capabilities on {} publication workers; L1D counters were required only where enabled",
        verified
    );
    Ok(())
}

#[derive(Debug, Clone)]
struct WorkerCommandContext {
    request_id: String,
    expected_epoch: Option<u64>,
    phase: Option<String>,
    benchmark_plateau_index: Option<usize>,
    benchmark_target_size: Option<usize>,
    benchmark_active_size: Option<usize>,
    benchmark_phase: Option<String>,
    benchmark_operation: Option<String>,
    benchmark_operation_seq: Option<usize>,
    benchmark_payload_size: Option<usize>,
    membership_batch_requested: Option<usize>,
    membership_batch_effective: Option<usize>,
    membership_batch_group_cap: Option<usize>,
    membership_batch_transition_cap: Option<usize>,
    membership_batch_source: Option<String>,
    device_kind: Option<String>,
    execution_backend: Option<String>,
    ciphersuite: Option<String>,
}

#[derive(Serialize)]
struct ProfiledOperationCursorEvent<'a> {
    ts_unix_ns: u128,
    lifecycle: &'a str,
    request_id: &'a str,
    logical_client_id: &'a str,
    physical_worker_id: &'a str,
    command: &'a str,
    benchmark_plateau_index: Option<usize>,
    benchmark_target_size: Option<usize>,
    benchmark_active_size: Option<usize>,
    benchmark_phase: Option<&'a str>,
    benchmark_operation: Option<&'a str>,
    benchmark_operation_seq: Option<usize>,
    benchmark_payload_size: Option<usize>,
}

fn configure_profiled_operation_journal(run_dir: &Path) {
    if let Ok(mut path) = profiled_operation_journal().lock() {
        *path = Some(run_dir.join("profiled-operation-cursors.jsonl"));
    }
}

fn record_profiled_operation_cursor(
    worker: &WorkerSpec,
    command: &Command,
    context: &WorkerCommandContext,
    lifecycle: &str,
) {
    if !worker.profile_enabled || worker.container_mode != ContainerMode::Singleton {
        return;
    }
    let Ok(path_guard) = profiled_operation_journal().lock() else {
        return;
    };
    let Some(path) = path_guard.as_ref() else {
        return;
    };
    let event = ProfiledOperationCursorEvent {
        ts_unix_ns: unix_time_ns(),
        lifecycle,
        request_id: &context.request_id,
        logical_client_id: &worker.id,
        physical_worker_id: &worker.physical_worker_id,
        command: command.kind(),
        benchmark_plateau_index: context.benchmark_plateau_index,
        benchmark_target_size: context.benchmark_target_size,
        benchmark_active_size: context.benchmark_active_size,
        benchmark_phase: context.benchmark_phase.as_deref(),
        benchmark_operation: context.benchmark_operation.as_deref(),
        benchmark_operation_seq: context.benchmark_operation_seq,
        benchmark_payload_size: context.benchmark_payload_size,
    };
    let result = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .and_then(|mut output| {
            serde_json::to_writer(&mut output, &event).map_err(io::Error::other)?;
            writeln!(output)?;
            output.flush()
        });
    if let Err(error) = result {
        eprintln!(
            "[failure-attribution] failed to write profiled operation cursor {}: {}",
            path.display(),
            error
        );
    }
}

impl WorkerCommandContext {
    fn new(worker: &WorkerSpec, command: &Command) -> Self {
        Self::with_metadata(worker, command, None, None, None)
    }

    fn with_metadata(
        worker: &WorkerSpec,
        command: &Command,
        expected_epoch: Option<u64>,
        phase: Option<&str>,
        cursor: Option<&BenchmarkCursor>,
    ) -> Self {
        let seq = WORKER_COMMAND_REQUEST_COUNTER.fetch_add(1, Ordering::Relaxed);
        let request_id = format!(
            "runner-{}-{}-{}-{}",
            std::process::id(),
            worker.id,
            command.kind(),
            seq
        );

        Self {
            request_id,
            expected_epoch,
            phase: phase.map(ToOwned::to_owned),
            benchmark_plateau_index: cursor.map(|c| c.plateau_index),
            benchmark_target_size: cursor.map(|c| c.target_size),
            benchmark_active_size: cursor.map(|c| c.active_size),
            benchmark_phase: cursor.map(|c| c.phase.clone()),
            benchmark_operation: cursor.map(|c| c.operation.clone()),
            benchmark_operation_seq: cursor.and_then(|c| c.operation_seq),
            benchmark_payload_size: cursor.and_then(|c| c.payload_size),
            membership_batch_requested: cursor.and_then(|c| c.membership_batch_requested),
            membership_batch_effective: cursor.and_then(|c| c.membership_batch_effective),
            membership_batch_group_cap: cursor.and_then(|c| c.membership_batch_group_cap),
            membership_batch_transition_cap: cursor.and_then(|c| c.membership_batch_transition_cap),
            membership_batch_source: cursor.and_then(|c| c.membership_batch_source.clone()),
            device_kind: if worker.device_kind.is_empty() {
                None
            } else {
                Some(worker.device_kind.clone())
            },
            execution_backend: None,
            ciphersuite: None,
        }
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

impl WorkerCommandError {
    fn is_ambiguous_transport(&self) -> bool {
        self.classification == WorkerCommandErrorClass::TransportRetryable
    }
}

impl std::fmt::Display for WorkerCommandError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "runner.worker_command failed: worker={} command={} url={} request_id={} attempts={} classification={} last_error={}",
            self.worker_id,
            self.command,
            self.url,
            self.request_id,
            self.attempts,
            self.classification.as_str(),
            self.last_error
        )?;

        if let Some(diagnostic) = &self.diagnostic {
            write!(f, " diagnostics={}", diagnostic)?;
        }

        Ok(())
    }
}

impl StdError for WorkerCommandError {}

async fn record_profiled_worker_failure(
    runner_events: &RunnerEventLog,
    cursor: &BenchmarkCursor,
    worker: &WorkerSpec,
    error: &anyhow::Error,
    action: &str,
    reassigned_to: Option<&WorkerSpec>,
) -> Result<bool> {
    let continue_profiled_failure = should_continue_after_profiled_failure();
    if let Some(evidence) = runner_events.find_oom_evidence(worker).await {
        runner_events.record_oom_failure(
            cursor,
            worker,
            error,
            &evidence,
            action,
            reassigned_to,
        )?;
        let must_stop_profiled_singleton =
            worker.profile_enabled && worker.container_mode == ContainerMode::Singleton;
        return Ok(continue_profiled_failure || !must_stop_profiled_singleton);
    }
    if worker.profile_enabled && worker.container_mode == ContainerMode::Singleton {
        let failure_class = classify_worker_error(error);
        let evidence_detail = format!("{:#}", error);
        runner_events.record_failure(
            cursor,
            worker,
            error,
            failure_class,
            Some("runner_observed_request_failure"),
            Some(&evidence_detail),
            action,
            reassigned_to,
        )?;
        return Ok(continue_profiled_failure);
    }
    Ok(false)
}

fn reqwest_error_diagnostic(err: &reqwest::Error) -> String {
    let mut parts = Vec::new();
    parts.push(format!("top_level={}", err));
    parts.push(format!("is_connect={}", err.is_connect()));
    parts.push(format!("is_timeout={}", err.is_timeout()));
    parts.push(format!("is_request={}", err.is_request()));
    parts.push(format!("is_body={}", err.is_body()));
    parts.push(format!(
        "status={}",
        err.status()
            .map(|status| status.to_string())
            .unwrap_or_else(|| "-".to_string())
    ));

    let inferred_stage = if err.is_connect() {
        "connect"
    } else if err.is_timeout() {
        "timeout while connecting, writing request, or waiting for response"
    } else if err.is_body() {
        "reading response body"
    } else if err.is_request() {
        "writing request or waiting for response headers"
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
        worker.id,
        command_name,
        attempt,
        WORKER_COMMAND_MAX_ATTEMPTS,
        sleep_for.as_millis(),
        url,
        err_text
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
        expected_epoch: context.expected_epoch,
        phase: context.phase.clone(),
        benchmark_plateau_index: context.benchmark_plateau_index,
        benchmark_target_size: context.benchmark_target_size,
        benchmark_active_size: context.benchmark_active_size,
        benchmark_phase: context.benchmark_phase.clone(),
        benchmark_operation: context.benchmark_operation.clone(),
        benchmark_operation_seq: context.benchmark_operation_seq,
        benchmark_payload_size: context.benchmark_payload_size,
        membership_batch_requested: context.membership_batch_requested,
        membership_batch_effective: context.membership_batch_effective,
        membership_batch_group_cap: context.membership_batch_group_cap,
        membership_batch_transition_cap: context.membership_batch_transition_cap,
        membership_batch_source: context.membership_batch_source.clone(),
        device_kind: context.device_kind.clone(),
        execution_backend: context.execution_backend.clone(),
        ciphersuite: context.ciphersuite.clone(),
    };
    record_profiled_operation_cursor(worker, command, context, "started");

    for attempt in 1..=WORKER_COMMAND_MAX_ATTEMPTS {
        let response = match http.post(&url).json(&request).send().await {
            Ok(response) => response,
            Err(err)
                if is_transient_reqwest_error(&err) || is_connect_stage_reqwest_error(&err) =>
            {
                let err_text = err.to_string();
                let diagnostic = reqwest_error_diagnostic(&err);

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
                    diagnostic: Some(reqwest_error_diagnostic(&err)),
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
            Ok(parsed) => {
                record_profiled_operation_cursor(worker, command, context, "completed");
                return Ok(parsed);
            }
            Err(err) if is_transient_reqwest_error(&err) => {
                let err_text = err.to_string();
                let diagnostic = reqwest_error_diagnostic(&err);

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
                    diagnostic: Some(reqwest_error_diagnostic(&err)),
                }
                .into());
            }
        }
    }

    unreachable!("worker command retry loop always returns")
}

async fn send_command(
    http: &reqwest::Client,
    worker: &WorkerSpec,
    command: &Command,
) -> Result<CommandResponse> {
    let context = WorkerCommandContext::new(worker, command);
    send_command_with_context(http, worker, command, &context).await
}

fn worker_command_next_delay(delay: Duration) -> Duration {
    let doubled_ms = delay.as_millis().saturating_mul(2);
    let max_ms = WORKER_COMMAND_MAX_DELAY.as_millis();
    Duration::from_millis(doubled_ms.min(max_ms) as u64)
}

fn worker_command_with_jitter(delay: Duration) -> Duration {
    let base_ms = delay.as_millis() as u64;
    let jitter_cap_ms = (base_ms / 10).clamp(1, 100);
    let jitter_ms = thread_rng().gen_range(0..=jitter_cap_ms);
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

fn is_queued_epoch_race_message(message: &str) -> bool {
    message.contains("lost the epoch race") && message.contains("queued for retry")
}

async fn send_cmd_until_ok(
    http: &reqwest::Client,
    worker: &WorkerSpec,
    command: &Command,
    ok_fragment: &str,
    retryable_error_fragment: &str,
    timeout: Duration,
) -> Result<String> {
    let start = Instant::now();

    while start.elapsed() < timeout {
        let response = send_command(http, worker, command).await?;

        match response.status.as_str() {
            "ok" if response.message.contains(ok_fragment) => return Ok(response.message),
            "ok" => {
                return Err(anyhow!(
                    "Worker {} returned unexpected ok message: {}",
                    worker.id,
                    response.message
                ));
            }
            "error" if response.message.contains(retryable_error_fragment) => {
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
            "error" => {
                return Err(anyhow!("Worker {} error: {}", worker.id, response.message));
            }
            other => {
                return Err(anyhow!(
                    "Worker {} returned unknown status '{}': {}",
                    worker.id,
                    other,
                    response.message
                ));
            }
        }
    }

    Err(anyhow!(
        "Timeout waiting for ok fragment '{}' from worker {}",
        ok_fragment,
        worker.id
    ))
}

fn parse_group_state_message(message: &str) -> Result<GroupStateSnapshot> {
    let msg = message
        .strip_prefix("group_id=")
        .ok_or_else(|| anyhow!("Unexpected show_group_state message: {}", message))?;

    let (group_id, rest) = msg
        .split_once(", epoch=")
        .ok_or_else(|| anyhow!("Missing epoch in show_group_state message: {}", message))?;

    let (epoch_str, members_str) = rest
        .split_once(", members=")
        .ok_or_else(|| anyhow!("Missing members in show_group_state message: {}", message))?;

    let epoch = epoch_str
        .parse::<u64>()
        .with_context(|| format!("Invalid epoch '{}' in '{}'", epoch_str, message))?;

    let mut members: Vec<String> = serde_json::from_str(members_str)
        .with_context(|| format!("Invalid members list '{}' in '{}'", members_str, message))?;

    members.sort();

    Ok(GroupStateSnapshot {
        group_id: group_id.to_string(),
        epoch,
        members,
    })
}

async fn show_group_state(
    http: &reqwest::Client,
    worker: &WorkerSpec,
) -> Result<GroupStateSnapshot> {
    let message =
        send_cmd_expect_ok_fragment(http, worker, &Command::ShowGroupState, "group_id=").await?;
    parse_group_state_message(&message)
}

fn expected_group_state(
    reference: &GroupStateSnapshot,
    epoch: u64,
    mut members: Vec<String>,
) -> ExpectedGroupState {
    members.sort();
    ExpectedGroupState {
        group_id: reference.group_id.clone(),
        epoch,
        members,
    }
}

fn expected_epoch(expected: &ExpectedReceiveCommitState) -> Option<u64> {
    match expected {
        ExpectedReceiveCommitState::Group(state) => Some(state.epoch),
        ExpectedReceiveCommitState::Removed { expected_epoch } => Some(*expected_epoch),
    }
}

async fn receive_commit_reconciled_by_state(
    http: &reqwest::Client,
    worker: &WorkerSpec,
    expected: &ExpectedReceiveCommitState,
    original_error: &str,
) -> Result<bool> {
    match show_group_state(http, worker).await {
        Ok(snapshot) => match expected {
            ExpectedReceiveCommitState::Group(expected_state) if expected_state.matches(&snapshot) => {
                eprintln!(
                    "receive_commit ambiguous transport error reconciled by epoch check: worker={} expected_epoch={} original_error={}",
                    worker.id, expected_state.epoch, original_error
                );
                Ok(true)
            }
            ExpectedReceiveCommitState::Group(expected_state) if snapshot.epoch < expected_state.epoch => {
                eprintln!(
                    "[reconcile] receive_commit worker={} is behind after ambiguous transport error: current_epoch={} expected_epoch={}",
                    worker.id, snapshot.epoch, expected_state.epoch
                );
                Ok(false)
            }
            ExpectedReceiveCommitState::Group(expected_state) => Err(anyhow!(
                "receive_commit ambiguous transport error could not be reconciled for worker {}: expected group_id={} epoch={} members={:?}; got group_id={} epoch={} members={:?}; original_error={}",
                worker.id,
                expected_state.group_id,
                expected_state.epoch,
                expected_state.members,
                snapshot.group_id,
                snapshot.epoch,
                snapshot.members,
                original_error
            )),
            ExpectedReceiveCommitState::Removed { expected_epoch } => Err(anyhow!(
                "receive_commit ambiguous transport error could not be reconciled for removed worker {}: expected removed after epoch {}, but ShowGroupState still returned group_id={} epoch={} members={:?}; original_error={}",
                worker.id,
                expected_epoch,
                snapshot.group_id,
                snapshot.epoch,
                snapshot.members,
                original_error
            )),
        },
        Err(err) => {
            let text = format!("{:#}", err);
            if matches!(expected, ExpectedReceiveCommitState::Removed { .. })
                && text.contains("Client is not in a group")
            {
                eprintln!(
                    "receive_commit ambiguous transport error reconciled by removed-state check: worker={} original_error={}",
                    worker.id, original_error
                );
                return Ok(true);
            }

            Err(anyhow!(
                "receive_commit ambiguous transport error reconciliation could not query worker {} state: {}; original_error={}",
                worker.id,
                text,
                original_error
            ))
        }
    }
}

async fn receive_commit_expect(
    http: &reqwest::Client,
    worker: &WorkerSpec,
    ok_fragment: &str,
    expected: ExpectedReceiveCommitState,
    phase: &str,
    cursor: Option<&BenchmarkCursor>,
) -> Result<()> {
    let command = Command::ReceiveCommit {
        profile: false,
        commit_create_op: None,
        commit_receive_sampling_policy: None,
        commit_receive_sampling_seed: None,
        commit_receive_sample_index: None,
        commit_receive_sample_count: None,
        commit_receive_population_size: None,
    };
    let context = WorkerCommandContext::with_metadata(
        worker,
        &command,
        expected_epoch(&expected),
        Some(phase),
        cursor,
    );

    match send_cmd_expect_ok_fragment_with_context(http, worker, &command, ok_fragment, &context)
        .await
    {
        Ok(_) => return Ok(()),
        Err(err) => {
            if is_resource_budget_failure(&err) {
                return Err(err);
            }

            let is_ambiguous = err
                .downcast_ref::<WorkerCommandError>()
                .map(WorkerCommandError::is_ambiguous_transport)
                .unwrap_or(false);

            if !is_ambiguous {
                return Err(err);
            }

            let original_error = format!("{:#}", err);
            if receive_commit_reconciled_by_state(http, worker, &expected, &original_error).await? {
                return Ok(());
            }

            eprintln!(
                "[reconcile] receive_commit retrying same request_id={} worker={} phase={} after behind-state check",
                context.request_id, worker.id, phase
            );

            match send_cmd_expect_ok_fragment_with_context(
                http,
                worker,
                &command,
                ok_fragment,
                &context,
            )
            .await
            {
                Ok(_) => Ok(()),
                Err(retry_err) => {
                    if is_resource_budget_failure(&retry_err) {
                        return Err(retry_err);
                    }

                    let retry_error = format!("{:#}", retry_err);
                    if receive_commit_reconciled_by_state(http, worker, &expected, &retry_error)
                        .await?
                    {
                        return Ok(());
                    }

                    Err(anyhow!(
                        "receive_commit failed after ambiguous transport reconciliation retry: worker={} request_id={} phase={} initial_error={} retry_error={}",
                        worker.id,
                        context.request_id,
                        phase,
                        original_error,
                        retry_error
                    ))
                }
            }
        }
    }
}

async fn expected_receive_commit_state_reached(
    http: &reqwest::Client,
    worker: &WorkerSpec,
    expected: &ExpectedReceiveCommitState,
) -> Result<bool> {
    match show_group_state(http, worker).await {
        Ok(snapshot) => match expected {
            ExpectedReceiveCommitState::Group(expected_state) => {
                Ok(expected_state.matches(&snapshot))
            }
            ExpectedReceiveCommitState::Removed { expected_epoch } => Err(anyhow!(
                "worker {} was expected to be removed after epoch {}, but ShowGroupState returned group_id={} epoch={} members={:?}",
                worker.id,
                expected_epoch,
                snapshot.group_id,
                snapshot.epoch,
                snapshot.members
            )),
        },
        Err(err) => {
            let text = format!("{:#}", err);
            if matches!(expected, ExpectedReceiveCommitState::Removed { .. })
                && text.contains("Client is not in a group")
            {
                return Ok(true);
            }

            Err(anyhow!(
                "could not query worker {} state after ProcessPending: {}",
                worker.id,
                text
            ))
        }
    }
}

#[allow(dead_code)]
async fn receive_application_message_expect(
    http: &reqwest::Client,
    worker: &WorkerSpec,
    profile: bool,
    phase: &str,
    cursor: Option<&BenchmarkCursor>,
) -> Result<()> {
    let command = Command::ReceiveApplicationMessage { profile };
    let context = WorkerCommandContext::with_metadata(worker, &command, None, Some(phase), cursor);

    match send_cmd_expect_ok_fragment_with_context(
        http,
        worker,
        &command,
        "application message received:",
        &context,
    )
    .await
    {
        Ok(_) => Ok(()),
        Err(err) => {
            let is_ambiguous = err
                .downcast_ref::<WorkerCommandError>()
                .map(WorkerCommandError::is_ambiguous_transport)
                .unwrap_or(false);

            if !is_ambiguous {
                return Err(err);
            }

            let original_error = format!("{:#}", err);
            eprintln!(
                "[reconcile] receive_application_message retrying same request_id={} worker={} phase={} after ambiguous transport error={}",
                context.request_id, worker.id, phase, original_error
            );

            send_cmd_expect_ok_fragment_with_context(
                http,
                worker,
                &command,
                "application message received:",
                &context,
            )
            .await
            .map(|_| ())
            .with_context(|| {
                format!(
                    "receive_application_message failed after ambiguous transport retry: worker={} request_id={} phase={} initial_error={}",
                    worker.id, context.request_id, phase, original_error
                )
            })
        }
    }
}

async fn process_pending_commit_expect(
    http: &reqwest::Client,
    worker: &WorkerSpec,
    expected: ExpectedReceiveCommitState,
    phase: &str,
    cursor: Option<&BenchmarkCursor>,
) -> Result<()> {
    let command = Command::ProcessPending {
        kinds: Some(vec![PendingKind::Commits]),
        max_messages: Some(8),
        expected_epoch: expected_epoch(&expected),
        profile: false,
        commit_create_op: None,
        commit_receive_sampling_policy: None,
        commit_receive_sampling_seed: None,
        commit_receive_sample_index: None,
        commit_receive_sample_count: None,
        commit_receive_population_size: None,
    };
    let context = WorkerCommandContext::with_metadata(
        worker,
        &command,
        expected_epoch(&expected),
        Some(phase),
        cursor,
    );

    match send_cmd_expect_ok_fragment_with_context(
        http,
        worker,
        &command,
        "process_pending processed;",
        &context,
    )
    .await
    {
        Ok(_) => {
            if expected_receive_commit_state_reached(http, worker, &expected).await? {
                Ok(())
            } else {
                Err(anyhow!(
                    "process_pending completed but worker {} did not reach expected state",
                    worker.id
                ))
            }
        }
        Err(err) => {
            if is_resource_budget_failure(&err) {
                return Err(err);
            }

            let original_error = format!("{:#}", err);
            if receive_commit_reconciled_by_state(http, worker, &expected, &original_error).await? {
                return Ok(());
            }

            eprintln!(
                "[reconcile] process_pending retrying same request_id={} worker={} phase={} after failure",
                context.request_id, worker.id, phase
            );

            match send_cmd_expect_ok_fragment_with_context(
                http,
                worker,
                &command,
                "process_pending processed;",
                &context,
            )
            .await
            {
                Ok(_) => {
                    if expected_receive_commit_state_reached(http, worker, &expected).await? {
                        Ok(())
                    } else {
                        Err(anyhow!(
                            "process_pending retry completed but worker {} did not reach expected state",
                            worker.id
                        ))
                    }
                }
                Err(retry_err) => {
                    if is_resource_budget_failure(&retry_err) {
                        return Err(retry_err);
                    }

                    let retry_error = format!("{:#}", retry_err);
                    if receive_commit_reconciled_by_state(http, worker, &expected, &retry_error)
                        .await?
                    {
                        return Ok(());
                    }

                    Err(anyhow!(
                        "process_pending failed after reconciliation retry: worker={} request_id={} phase={} initial_error={} retry_error={}",
                        worker.id,
                        context.request_id,
                        phase,
                        original_error,
                        retry_error
                    ))
                }
            }
        }
    }
}

async fn ensure_converged(
    http: &reqwest::Client,
    active_workers: &[WorkerSpec],
    expected_active_ids: &[String],
    max_parallelism: usize,
) -> Result<GroupStateSnapshot> {
    if active_workers.is_empty() {
        return Err(anyhow!("No active workers to verify"));
    }

    let mut expected_members = expected_active_ids.to_vec();
    expected_members.sort();

    let states = collect_worker_group_states(http, active_workers, max_parallelism).await?;
    let (reference_worker, reference) = states
        .first()
        .ok_or_else(|| anyhow!("No active workers to verify"))?;

    if reference.members != expected_members {
        return Err(anyhow!(
            "Reference worker {} member list mismatch. Expected {:?}, got {:?}",
            reference_worker.id,
            expected_members,
            reference.members
        ));
    }

    for (worker, state) in states.iter().skip(1) {
        if state.group_id != reference.group_id
            || state.epoch != reference.epoch
            || state.members != reference.members
        {
            return Err(anyhow!(
                "Convergence mismatch on worker {}. Expected group_id={}, epoch={}, members={:?}; got group_id={}, epoch={}, members={:?}",
                worker.id,
                reference.group_id,
                reference.epoch,
                reference.members,
                state.group_id,
                state.epoch,
                state.members
            ));
        }
    }

    Ok(reference.clone())
}

async fn ensure_converged_with_attrition(
    http: &reqwest::Client,
    active: &mut Vec<WorkerSpec>,
    fanout: &mut FanoutController,
    process_pending_fanout: bool,
    plateau_size: usize,
    max_commit_receive_samples_per_plateau: usize,
    commit_receive_sampling_seed: u64,
    max_parallelism: usize,
    plateau_index: usize,
    runner_events: &RunnerEventLog,
) -> Result<GroupStateSnapshot> {
    loop {
        let active_ids: Vec<String> = active.iter().map(|w| w.id.clone()).collect();
        let result = ensure_converged(http, active, &active_ids, max_parallelism).await;
        match result {
            Ok(state) => return Ok(state),
            Err(error) => {
                let cursor = BenchmarkCursor::new(
                    plateau_index,
                    plateau_size,
                    active.len(),
                    "convergence",
                    "show_group_state",
                );
                let Some(dead_workers) = record_batch_oom_failures(
                    runner_events,
                    &cursor,
                    &error,
                    "evict_convergence_member_and_retry",
                    active.first(),
                )
                .await?
                else {
                    return Err(error);
                };
                evict_oom_group_members(
                    http,
                    active,
                    &dead_workers,
                    fanout,
                    process_pending_fanout,
                    plateau_size,
                    max_commit_receive_samples_per_plateau,
                    commit_receive_sampling_seed,
                    runner_events,
                )
                .await?;
            }
        }
    }
}

async fn collect_worker_group_states(
    http: &reqwest::Client,
    workers: &[WorkerSpec],
    max_parallelism: usize,
) -> Result<Vec<(WorkerSpec, GroupStateSnapshot)>> {
    let max_parallelism = max_parallelism.max(1);
    let collect = fanout_collect_workers(
        "convergence",
        workers.len(),
        "show_group_state",
        workers,
        max_parallelism,
        |worker| async move { show_group_state(http, &worker).await },
    )
    .await;

    emit_fanout_metrics(
        "convergence",
        workers.len(),
        "show_group_state",
        &collect.summary,
    );

    if !collect.failures.is_empty() {
        return Err(BatchFanoutError {
            phase: "convergence".to_string(),
            operation: "show_group_state".to_string(),
            failures: collect.failures,
        }
        .into());
    }

    let mut by_id = HashMap::with_capacity(collect.successes.len());
    for (worker, state) in collect.successes {
        by_id.insert(worker.id.clone(), (worker, state));
    }

    let mut ordered = Vec::with_capacity(workers.len());
    for worker in workers {
        let state = by_id
            .remove(&worker.id)
            .ok_or_else(|| anyhow!("Missing ShowGroupState result for worker {}", worker.id))?;
        ordered.push(state);
    }

    Ok(ordered)
}

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

#[allow(dead_code)]
async fn fanout_workers<F, Fut>(
    phase: &str,
    group_size: usize,
    operation: &str,
    workers: &[WorkerSpec],
    fanout: &mut FanoutController,
    op: F,
) -> Result<()>
where
    F: Fn(WorkerSpec) -> Fut,
    Fut: Future<Output = Result<()>>,
{
    let max_parallelism = fanout.parallelism();
    let collect =
        fanout_collect_workers(phase, group_size, operation, workers, max_parallelism, op).await;

    emit_fanout_metrics(phase, group_size, operation, &collect.summary);
    fanout.record(phase, operation, &collect.summary);

    if collect.failures.is_empty() {
        return Ok(());
    }

    Err(anyhow!(
        "fanout phase={} operation={} failed_workers={} max_parallelism={} failures=[{}]",
        phase,
        operation,
        collect.failures.len(),
        max_parallelism,
        format_fanout_failures(&collect.failures)
    ))
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
        write!(
            formatter,
            "batch_fanout phase={} operation={} failed_workers={} failures=[{}]",
            self.phase,
            self.operation,
            self.failures.len(),
            format_fanout_failures(&self.failures)
        )
    }
}

impl StdError for BatchFanoutError {}

async fn record_batch_oom_failures(
    runner_events: &RunnerEventLog,
    cursor: &BenchmarkCursor,
    error: &anyhow::Error,
    action: &str,
    reassigned_to: Option<&WorkerSpec>,
) -> Result<Option<Vec<WorkerSpec>>> {
    let Some(batch_error) = error.downcast_ref::<BatchFanoutError>() else {
        return Ok(None);
    };
    let mut dead_workers = Vec::new();
    for failure in &batch_error.failures {
        if !record_profiled_worker_failure(
            runner_events,
            cursor,
            &failure.worker,
            &failure.error,
            action,
            reassigned_to,
        )
        .await?
        {
            return Ok(None);
        }
        dead_workers.push(failure.worker.clone());
    }
    Ok(Some(dead_workers))
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

fn is_resource_budget_failure(error: &anyhow::Error) -> bool {
    matches!(
        classify_worker_error(error),
        "app_heap_budget_exceeded"
            | "app_heap_budget_allocator_abort"
            | "embedded_budget_timeout"
            | "cpu_starvation_timeout"
    )
}

fn is_profiled_resource_budget_failure(worker: &WorkerSpec, error: &anyhow::Error) -> bool {
    worker.profile_enabled
        && worker.container_mode == ContainerMode::Singleton
        && is_resource_budget_failure(error)
}

#[derive(Debug)]
struct FanoutAttempt<T> {
    worker: WorkerSpec,
    latency_ms: u128,
    result: Result<T>,
}

#[derive(Debug)]
struct FanoutCollect<T> {
    successes: Vec<(WorkerSpec, T)>,
    failures: Vec<FanoutFailure>,
    summary: FanoutSummary,
}

async fn fanout_collect_workers<F, Fut, T>(
    phase: &str,
    group_size: usize,
    operation: &str,
    workers: &[WorkerSpec],
    max_parallelism: usize,
    op: F,
) -> FanoutCollect<T>
where
    F: Fn(WorkerSpec) -> Fut,
    Fut: Future<Output = Result<T>>,
{
    let started = Instant::now();
    let max_parallelism = max_parallelism.max(1);
    let mut request_count = 0usize;
    let mut retry_pass_count = 0usize;
    let mut effective_parallelism = 0usize;
    let mut latencies = Vec::new();
    let mut successes = Vec::with_capacity(workers.len());

    let first_pass = run_fanout_pass(workers, max_parallelism, &op).await;
    request_count += first_pass.0.len();
    effective_parallelism = effective_parallelism.max(first_pass.1);

    let mut retry_workers = Vec::new();
    for attempt in first_pass.0 {
        latencies.push((attempt.worker.id.clone(), attempt.latency_ms));
        match attempt.result {
            Ok(value) => successes.push((attempt.worker, value)),
            Err(error) => retry_workers.push(FanoutFailure {
                worker: attempt.worker,
                error,
            }),
        }
    }

    for retry_pass in 1..=DEFAULT_FANOUT_RETRY_PASSES {
        if retry_workers.is_empty() {
            break;
        }

        eprintln!(
            "[fanout-retry] phase={} group_size={} operation={} pass={} retry_workers={}",
            phase,
            group_size,
            operation,
            retry_pass,
            retry_workers.len()
        );

        retry_pass_count += 1;
        let retry_inputs = retry_workers
            .iter()
            .map(|failure| failure.worker.clone())
            .collect::<Vec<_>>();
        retry_workers.clear();

        let retry_pass = run_fanout_pass(&retry_inputs, max_parallelism, &op).await;
        request_count += retry_pass.0.len();
        effective_parallelism = effective_parallelism.max(retry_pass.1);

        for attempt in retry_pass.0 {
            latencies.push((attempt.worker.id.clone(), attempt.latency_ms));
            match attempt.result {
                Ok(value) => successes.push((attempt.worker, value)),
                Err(error) => retry_workers.push(FanoutFailure {
                    worker: attempt.worker,
                    error,
                }),
            }
        }
    }

    let failures = retry_workers;

    let latency_values = latencies.iter().map(|(_, ms)| *ms).collect::<Vec<_>>();
    let (p50, p95, p99, max) = latency_percentiles(latency_values);
    latencies.sort_by(|left, right| right.1.cmp(&left.1));
    let slowest_worker_ids = latencies
        .iter()
        .take(5)
        .map(|(worker_id, latency_ms)| format!("{}:{}ms", worker_id, latency_ms))
        .collect::<Vec<_>>();

    let (timeout_count, connect_error_count) = classify_fanout_failures(&failures);
    let summary = FanoutSummary {
        request_count,
        recipient_count: workers.len(),
        success_count: successes.len(),
        failure_count: failures.len(),
        timeout_count,
        connect_error_count,
        max_parallelism,
        effective_parallelism,
        retry_pass_count,
        wall_ms: started.elapsed().as_millis(),
        latency_p50_ms: p50,
        latency_p95_ms: p95,
        latency_p99_ms: p99,
        latency_max_ms: max,
        slowest_worker_ids,
    };

    FanoutCollect {
        successes,
        failures,
        summary,
    }
}

async fn run_fanout_pass<F, Fut, T>(
    workers: &[WorkerSpec],
    max_parallelism: usize,
    op: &F,
) -> (Vec<FanoutAttempt<T>>, usize)
where
    F: Fn(WorkerSpec) -> Fut,
    Fut: Future<Output = Result<T>>,
{
    let in_flight = Arc::new(AtomicUsize::new(0));
    let max_in_flight = Arc::new(AtomicUsize::new(0));

    let mut results = stream::iter(workers.iter().cloned())
        .map(|worker| {
            let worker_for_result = worker.clone();
            let future = op(worker);
            let in_flight = Arc::clone(&in_flight);
            let max_in_flight = Arc::clone(&max_in_flight);

            async move {
                let command_started = Instant::now();
                let current = in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                update_atomic_max(&max_in_flight, current);
                let result = future.await;
                in_flight.fetch_sub(1, Ordering::SeqCst);

                FanoutAttempt {
                    worker: worker_for_result,
                    latency_ms: command_started.elapsed().as_millis(),
                    result,
                }
            }
        })
        .buffer_unordered(max_parallelism.max(1));

    let mut attempts = Vec::with_capacity(workers.len());
    while let Some(attempt) = results.next().await {
        attempts.push(attempt);
    }

    (attempts, max_in_flight.load(Ordering::SeqCst))
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchFanoutCommand {
    pub client_id: String,
    #[serde(default)]
    pub request_id: Option<String>,
    pub command: Command,
    pub expected_epoch: Option<u64>,
    pub phase: Option<String>,
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
    pub membership_batch_requested: Option<usize>,
    #[serde(default)]
    pub membership_batch_effective: Option<usize>,
    #[serde(default)]
    pub membership_batch_group_cap: Option<usize>,
    #[serde(default)]
    pub membership_batch_transition_cap: Option<usize>,
    #[serde(default)]
    pub membership_batch_source: Option<String>,
}

impl BatchFanoutCommand {
    fn with_benchmark_cursor(mut self, cursor: &BenchmarkCursor) -> Self {
        self.benchmark_plateau_index = Some(cursor.plateau_index);
        self.benchmark_target_size = Some(cursor.target_size);
        self.benchmark_active_size = Some(cursor.active_size);
        self.benchmark_phase = Some(cursor.phase.clone());
        self.benchmark_operation = Some(cursor.operation.clone());
        self.benchmark_operation_seq = cursor.operation_seq;
        self.benchmark_payload_size = cursor.payload_size;
        self.membership_batch_requested = cursor.membership_batch_requested;
        self.membership_batch_effective = cursor.membership_batch_effective;
        self.membership_batch_group_cap = cursor.membership_batch_group_cap;
        self.membership_batch_transition_cap = cursor.membership_batch_transition_cap;
        self.membership_batch_source = cursor.membership_batch_source.clone();
        self
    }
}

pub fn build_batch_commands<F>(
    workers: &[WorkerSpec],
    mut command_for: F,
) -> Vec<(String, Vec<BatchFanoutCommand>)>
where
    F: FnMut(&WorkerSpec) -> BatchFanoutCommand,
{
    let groups = physical_groups(workers.iter());
    let mut result: Vec<(String, Vec<BatchFanoutCommand>)> = Vec::new();
    for (physical_id, group) in groups {
        let cmds: Vec<BatchFanoutCommand> = group
            .iter()
            .map(|w| {
                let mut command = command_for(*w);
                if command.request_id.is_none() {
                    command.request_id = Some(batch_command_request_id(*w, &command));
                }
                command
            })
            .collect();
        result.push((physical_id, cmds));
    }
    result
}

fn batch_command_request_id(worker: &WorkerSpec, command: &BatchFanoutCommand) -> String {
    WorkerCommandContext::with_metadata(
        worker,
        &command.command,
        command.expected_epoch,
        command.phase.as_deref(),
        None,
    )
    .request_id
}

async fn batch_fanout_workers(
    http: &reqwest::Client,
    phase: &str,
    group_size: usize,
    operation: &str,
    workers: &[WorkerSpec],
    fanout: &mut FanoutController,
    commands_by_physical: &[(String, Vec<BatchFanoutCommand>)],
    expected_by_client: Option<&HashMap<String, ExpectedReceiveCommitState>>,
) -> Result<()> {
    let max_parallelism = fanout.parallelism();

    let started = Instant::now();
    let mut all_successes: Vec<(WorkerSpec, ())> = Vec::new();
    let mut all_failures: Vec<FanoutFailure> = Vec::new();
    let mut all_latencies: Vec<(String, u128)> = Vec::new();
    let mut request_count = 0usize;
    let mut retry_pass_count = 0usize;
    let mut total_timeout_count = 0usize;
    let mut total_connect_error_count = 0usize;
    let mut effective_parallelism = 0usize;

    let mut retry_commands: Vec<(String, Vec<BatchFanoutCommand>)> =
        commands_by_physical.iter().cloned().collect();

    for retry_pass in 0..=DEFAULT_FANOUT_RETRY_PASSES {
        if retry_commands.is_empty() {
            break;
        }

        if retry_pass > 0 {
            eprintln!(
                "[batch-fanout-retry] phase={} operation={} pass={} retry_physical_workers={}",
                phase,
                operation,
                retry_pass,
                retry_commands.len()
            );
        }

        let pass = batch_fanout_collect_pass(http, workers, max_parallelism, &retry_commands).await;

        request_count += pass.request_count;
        effective_parallelism = effective_parallelism.max(pass.effective_parallelism);
        if retry_pass > 0 {
            retry_pass_count += 1;
        }

        let mut pass_failures = pass.failures;
        let mut reconciled_successes = Vec::new();
        if let Some(expected_by_client) = expected_by_client {
            let reconciled =
                reconcile_batch_receive_failures(http, pass_failures, expected_by_client).await;
            reconciled_successes = reconciled.0;
            pass_failures = reconciled.1;
        }

        let (classified_timeout_count, classified_connect_error_count) =
            classify_fanout_failures(&pass_failures);
        total_timeout_count += pass.timeout_count.max(classified_timeout_count);
        total_connect_error_count += pass.connect_error_count.max(classified_connect_error_count);

        let still_failing = retry_batch_commands_for_failures(&retry_commands, &pass_failures);
        all_latencies.extend(pass.latencies);
        all_successes.extend(pass.successes);
        all_successes.extend(reconciled_successes);
        all_failures = pass_failures;
        retry_commands = still_failing;
    }

    let wall_ms = started.elapsed().as_millis();
    let latency_values = all_latencies.iter().map(|(_, lat)| *lat).collect();
    let (p50, p95, p99, max_lat) = latency_percentiles(latency_values);

    let mut latencies_with_ids = all_latencies.clone();
    latencies_with_ids.sort_by(|a, b| b.1.cmp(&a.1));
    let slowest_worker_ids = latencies_with_ids
        .iter()
        .take(5)
        .map(|(id, lat)| format!("{}:{}ms", id, lat))
        .collect::<Vec<_>>();

    let summary = FanoutSummary {
        request_count,
        recipient_count: workers.len(),
        success_count: all_successes.len(),
        failure_count: all_failures.len(),
        timeout_count: total_timeout_count,
        connect_error_count: total_connect_error_count,
        max_parallelism,
        effective_parallelism,
        retry_pass_count,
        wall_ms,
        latency_p50_ms: p50,
        latency_p95_ms: p95,
        latency_p99_ms: p99,
        latency_max_ms: max_lat,
        slowest_worker_ids,
    };

    emit_fanout_metrics(phase, group_size, operation, &summary);
    fanout.record(phase, operation, &summary);

    if all_failures.is_empty() {
        return Ok(());
    }

    Err(BatchFanoutError {
        phase: phase.to_string(),
        operation: operation.to_string(),
        failures: all_failures,
    }
    .into())
}

async fn reconcile_batch_receive_failures(
    http: &reqwest::Client,
    failures: Vec<FanoutFailure>,
    expected_by_client: &HashMap<String, ExpectedReceiveCommitState>,
) -> (Vec<(WorkerSpec, ())>, Vec<FanoutFailure>) {
    let mut reconciled_ids = HashSet::new();
    let mut remaining = Vec::new();

    for failure in failures {
        let Some(expected) = expected_by_client.get(&failure.worker.id) else {
            remaining.push(failure);
            continue;
        };

        if is_profiled_resource_budget_failure(&failure.worker, &failure.error) {
            remaining.push(failure);
            continue;
        }

        let original_error = format!("{:#}", failure.error);
        match receive_commit_reconciled_by_state(http, &failure.worker, expected, &original_error)
            .await
        {
            Ok(true) => {
                reconciled_ids.insert(failure.worker.id.clone());
                remaining.push(failure);
            }
            Ok(false) => remaining.push(failure),
            Err(err) => {
                let text = format!("{:#}", err);
                let error = if text.contains("could not query worker") {
                    failure.error
                } else {
                    anyhow!(
                        "batch receive reconciliation failed: {}; original_error={}",
                        text,
                        original_error
                    )
                };
                remaining.push(FanoutFailure {
                    worker: failure.worker,
                    error,
                });
            }
        }
    }

    let (successes, remaining) =
        partition_batch_failures_by_reconciled_state(remaining, &reconciled_ids);
    (successes, remaining)
}

fn partition_batch_failures_by_reconciled_state(
    failures: Vec<FanoutFailure>,
    reconciled_ids: &HashSet<String>,
) -> (Vec<(WorkerSpec, ())>, Vec<FanoutFailure>) {
    let mut successes = Vec::new();
    let mut remaining = Vec::new();

    for failure in failures {
        if reconciled_ids.contains(&failure.worker.id) {
            successes.push((failure.worker, ()));
        } else {
            remaining.push(failure);
        }
    }

    (successes, remaining)
}

fn retry_batch_commands_for_failures(
    commands_by_physical: &[(String, Vec<BatchFanoutCommand>)],
    failures: &[FanoutFailure],
) -> Vec<(String, Vec<BatchFanoutCommand>)> {
    let failed_ids: HashSet<&str> = failures
        .iter()
        .map(|failure| failure.worker.id.as_str())
        .collect();

    commands_by_physical
        .iter()
        .filter_map(|(physical_id, cmds)| {
            let retry_cmds = cmds
                .iter()
                .filter(|cmd| failed_ids.contains(cmd.client_id.as_str()))
                .cloned()
                .collect::<Vec<_>>();

            if retry_cmds.is_empty() {
                None
            } else {
                Some((physical_id.clone(), retry_cmds))
            }
        })
        .collect()
}

struct BatchFanoutCollectPass {
    successes: Vec<(WorkerSpec, ())>,
    failures: Vec<FanoutFailure>,
    latencies: Vec<(String, u128)>,
    request_count: usize,
    timeout_count: usize,
    connect_error_count: usize,
    effective_parallelism: usize,
}

struct BatchPhysicalAttempt {
    physical_id: String,
    successes: Vec<(WorkerSpec, ())>,
    failures: Vec<FanoutFailure>,
    latency_ms: u128,
    request_count: usize,
    timeout_count: usize,
    connect_error_count: usize,
}

async fn batch_fanout_collect_pass(
    http: &reqwest::Client,
    workers: &[WorkerSpec],
    max_parallelism: usize,
    commands_by_physical: &[(String, Vec<BatchFanoutCommand>)],
) -> BatchFanoutCollectPass {
    let mut failures: Vec<FanoutFailure> = Vec::new();
    let mut successes: Vec<(WorkerSpec, ())> = Vec::new();
    let mut latencies: Vec<(String, u128)> = Vec::new();
    let mut request_count = 0usize;
    let mut timeout_count = 0usize;
    let mut connect_error_count = 0usize;

    let workers_by_id = Arc::new(
        workers
            .iter()
            .map(|worker| (worker.id.clone(), worker.clone()))
            .collect::<HashMap<_, _>>(),
    );
    let in_flight = Arc::new(AtomicUsize::new(0));
    let max_in_flight = Arc::new(AtomicUsize::new(0));

    let mut attempts = stream::iter(commands_by_physical.iter().cloned())
        .map(|(physical_id, cmds)| {
            let http = http.clone();
            let workers_by_id = Arc::clone(&workers_by_id);
            let in_flight = Arc::clone(&in_flight);
            let max_in_flight = Arc::clone(&max_in_flight);

            async move {
                let current = in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                update_atomic_max(&max_in_flight, current);
                let attempt =
                    batch_fanout_collect_physical(&http, &workers_by_id, physical_id, cmds).await;
                in_flight.fetch_sub(1, Ordering::SeqCst);
                attempt
            }
        })
        .buffer_unordered(max_parallelism.max(1));

    while let Some(attempt) = attempts.next().await {
        request_count += attempt.request_count;
        timeout_count += attempt.timeout_count;
        connect_error_count += attempt.connect_error_count;
        latencies.push((attempt.physical_id, attempt.latency_ms));
        successes.extend(attempt.successes);
        failures.extend(attempt.failures);
    }

    BatchFanoutCollectPass {
        successes,
        failures,
        latencies,
        request_count,
        timeout_count,
        connect_error_count,
        effective_parallelism: max_in_flight.load(Ordering::SeqCst),
    }
}

async fn batch_fanout_collect_physical(
    http: &reqwest::Client,
    workers_by_id: &HashMap<String, WorkerSpec>,
    physical_id: String,
    cmds: Vec<BatchFanoutCommand>,
) -> BatchPhysicalAttempt {
    let physical_url = batch_physical_base_url(&physical_id, &cmds, workers_by_id);
    let batch_url = format!("{}/batch-command", physical_url.trim_end_matches('/'));
    let logical_count = cmds.len();

    let batch_items: Vec<BatchCommandItem> = cmds.iter().map(batch_command_item).collect();

    let batch_req = BatchCommandRequest { items: batch_items };
    let attempt_start = Instant::now();
    let result = http.post(&batch_url).json(&batch_req).send().await;
    let latency_ms = attempt_start.elapsed().as_millis();

    let mut failures = Vec::new();
    let mut successes = Vec::new();
    let mut timeout_count = 0usize;
    let mut connect_error_count = 0usize;

    match result {
        Ok(response) => {
            if response.status().is_success() {
                match response.json::<BatchCommandResponse>().await {
                    Ok(batch_resp) => {
                        for item_result in &batch_resp.items {
                            if item_result.response.status == "ok" {
                                if let Some(w) = workers_by_id.get(&item_result.client_id) {
                                    successes.push((w.clone(), ()));
                                }
                            } else if let Some(w) = workers_by_id.get(&item_result.client_id) {
                                failures.push(FanoutFailure {
                                    worker: w.clone(),
                                    error: anyhow!(
                                        "client {} batch error: {}",
                                        item_result.client_id,
                                        item_result.response.message
                                    ),
                                });
                            }
                        }
                    }
                    Err(err) => {
                        for c in &cmds {
                            if let Some(w) = workers_by_id.get(&c.client_id) {
                                failures.push(FanoutFailure {
                                    worker: w.clone(),
                                    error: anyhow!("batch response parse error: {}", err),
                                });
                            }
                        }
                    }
                }
            } else {
                let status = response.status();
                let body = response.text().await.unwrap_or_default();
                for c in &cmds {
                    if let Some(w) = workers_by_id.get(&c.client_id) {
                        failures.push(FanoutFailure {
                            worker: w.clone(),
                            error: anyhow!("batch HTTP error: {} {}", status, body),
                        });
                    }
                }
            }
        }
        Err(err) => {
            let err_text = err.to_string();
            let is_timeout = err_text.contains("timeout")
                || err_text.contains("timed out")
                || err_text.contains("deadline");
            let is_connect = err_text.contains("unreachable")
                || err_text.contains("connect")
                || err_text.contains("refused");

            if is_timeout {
                timeout_count += logical_count;
            }
            if is_connect {
                connect_error_count += logical_count;
            }
            for c in &cmds {
                if let Some(w) = workers_by_id.get(&c.client_id) {
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
        timeout_count,
        connect_error_count,
    }
}

fn batch_command_item(command: &BatchFanoutCommand) -> BatchCommandItem {
    BatchCommandItem {
        client_id: command.client_id.clone(),
        request_id: command.request_id.clone(),
        command: command.command.clone(),
        expected_epoch: command.expected_epoch,
        phase: command.phase.clone(),
        profile: command.profile,
        benchmark_plateau_index: command.benchmark_plateau_index,
        benchmark_target_size: command.benchmark_target_size,
        benchmark_active_size: command.benchmark_active_size,
        benchmark_phase: command.benchmark_phase.clone(),
        benchmark_operation: command.benchmark_operation.clone(),
        benchmark_operation_seq: command.benchmark_operation_seq,
        benchmark_payload_size: command.benchmark_payload_size,
        membership_batch_requested: command.membership_batch_requested,
        membership_batch_effective: command.membership_batch_effective,
        membership_batch_group_cap: command.membership_batch_group_cap,
        membership_batch_transition_cap: command.membership_batch_transition_cap,
        membership_batch_source: command.membership_batch_source.clone(),
        device_kind: None,
        execution_backend: None,
        ciphersuite: None,
    }
}

fn batch_physical_base_url(
    physical_id: &str,
    cmds: &[BatchFanoutCommand],
    workers_by_id: &HashMap<String, WorkerSpec>,
) -> String {
    if let Some(worker) = cmds
        .iter()
        .find_map(|cmd| workers_by_id.get(&cmd.client_id))
    {
        if let Some((base, _)) = worker.url.split_once("/client/") {
            return base.trim_end_matches('/').to_string();
        }

        return worker.url.trim_end_matches('/').to_string();
    }

    format!("http://{}:8080", physical_id)
}

fn classify_fanout_failures(failures: &[FanoutFailure]) -> (usize, usize) {
    failures
        .iter()
        .fold((0usize, 0usize), |(timeouts, connects), failure| {
            let lower = format!("{:#}", failure.error).to_ascii_lowercase();
            let is_timeout = lower.contains("timeout")
                || lower.contains("timed out")
                || lower.contains("deadline has elapsed");
            let is_connect = lower.contains("host is unreachable")
                || lower.contains("no route to host")
                || lower.contains("network is unreachable")
                || lower.contains("tcp connect")
                || lower.contains("connect error")
                || lower.contains("failed to connect")
                || lower.contains("dns error")
                || lower.contains("failed to resolve");

            (
                timeouts + usize::from(is_timeout),
                connects + usize::from(is_connect),
            )
        })
}

fn format_fanout_failures(failures: &[FanoutFailure]) -> String {
    failures
        .iter()
        .map(|failure| format!("{}: {:#}", failure.worker.id, failure.error))
        .collect::<Vec<_>>()
        .join("; ")
}

fn emit_fanout_metrics(phase: &str, group_size: usize, operation: &str, summary: &FanoutSummary) {
    emit_network_metrics(NetworkPhaseMetrics {
        phase: phase.to_string(),
        group_size,
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

fn stepped_sizes(min_size: usize, max_size: usize, step_size: usize) -> Vec<usize> {
    let mut sizes = Vec::new();
    let mut current = min_size;

    sizes.push(current);
    while current < max_size {
        let next = current.saturating_add(step_size);
        current = next.min(max_size);
        if sizes.last().copied() != Some(current) {
            sizes.push(current);
        }
    }

    sizes
}

fn build_plateau_sequence(
    min_size: usize,
    max_size: usize,
    step_size: usize,
    roundtrips: usize,
) -> Vec<usize> {
    let ascent = stepped_sizes(min_size, max_size, step_size);
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
    rng: &mut R,
) -> Vec<usize> {
    if let StepSize::Fixed(step_size) = step_size {
        return build_plateau_sequence(min_size, max_size, *step_size, roundtrips);
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
    rng: &mut R,
) -> Vec<usize> {
    if let StepSize::Fixed(step_size) = step_size {
        return stepped_sizes(min_size, max_size, *step_size);
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
    rng: &mut R,
) -> Vec<usize> {
    if plateau_order == PlateauOrder::Staircase {
        return build_plateau_sequence_for_step_size(
            min_size, max_size, step_size, roundtrips, rng,
        );
    }
    if plateau_order == PlateauOrder::Ascending {
        return build_ascending_plateau_sequence_for_step_size(min_size, max_size, step_size, rng);
    }

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

fn cap_count(raw: usize, cap: usize) -> usize {
    if cap == 0 {
        0
    } else {
        raw.min(cap)
    }
}

fn membership_batch_group_cap(_current_group_size: usize) -> usize {
    // AddCommit permits k > N. An N-dependent ceiling systematically starved
    // larger k values, so the publication sampler uses a fixed configured cap.
    MAX_RANDOM_MEMBERSHIP_BATCH_SIZE
}

fn update_ops_for_plateau(
    size: usize,
    update_rounds: usize,
    max_update_samples_per_plateau: usize,
) -> usize {
    cap_count(
        update_rounds.saturating_mul(size),
        max_update_samples_per_plateau,
    )
}

fn app_sends_per_payload_for_plateau(
    size: usize,
    app_rounds: usize,
    max_app_samples_per_payload: usize,
) -> usize {
    if size < 2 {
        0
    } else {
        cap_count(app_rounds.saturating_mul(size), max_app_samples_per_payload)
    }
}

fn app_ops_for_plateau(
    size: usize,
    app_rounds: usize,
    max_app_samples_per_payload: usize,
    payload_count: usize,
) -> usize {
    app_sends_per_payload_for_plateau(size, app_rounds, max_app_samples_per_payload)
        .saturating_mul(payload_count)
}

fn is_external_device(worker: &WorkerSpec) -> bool {
    !worker.device_kind.is_empty() && worker.device_kind != "scratch_container"
}

fn active_external_indices(active: &[WorkerSpec]) -> Vec<usize> {
    active
        .iter()
        .enumerate()
        .filter(|(_, worker)| worker.profile_enabled && is_external_device(worker))
        .map(|(idx, _)| idx)
        .collect()
}

fn active_profiled_indices(active: &[WorkerSpec]) -> Vec<usize> {
    active
        .iter()
        .enumerate()
        .filter_map(|(idx, worker)| worker.profile_enabled.then_some(idx))
        .collect()
}

fn active_profiled_non_external_indices(active: &[WorkerSpec]) -> Vec<usize> {
    active
        .iter()
        .enumerate()
        .filter(|(_, worker)| worker.profile_enabled && !is_external_device(worker))
        .map(|(idx, _)| idx)
        .collect()
}

fn external_remove_rejoin_pairs(active: &[WorkerSpec]) -> Vec<(String, String)> {
    let mut external_ids = active
        .iter()
        .filter(|worker| worker.profile_enabled && is_external_device(worker))
        .map(|worker| worker.id.clone())
        .collect::<Vec<_>>();
    external_ids.sort();

    if external_ids.len() < 2 {
        return Vec::new();
    }

    external_ids
        .iter()
        .enumerate()
        .map(|(idx, victim_id)| {
            (
                victim_id.clone(),
                external_ids[(idx + 1) % external_ids.len()].clone(),
            )
        })
        .collect()
}

fn least_used_external_actor_id(
    active: &[WorkerSpec],
    actor_use_counts: &HashMap<String, usize>,
    rng: &mut StdRng,
) -> Option<String> {
    let minimum = active
        .iter()
        .filter(|worker| is_external_device(worker))
        .map(|worker| actor_use_counts.get(&worker.id).copied().unwrap_or(0))
        .min()?;
    let mut candidates = active
        .iter()
        .filter(|worker| {
            is_external_device(worker)
                && actor_use_counts.get(&worker.id).copied().unwrap_or(0) == minimum
        })
        .map(|worker| worker.id.clone())
        .collect::<Vec<_>>();
    candidates.shuffle(rng);
    candidates.pop()
}

fn least_sampled_active_external_id(
    active: &[WorkerSpec],
    success_counts: &HashMap<String, usize>,
    required: usize,
) -> Option<String> {
    active
        .iter()
        .filter(|worker| worker.profile_enabled && is_external_device(worker))
        .filter(|worker| success_counts.get(&worker.id).copied().unwrap_or(0) < required)
        .min_by_key(|worker| {
            (
                success_counts.get(&worker.id).copied().unwrap_or(0),
                worker.id.clone(),
            )
        })
        .map(|worker| worker.id.clone())
}

fn sampled_member_index(member_count: usize, sample_count: usize, seq_no: usize) -> usize {
    assert!(member_count > 0, "member_count must be greater than zero");
    assert!(sample_count > 0, "sample_count must be greater than zero");

    if sample_count >= member_count {
        return seq_no % member_count;
    }

    let sample_no = seq_no % sample_count;
    let one_based_index =
        ((sample_no + 1) as u128 * member_count as u128 / sample_count as u128) as usize;

    // Pick the right edge of each equal bucket: e.g. 20 / 4 => 5, 10, 15, 20.
    one_based_index.saturating_sub(1)
}

fn deterministic_commit_receive_sample_indices(
    recipient_count: usize,
    max_samples: usize,
    seed: u64,
    plateau_size: usize,
    epoch: u64,
    seq_no: usize,
) -> Vec<usize> {
    if recipient_count == 0 || max_samples == 0 {
        return Vec::new();
    }
    if recipient_count <= max_samples {
        return (0..recipient_count).collect();
    }

    let mut chosen = Vec::new();
    chosen.push(0);
    if recipient_count > 1 {
        chosen.push(recipient_count - 1);
    }
    if recipient_count > 2 {
        chosen.push(recipient_count / 2);
    }

    let mut state = seed
        ^ ((plateau_size as u64) << 32)
        ^ epoch
        ^ ((seq_no as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15));
    while chosen.len() < max_samples {
        state = state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        let idx = (state as usize) % recipient_count;
        if !chosen.contains(&idx) {
            chosen.push(idx);
        }
    }
    chosen.sort_unstable();
    chosen
}

fn build_commit_receive_sampling_map(
    recipients: &[WorkerSpec],
    max_samples: usize,
    seed: u64,
    plateau_size: usize,
    epoch: u64,
    seq_no: usize,
) -> (HashSet<String>, HashMap<String, usize>, usize) {
    let mut sample_indices = deterministic_commit_receive_sample_indices(
        recipients.len(),
        max_samples,
        seed,
        plateau_size,
        epoch,
        seq_no,
    );

    // External devices are the scarce observations in hybrid campaigns. They
    // remain part of the deterministic sample even when the ordinary cap would
    // select different recipients. This can exceed max_samples by the number
    // of external devices, and the recorded sample count/index reflects that.
    for (idx, worker) in recipients.iter().enumerate() {
        if worker.profile_enabled && is_external_device(worker) && !sample_indices.contains(&idx) {
            sample_indices.push(idx);
        }
    }
    sample_indices.sort_unstable();
    let sampled_ids: HashSet<String> = sample_indices
        .iter()
        .filter_map(|idx| recipients.get(*idx))
        .map(|w| w.id.clone())
        .collect();
    let index_map = sample_indices
        .iter()
        .enumerate()
        .filter_map(|(sample_index, recipient_index)| {
            recipients
                .get(*recipient_index)
                .map(|w| (w.id.clone(), sample_index))
        })
        .collect::<HashMap<_, _>>();
    let sample_count = sampled_ids.len();
    (sampled_ids, index_map, sample_count)
}

fn estimate_total_units(
    plateau_sequence: &[usize],
    update_rounds: usize,
    app_rounds: usize,
    max_update_samples_per_plateau: usize,
    max_app_samples_per_payload: usize,
    payload_count: usize,
    external_device_count: usize,
    external_coverage_lane: bool,
    min_profiled_samples_per_operation: usize,
) -> usize {
    let mut total = 1usize;
    let mut current_size = 1usize;

    for &target in plateau_sequence {
        total = total.saturating_add(target.abs_diff(current_size));
        let mut update_ops =
            update_ops_for_plateau(target, update_rounds, max_update_samples_per_plateau);
        let mut app_ops = app_ops_for_plateau(
            target,
            app_rounds,
            max_app_samples_per_payload,
            payload_count,
        );

        if external_device_count > 0 && target >= 2 {
            let estimated_active_external = external_device_count.min(target.saturating_sub(1));
            let has_non_external = target > estimated_active_external;

            if external_coverage_lane {
                let profiled_actor_count =
                    estimated_active_external + usize::from(has_non_external);
                let required_update_ops =
                    profiled_actor_count.saturating_mul(min_profiled_samples_per_operation.max(1));
                update_ops = update_ops.max(required_update_ops);

                let base_app_per_payload = app_sends_per_payload_for_plateau(
                    target,
                    app_rounds,
                    max_app_samples_per_payload,
                );
                let required_app_per_payload =
                    profiled_actor_count.saturating_mul(min_profiled_samples_per_operation.max(1));
                app_ops =
                    app_ops.max(base_app_per_payload.max(required_app_per_payload) * payload_count);

                if min_profiled_samples_per_operation > 0 {
                    let density_units = estimated_active_external
                        .saturating_mul(min_profiled_samples_per_operation)
                        .saturating_mul(18);
                    total = total.saturating_add(density_units);
                }
            } else if app_sends_per_payload_for_plateau(
                target,
                app_rounds,
                max_app_samples_per_payload,
            ) == 1
            {
                total = total.saturating_add(payload_count);
            }
        }

        total = total.saturating_add(update_ops);
        total = total.saturating_add(app_ops);
        current_size = target;
    }

    total
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

async fn create_group(
    http: &reqwest::Client,
    leader: &WorkerSpec,
    progress: &mut Progress,
) -> Result<()> {
    send_cmd_expect_ok_fragment(
        http,
        leader,
        &Command::CreateGroup,
        "group created and DS group state registered",
    )
    .await?;
    progress.tick("create_group");
    Ok(())
}

async fn add_n_members(
    http: &reqwest::Client,
    active: &mut Vec<WorkerSpec>,
    idle: &mut VecDeque<WorkerSpec>,
    mut batch_decision: MembershipBatchDecision,
    fanout: &mut FanoutController,
    progress: &mut Progress,
    process_pending_fanout: bool,
    forced_actor_id: Option<&str>,
    rng: &mut StdRng,
    plateau_index: usize,
    target_size: usize,
    max_commit_receive_samples_per_plateau: usize,
    commit_receive_sampling_seed: u64,
    runner_events: &RunnerEventLog,
) -> Result<()> {
    let batch_size = batch_decision.effective;
    let timeout = Duration::from_secs(30);

    if active.is_empty() {
        return Err(anyhow!(
            "No active group member available to add new member"
        ));
    }
    if batch_size == 0 {
        return Err(anyhow!("Cannot add zero members"));
    }
    if idle.len() < batch_size {
        return Err(anyhow!(
            "Requested add batch of {} members, but only {} idle workers are available",
            batch_size,
            idle.len()
        ));
    }

    let actor_idx = forced_actor_id
        .and_then(|actor_id| active.iter().position(|worker| worker.id == actor_id))
        .unwrap_or_else(|| rng.gen_range(0..active.len()));
    let actor = active[actor_idx].clone();

    let mut joiners = Vec::with_capacity(batch_size);
    for _ in 0..batch_size {
        let joiner = idle
            .pop_front()
            .ok_or_else(|| anyhow!("No idle worker available to add"))?;
        joiners.push(joiner);
    }
    let mut prepared_joiners = Vec::with_capacity(joiners.len());
    for joiner in joiners {
        let fragment = format!("key package uploaded for {}", joiner.id);
        let cursor = BenchmarkCursor::new(
            plateau_index,
            target_size,
            active.len(),
            "membership_add",
            "generate_key_package",
        );
        if let Err(error) =
            send_cmd_expect_ok_fragment(http, &joiner, &Command::GenerateKeyPackage, &fragment)
                .await
        {
            if record_profiled_worker_failure(
                runner_events,
                &cursor,
                &joiner,
                &error,
                "drop_idle_joiner",
                Some(&actor),
            )
            .await?
            {
                continue;
            }
            return Err(error);
        }
        prepared_joiners.push(joiner);
    }

    if prepared_joiners.is_empty() {
        return Ok(());
    }
    let joiners = prepared_joiners;
    // OOM attrition can shrink a planned batch. Persist the actual commit k,
    // while retaining the requested value and caps for sampling diagnostics.
    batch_decision.effective = joiners.len();
    let joiner_ids: Vec<String> = joiners.iter().map(|joiner| joiner.id.clone()).collect();

    let actor_before = show_group_state(http, &actor).await?;
    let mut expected_members = actor_before.members.clone();
    expected_members.extend(joiner_ids.iter().cloned());
    let expected_after_commit =
        expected_group_state(&actor_before, actor_before.epoch + 1, expected_members);

    let cursor = BenchmarkCursor::new(
        plateau_index,
        target_size,
        active.len(),
        "membership_add",
        "add_commit",
    )
    .with_membership_batch(&batch_decision);

    let add_command = Command::AddMembers {
        members: joiner_ids.clone(),
    };
    let add_context = WorkerCommandContext::with_metadata(
        &actor,
        &add_command,
        None,
        Some("add_member.create"),
        Some(&cursor),
    );
    if let Err(error) = send_cmd_expect_ok_fragment_with_context(
        http,
        &actor,
        &add_command,
        "added locally in one commit",
        &add_context,
    )
    .await
    {
        if record_profiled_worker_failure(
            runner_events,
            &cursor,
            &actor,
            &error,
            "evict_add_actor_and_retry",
            active.iter().find(|worker| worker.id != actor.id),
        )
        .await?
        {
            let mut dead_workers = Vec::with_capacity(joiners.len() + 1);
            dead_workers.push(actor.clone());
            dead_workers.extend(joiners.iter().cloned());
            for joiner in joiners.into_iter().rev() {
                idle.push_front(joiner);
            }
            evict_oom_group_members(
                http,
                active,
                &dead_workers,
                fanout,
                process_pending_fanout,
                target_size,
                max_commit_receive_samples_per_plateau,
                commit_receive_sampling_seed,
                runner_events,
            )
            .await?;
            return Ok(());
        }
        return Err(error);
    }
    let actor_commit_result = if process_pending_fanout {
        process_pending_commit_expect(
            http,
            &actor,
            ExpectedReceiveCommitState::Group(expected_after_commit.clone()),
            "add_member.actor_process_pending",
            Some(&cursor),
        )
        .await
    } else {
        receive_commit_expect(
            http,
            &actor,
            "own commit accepted from DS",
            ExpectedReceiveCommitState::Group(expected_after_commit.clone()),
            "add_member.actor_receive_commit",
            Some(&cursor),
        )
        .await
    };
    if let Err(error) = actor_commit_result {
        if record_profiled_worker_failure(
            runner_events,
            &cursor,
            &actor,
            &error,
            "evict_add_actor_receive_and_retry",
            active.iter().find(|worker| worker.id != actor.id),
        )
        .await?
        {
            let mut dead_workers = Vec::with_capacity(joiners.len() + 1);
            dead_workers.push(actor.clone());
            dead_workers.extend(joiners.iter().cloned());
            for joiner in joiners.into_iter().rev() {
                idle.push_front(joiner);
            }
            evict_oom_group_members(
                http,
                active,
                &dead_workers,
                fanout,
                process_pending_fanout,
                target_size,
                max_commit_receive_samples_per_plateau,
                commit_receive_sampling_seed,
                runner_events,
            )
            .await?;
            return Ok(());
        }
        return Err(error);
    }

    let mut joined = Vec::with_capacity(joiners.len());
    let mut failed_joiners = Vec::new();
    for joiner in joiners {
        let join_fragment = format!("{} joined from welcome", joiner.id);
        let joined_result = send_cmd_until_ok(
            http,
            &joiner,
            &Command::JoinFromWelcome,
            &join_fragment,
            "404 Not Found",
            timeout,
        )
        .await;
        if let Err(error) = joined_result {
            let cursor = BenchmarkCursor::new(
                plateau_index,
                target_size,
                active.len(),
                "membership_add",
                "join_from_welcome",
            );
            if record_profiled_worker_failure(
                runner_events,
                &cursor,
                &joiner,
                &error,
                "evict_joiner_after_add_commit",
                Some(&actor),
            )
            .await?
            {
                failed_joiners.push(joiner);
                continue;
            }
            return Err(error);
        }
        joined.push(joiner);
    }

    let recipients: Vec<WorkerSpec> = active
        .iter()
        .enumerate()
        .filter(|(idx, _)| *idx != actor_idx)
        .map(|(_, worker)| worker.clone())
        .collect();
    let expected_state = ExpectedReceiveCommitState::Group(expected_after_commit.clone());
    let expected_ep = expected_epoch(&expected_state);
    let fanout_phase = if process_pending_fanout {
        "add_member.fanout_process_pending"
    } else {
        "add_member.fanout_receive_commit"
    };
    let (sampled_ids, sample_index_map, sample_count) = build_commit_receive_sampling_map(
        &recipients,
        max_commit_receive_samples_per_plateau,
        commit_receive_sampling_seed,
        target_size,
        expected_after_commit.epoch,
        0,
    );
    let commands_by_physical = build_batch_commands(&recipients, |worker| {
        BatchFanoutCommand {
            client_id: worker.id.clone(),
            request_id: None,
            command: if process_pending_fanout {
                let sampled = sampled_ids.contains(&worker.id);
                Command::ProcessPending {
                    kinds: Some(vec![PendingKind::Commits]),
                    max_messages: None,
                    expected_epoch: expected_ep,
                    profile: sampled,
                    commit_create_op: Some("add".to_string()),
                    commit_receive_sampling_policy: Some("edge_middle_seeded_v1".to_string()),
                    commit_receive_sampling_seed: Some(commit_receive_sampling_seed),
                    commit_receive_sample_index: sample_index_map.get(&worker.id).copied(),
                    commit_receive_sample_count: Some(sample_count),
                    commit_receive_population_size: Some(recipients.len()),
                }
            } else {
                let sampled = sampled_ids.contains(&worker.id);
                Command::ReceiveCommit {
                    profile: sampled,
                    commit_create_op: Some("add".to_string()),
                    commit_receive_sampling_policy: Some("edge_middle_seeded_v1".to_string()),
                    commit_receive_sampling_seed: Some(commit_receive_sampling_seed),
                    commit_receive_sample_index: sample_index_map.get(&worker.id).copied(),
                    commit_receive_sample_count: Some(sample_count),
                    commit_receive_population_size: Some(recipients.len()),
                }
            },
            expected_epoch: expected_ep,
            phase: Some(fanout_phase.to_string()),
            profile: sampled_ids.contains(&worker.id).then_some(true),
            benchmark_plateau_index: None,
            benchmark_target_size: None,
            benchmark_active_size: None,
            benchmark_phase: None,
            benchmark_operation: None,
            benchmark_operation_seq: None,
            benchmark_payload_size: None,
            membership_batch_requested: None,
            membership_batch_effective: None,
            membership_batch_group_cap: None,
            membership_batch_transition_cap: None,
            membership_batch_source: None,
        }
        .with_benchmark_cursor(&cursor)
    });
    let expected_by_client = recipients
        .iter()
        .map(|worker| (worker.id.clone(), expected_state.clone()))
        .collect::<HashMap<_, _>>();
    let fanout_result = batch_fanout_workers(
        http,
        "add_member",
        target_size,
        "receive_commit",
        &recipients,
        fanout,
        &commands_by_physical,
        Some(&expected_by_client),
    )
    .await;

    let mut failed_recipients = Vec::new();
    if let Err(error) = fanout_result {
        if let Some(dead_workers) = record_batch_oom_failures(
            runner_events,
            &cursor,
            &error,
            "evict_add_recipient_and_retry",
            Some(&actor),
        )
        .await?
        {
            failed_recipients = dead_workers;
        } else {
            return Err(error);
        }
    }

    let joined_count = joined.len();
    active.extend(joined);
    failed_joiners.extend(failed_recipients);
    evict_oom_group_members(
        http,
        active,
        &failed_joiners,
        fanout,
        process_pending_fanout,
        target_size,
        max_commit_receive_samples_per_plateau,
        commit_receive_sampling_seed,
        runner_events,
    )
    .await?;
    progress.tick_units(
        joined_count,
        &format!(
            "add {} member(s) {:?} actor={}",
            batch_size, joiner_ids, actor.id
        ),
    );
    Ok(())
}

async fn remove_n_members(
    http: &reqwest::Client,
    active: &mut Vec<WorkerSpec>,
    idle: &mut VecDeque<WorkerSpec>,
    batch_decision: MembershipBatchDecision,
    fanout: &mut FanoutController,
    progress: &mut Progress,
    process_pending_fanout: bool,
    forced_actor_id: Option<&str>,
    protect_external_members: bool,
    protect_profile_enabled_members: bool,
    plateau_size: usize,
    max_commit_receive_samples_per_plateau: usize,
    commit_receive_sampling_seed: u64,
    rng: &mut StdRng,
    runner_events: &RunnerEventLog,
) -> Result<()> {
    let mut batch_decision = batch_decision;
    let mut batch_size = batch_decision.effective;
    if active.len() <= 1 {
        return Err(anyhow!("Cannot remove the last remaining member"));
    }
    if batch_size == 0 {
        return Err(anyhow!("Cannot remove zero members"));
    }
    if batch_size >= active.len() {
        return Err(anyhow!(
            "Requested remove batch of {} members from {} active members; actor self-removal is not supported",
            batch_size,
            active.len()
        ));
    }

    let (actor_idx, mut removable_indices) = choose_remove_actor_and_removable_indices(
        active,
        forced_actor_id,
        protect_external_members,
        protect_profile_enabled_members,
        batch_size,
        rng,
    )?;
    let actor = active[actor_idx].clone();

    if removable_indices.is_empty() {
        return Err(anyhow!(
            "Cannot remove any member from {} active members without removing protected clients; actor={}",
            active.len(),
            actor.id
        ));
    }
    if removable_indices.len() < batch_size {
        eprintln!(
            "[remove] reducing batch {} -> {} to preserve protected clients; actor={}",
            batch_size,
            removable_indices.len(),
            actor.id
        );
        batch_size = removable_indices.len();
        batch_decision.effective = batch_size;
    }
    if removable_indices.len() < batch_size {
        return Err(anyhow!(
            "Cannot remove {} member(s) from {} active members without removing protected clients; actor={}, removable={}",
            batch_size,
            active.len(),
            actor.id,
            removable_indices.len()
        ));
    }
    let mut removed = Vec::with_capacity(batch_size);
    for _ in 0..batch_size {
        let candidate_pos = rng.gen_range(0..removable_indices.len());
        let removed_idx = removable_indices.swap_remove(candidate_pos);
        removed.push(active[removed_idx].clone());
    }
    let removed_ids: Vec<String> = removed.iter().map(|worker| worker.id.clone()).collect();
    let removed_id_set: HashSet<String> = removed_ids.iter().cloned().collect();

    let actor_before = show_group_state(http, &actor).await?;
    let expected_members = actor_before
        .members
        .iter()
        .filter(|member| !removed_id_set.contains(*member))
        .cloned()
        .collect::<Vec<_>>();
    let expected_after_commit =
        expected_group_state(&actor_before, actor_before.epoch + 1, expected_members);

    let cursor = BenchmarkCursor::new(
        plateau_size,
        plateau_size,
        active.len(),
        "membership_remove",
        "remove_commit",
    )
    .with_membership_batch(&batch_decision);

    let remove_command = Command::RemoveMembers {
        members: removed_ids.clone(),
    };
    let remove_context = WorkerCommandContext::with_metadata(
        &actor,
        &remove_command,
        None,
        Some("remove_member.create"),
        Some(&cursor),
    );
    if let Err(error) = send_cmd_expect_ok_fragment_with_context(
        http,
        &actor,
        &remove_command,
        "removed locally; group commit published",
        &remove_context,
    )
    .await
    {
        if record_profiled_worker_failure(
            runner_events,
            &cursor,
            &actor,
            &error,
            "evict_remove_actor_and_retry",
            active.iter().find(|worker| worker.id != actor.id),
        )
        .await?
        {
            evict_oom_group_members(
                http,
                active,
                &[actor],
                fanout,
                process_pending_fanout,
                plateau_size,
                max_commit_receive_samples_per_plateau,
                commit_receive_sampling_seed,
                runner_events,
            )
            .await?;
            return Ok(());
        }
        return Err(error);
    }

    if process_pending_fanout {
        process_pending_commit_expect(
            http,
            &actor,
            ExpectedReceiveCommitState::Group(expected_after_commit.clone()),
            "remove_member.actor_process_pending",
            Some(&cursor),
        )
        .await?;
    } else {
        receive_commit_expect(
            http,
            &actor,
            "own commit accepted from DS",
            ExpectedReceiveCommitState::Group(expected_after_commit.clone()),
            "remove_member.actor_receive_commit",
            Some(&cursor),
        )
        .await?;
    }

    let recipients: Vec<WorkerSpec> = active
        .iter()
        .filter(|worker| worker.id != actor.id)
        .cloned()
        .collect();
    let fanout_phase = if process_pending_fanout {
        "remove_member.fanout_process_pending"
    } else {
        "remove_member.fanout_receive_commit"
    };
    let (sampled_ids, sample_index_map, sample_count) = build_commit_receive_sampling_map(
        &recipients,
        max_commit_receive_samples_per_plateau,
        commit_receive_sampling_seed,
        plateau_size,
        expected_after_commit.epoch,
        0,
    );
    let commands_by_physical = build_batch_commands(&recipients, |worker| {
        let is_removed = removed_id_set.contains(&worker.id);
        let expected_ep = if is_removed {
            Some(expected_after_commit.epoch)
        } else {
            expected_epoch(&ExpectedReceiveCommitState::Group(
                expected_after_commit.clone(),
            ))
        };
        let sampled = sampled_ids.contains(&worker.id);
        BatchFanoutCommand {
            client_id: worker.id.clone(),
            request_id: None,
            command: if process_pending_fanout {
                Command::ProcessPending {
                    kinds: Some(vec![PendingKind::Commits]),
                    max_messages: None,
                    expected_epoch: expected_ep,
                    profile: sampled,
                    commit_create_op: Some("remove".to_string()),
                    commit_receive_sampling_policy: Some("edge_middle_seeded_v1".to_string()),
                    commit_receive_sampling_seed: Some(commit_receive_sampling_seed),
                    commit_receive_sample_index: sample_index_map.get(&worker.id).copied(),
                    commit_receive_sample_count: Some(sample_count),
                    commit_receive_population_size: Some(recipients.len()),
                }
            } else {
                Command::ReceiveCommit {
                    profile: sampled,
                    commit_create_op: Some("remove".to_string()),
                    commit_receive_sampling_policy: Some("edge_middle_seeded_v1".to_string()),
                    commit_receive_sampling_seed: Some(commit_receive_sampling_seed),
                    commit_receive_sample_index: sample_index_map.get(&worker.id).copied(),
                    commit_receive_sample_count: Some(sample_count),
                    commit_receive_population_size: Some(recipients.len()),
                }
            },
            expected_epoch: expected_ep,
            phase: Some(fanout_phase.to_string()),
            profile: sampled.then_some(true),
            benchmark_plateau_index: None,
            benchmark_target_size: None,
            benchmark_active_size: None,
            benchmark_phase: None,
            benchmark_operation: None,
            benchmark_operation_seq: None,
            benchmark_payload_size: None,
            membership_batch_requested: None,
            membership_batch_effective: None,
            membership_batch_group_cap: None,
            membership_batch_transition_cap: None,
            membership_batch_source: None,
        }
        .with_benchmark_cursor(&cursor)
    });
    let expected_by_client = recipients
        .iter()
        .map(|worker| {
            let expected = if removed_id_set.contains(&worker.id) {
                ExpectedReceiveCommitState::Removed {
                    expected_epoch: expected_after_commit.epoch,
                }
            } else {
                ExpectedReceiveCommitState::Group(expected_after_commit.clone())
            };
            (worker.id.clone(), expected)
        })
        .collect::<HashMap<_, _>>();
    batch_fanout_workers(
        http,
        "remove_member",
        active.len(),
        "receive_commit",
        &recipients,
        fanout,
        &commands_by_physical,
        Some(&expected_by_client),
    )
    .await?;

    active.retain(|worker| !removed_id_set.contains(&worker.id));
    for removed_worker in removed {
        idle.push_front(removed_worker);
    }

    progress.tick_units(
        batch_size,
        &format!(
            "remove {} member(s) {:?} actor={}",
            batch_size, removed_ids, actor.id
        ),
    );
    Ok(())
}

fn removable_member_indices(
    active: &[WorkerSpec],
    actor_idx: usize,
    protect_external_members: bool,
    protect_profile_enabled_members: bool,
) -> Vec<usize> {
    (0..active.len())
        .filter(|idx| {
            *idx != actor_idx
                && (!protect_external_members || !is_external_device(&active[*idx]))
                && (!protect_profile_enabled_members || !active[*idx].profile_enabled)
        })
        .collect()
}

fn choose_remove_actor_and_removable_indices(
    active: &[WorkerSpec],
    forced_actor_id: Option<&str>,
    protect_external_members: bool,
    protect_profile_enabled_members: bool,
    requested_batch_size: usize,
    rng: &mut StdRng,
) -> Result<(usize, Vec<usize>)> {
    if let Some(actor_id) = forced_actor_id {
        if let Some(actor_idx) = active.iter().position(|worker| worker.id == actor_id) {
            let removable = removable_member_indices(
                active,
                actor_idx,
                protect_external_members,
                protect_profile_enabled_members,
            );
            return Ok((actor_idx, removable));
        }
    }

    let candidates = (0..active.len())
        .map(|actor_idx| {
            let removable = removable_member_indices(
                active,
                actor_idx,
                protect_external_members,
                protect_profile_enabled_members,
            );
            (actor_idx, removable)
        })
        .collect::<Vec<_>>();

    let capable = candidates
        .iter()
        .filter(|(_, removable)| removable.len() >= requested_batch_size)
        .collect::<Vec<_>>();
    if !capable.is_empty() {
        let selected = capable[rng.gen_range(0..capable.len())];
        return Ok((selected.0, selected.1.clone()));
    }

    let max_removable = candidates
        .iter()
        .map(|(_, removable)| removable.len())
        .max()
        .unwrap_or(0);
    let best = candidates
        .iter()
        .filter(|(_, removable)| removable.len() == max_removable)
        .collect::<Vec<_>>();
    if best.is_empty() {
        return Err(anyhow!("No active removal actor candidates"));
    }
    let selected = best[rng.gen_range(0..best.len())];
    Ok((selected.0, selected.1.clone()))
}

fn protected_member_floor(
    workers: &[WorkerSpec],
    protect_profile_enabled_members: bool,
    protect_external_members: bool,
) -> usize {
    let mut protected = HashSet::new();
    if protect_profile_enabled_members {
        protected.extend(
            workers
                .iter()
                .filter(|worker| worker.profile_enabled)
                .map(|worker| worker.id.clone()),
        );
    }
    if protect_external_members {
        protected.extend(
            workers
                .iter()
                .filter(|worker| is_external_device(worker))
                .map(|worker| worker.id.clone()),
        );
    }
    protected.len()
}

async fn best_effort_process_pending_commits_after_epoch_race(
    http: &reqwest::Client,
    actor: &WorkerSpec,
    cursor: &BenchmarkCursor,
    attempt: usize,
) -> Option<String> {
    let command = Command::ProcessPending {
        kinds: Some(vec![PendingKind::Commits]),
        max_messages: Some(8),
        expected_epoch: None,
        profile: false,
        commit_create_op: None,
        commit_receive_sampling_policy: None,
        commit_receive_sampling_seed: None,
        commit_receive_sample_index: None,
        commit_receive_sample_count: None,
        commit_receive_population_size: None,
    };
    let phase = format!("oom_eviction.actor_process_pending_after_epoch_race_{attempt}");
    let context = WorkerCommandContext::with_metadata(
        actor,
        &command,
        None,
        Some(phase.as_str()),
        Some(cursor),
    );

    match send_cmd_expect_ok_fragment_with_context(
        http,
        actor,
        &command,
        "process_pending processed;",
        &context,
    )
    .await
    {
        Ok(message) => Some(message),
        Err(error) => {
            eprintln!(
                "[oom-eviction] process_pending after epoch race failed; actor={} attempt={} error={:#}; retrying from fresh group state",
                actor.id, attempt, error
            );
            None
        }
    }
}

fn queued_remove_members_republished(message: &str) -> bool {
    message.contains("queued remove_members") && message.contains("was retried and published")
}

async fn publish_remove_members_with_epoch_race_recovery(
    http: &reqwest::Client,
    actor: &WorkerSpec,
    dead_ids: &HashSet<String>,
    cursor: &BenchmarkCursor,
) -> Result<ExpectedGroupState> {
    for attempt in 1..=3 {
        let actor_before = show_group_state(http, actor).await?;
        let removed_ids = actor_before
            .members
            .iter()
            .filter(|member| dead_ids.contains(*member))
            .cloned()
            .collect::<Vec<_>>();
        let missing_dead_ids = dead_ids
            .iter()
            .filter(|dead_id| !actor_before.members.contains(*dead_id))
            .cloned()
            .collect::<Vec<_>>();
        if !missing_dead_ids.is_empty()
            && best_effort_process_pending_commits_after_epoch_race(http, actor, cursor, attempt)
                .await
                .is_some()
        {
            eprintln!(
                "[oom-eviction] caught up actor before removing dead members not visible at current epoch; actor={} attempt={} missing_dead_ids={:?}",
                actor.id, attempt, missing_dead_ids
            );
            continue;
        }
        let expected_members = actor_before
            .members
            .iter()
            .filter(|member| !dead_ids.contains(*member))
            .cloned()
            .collect::<Vec<_>>();

        if removed_ids.is_empty() {
            return Ok(expected_group_state(
                &actor_before,
                actor_before.epoch,
                expected_members,
            ));
        }

        let expected_after_commit =
            expected_group_state(&actor_before, actor_before.epoch + 1, expected_members);
        let command = Command::RemoveMembers {
            members: removed_ids.clone(),
        };
        let context = WorkerCommandContext::with_metadata(
            actor,
            &command,
            None,
            Some("oom_eviction.remove_members"),
            Some(cursor),
        );
        let response = send_command_with_context(http, actor, &command, &context).await?;

        match response.status.as_str() {
            "ok" if response
                .message
                .contains("removed locally; group commit published") =>
            {
                return Ok(expected_after_commit);
            }
            "ok" if is_queued_epoch_race_message(&response.message) => {
                eprintln!(
                    "[oom-eviction] remove_members queued after epoch race; actor={} attempt={} message={}",
                    actor.id, attempt, response.message
                );
                if let Some(message) = best_effort_process_pending_commits_after_epoch_race(
                    http, actor, cursor, attempt,
                )
                .await
                {
                    if queued_remove_members_republished(&message) {
                        let actor_after = show_group_state(http, actor).await?;
                        let expected_members = actor_after
                            .members
                            .iter()
                            .filter(|member| !dead_ids.contains(*member))
                            .cloned()
                            .collect::<Vec<_>>();
                        let expected_epoch = if expected_members.len() == actor_after.members.len()
                        {
                            actor_after.epoch
                        } else {
                            actor_after.epoch + 1
                        };
                        return Ok(expected_group_state(
                            &actor_after,
                            expected_epoch,
                            expected_members,
                        ));
                    }
                }
            }
            "ok" => {
                return Err(anyhow!(
                    "Worker {} returned unexpected ok message: {}",
                    actor.id,
                    response.message
                ));
            }
            "error" if response.message.contains("pending commit exists") => {
                eprintln!(
                    "[oom-eviction] remove_members blocked by pending commit; actor={} attempt={} message={}",
                    actor.id, attempt, response.message
                );
                best_effort_process_pending_commits_after_epoch_race(http, actor, cursor, attempt)
                    .await;
            }
            "error" => return Err(anyhow!("Worker {} error: {}", actor.id, response.message)),
            other => {
                return Err(anyhow!(
                    "Worker {} returned unknown status '{}': {}",
                    actor.id,
                    other,
                    response.message
                ));
            }
        }
    }

    Err(anyhow!(
        "remove_members for OOM eviction kept losing the epoch race after 3 attempts"
    ))
}

async fn evict_oom_group_members(
    http: &reqwest::Client,
    active: &mut Vec<WorkerSpec>,
    dead_workers: &[WorkerSpec],
    fanout: &mut FanoutController,
    process_pending_fanout: bool,
    plateau_size: usize,
    max_commit_receive_samples_per_plateau: usize,
    commit_receive_sampling_seed: u64,
    runner_events: &RunnerEventLog,
) -> Result<()> {
    if dead_workers.is_empty() {
        return Ok(());
    }
    let mut dead_ids = dead_workers
        .iter()
        .map(|worker| worker.id.clone())
        .collect::<HashSet<_>>();

    loop {
        let actor = active
            .iter()
            .find(|worker| !dead_ids.contains(&worker.id))
            .cloned()
            .ok_or_else(|| anyhow!("No live OpenMLS member remains to evict OOM workers"))?;

        let cursor = BenchmarkCursor::new(
            plateau_size,
            plateau_size,
            active.len(),
            "oom_eviction",
            "evict_commit",
        );

        let actor_before = match show_group_state(http, &actor).await {
            Ok(state) => state,
            Err(error) => {
                if record_profiled_worker_failure(
                    runner_events,
                    &cursor,
                    &actor,
                    &error,
                    "evict_oom_eviction_actor_and_retry",
                    active
                        .iter()
                        .find(|worker| worker.id != actor.id && !dead_ids.contains(&worker.id)),
                )
                .await?
                {
                    dead_ids.insert(actor.id);
                    continue;
                }
                return Err(error);
            }
        };
        let removed_ids = actor_before
            .members
            .iter()
            .filter(|member| dead_ids.contains(*member))
            .cloned()
            .collect::<Vec<_>>();
        if removed_ids.is_empty() {
            active.retain(|worker| !dead_ids.contains(&worker.id));
            return Ok(());
        }

        let expected_after_commit =
            match publish_remove_members_with_epoch_race_recovery(http, &actor, &dead_ids, &cursor)
                .await
            {
                Ok(expected) => expected,
                Err(error) => {
                    if record_profiled_worker_failure(
                        runner_events,
                        &cursor,
                        &actor,
                        &error,
                        "evict_oom_eviction_actor_and_retry",
                        active
                            .iter()
                            .find(|worker| worker.id != actor.id && !dead_ids.contains(&worker.id)),
                    )
                    .await?
                    {
                        dead_ids.insert(actor.id);
                        continue;
                    }
                    return Err(error);
                }
            };

        let actor_commit_result = if process_pending_fanout {
            process_pending_commit_expect(
                http,
                &actor,
                ExpectedReceiveCommitState::Group(expected_after_commit.clone()),
                "oom_eviction.actor_process_pending",
                Some(&cursor),
            )
            .await
        } else {
            receive_commit_expect(
                http,
                &actor,
                "own commit accepted from DS",
                ExpectedReceiveCommitState::Group(expected_after_commit.clone()),
                "oom_eviction.actor_receive_commit",
                Some(&cursor),
            )
            .await
        };
        if let Err(error) = actor_commit_result {
            if record_profiled_worker_failure(
                runner_events,
                &cursor,
                &actor,
                &error,
                "evict_oom_eviction_actor_and_retry",
                active
                    .iter()
                    .find(|worker| worker.id != actor.id && !dead_ids.contains(&worker.id)),
            )
            .await?
            {
                dead_ids.insert(actor.id);
                continue;
            }
            return Err(error);
        }

        let recipients = active
            .iter()
            .filter(|worker| worker.id != actor.id && !dead_ids.contains(&worker.id))
            .cloned()
            .collect::<Vec<_>>();
        let expected_state = ExpectedReceiveCommitState::Group(expected_after_commit.clone());
        let expected_ep = expected_epoch(&expected_state);
        let fanout_phase = if process_pending_fanout {
            "oom_eviction.fanout_process_pending"
        } else {
            "oom_eviction.fanout_receive_commit"
        };
        let (sampled_ids, sample_index_map, sample_count) = build_commit_receive_sampling_map(
            &recipients,
            max_commit_receive_samples_per_plateau,
            commit_receive_sampling_seed,
            plateau_size,
            expected_after_commit.epoch,
            0,
        );
        let commands_by_physical = build_batch_commands(&recipients, |worker| {
            let sampled = sampled_ids.contains(&worker.id);
            BatchFanoutCommand {
                client_id: worker.id.clone(),
                request_id: None,
                command: if process_pending_fanout {
                    Command::ProcessPending {
                        kinds: Some(vec![PendingKind::Commits]),
                        max_messages: None,
                        expected_epoch: expected_ep,
                        profile: sampled,
                        commit_create_op: Some("remove".to_string()),
                        commit_receive_sampling_policy: Some("edge_middle_seeded_v1".to_string()),
                        commit_receive_sampling_seed: Some(commit_receive_sampling_seed),
                        commit_receive_sample_index: sample_index_map.get(&worker.id).copied(),
                        commit_receive_sample_count: Some(sample_count),
                        commit_receive_population_size: Some(recipients.len()),
                    }
                } else {
                    Command::ReceiveCommit {
                        profile: sampled,
                        commit_create_op: Some("remove".to_string()),
                        commit_receive_sampling_policy: Some("edge_middle_seeded_v1".to_string()),
                        commit_receive_sampling_seed: Some(commit_receive_sampling_seed),
                        commit_receive_sample_index: sample_index_map.get(&worker.id).copied(),
                        commit_receive_sample_count: Some(sample_count),
                        commit_receive_population_size: Some(recipients.len()),
                    }
                },
                expected_epoch: expected_ep,
                phase: Some(fanout_phase.to_string()),
                profile: sampled.then_some(true),
                benchmark_plateau_index: None,
                benchmark_target_size: None,
                benchmark_active_size: None,
                benchmark_phase: None,
                benchmark_operation: None,
                benchmark_operation_seq: None,
                benchmark_payload_size: None,
                membership_batch_requested: None,
                membership_batch_effective: None,
                membership_batch_group_cap: None,
                membership_batch_transition_cap: None,
                membership_batch_source: None,
            }
            .with_benchmark_cursor(&cursor)
        });
        let expected_by_client = recipients
            .iter()
            .map(|worker| (worker.id.clone(), expected_state.clone()))
            .collect::<HashMap<_, _>>();
        let fanout_result = batch_fanout_workers(
            http,
            "oom_eviction",
            recipients.len(),
            if process_pending_fanout {
                "process_pending"
            } else {
                "receive_commit"
            },
            &recipients,
            fanout,
            &commands_by_physical,
            Some(&expected_by_client),
        )
        .await;
        if let Err(error) = fanout_result {
            if let Some(new_dead_workers) = record_batch_oom_failures(
                runner_events,
                &cursor,
                &error,
                "evict_oom_eviction_recipient_and_retry",
                Some(&actor),
            )
            .await?
            {
                for worker in new_dead_workers {
                    dead_ids.insert(worker.id);
                }
                continue;
            }
            return Err(error);
        }

        active.retain(|worker| !dead_ids.contains(&worker.id));
        return Ok(());
    }
}

async fn transition_to_size(
    http: &reqwest::Client,
    active: &mut Vec<WorkerSpec>,
    idle: &mut VecDeque<WorkerSpec>,
    target_size: usize,
    fanout: &mut FanoutController,
    add_membership_batches: &mut MembershipBatchPlans,
    remove_membership_batches: &mut MembershipBatchPlans,
    progress: &mut Progress,
    process_pending_fanout: bool,
    external_coverage_lane: bool,
    protect_profile_enabled_members: bool,
    profiled_add_actor_seen: &mut HashSet<String>,
    max_commit_receive_samples_per_plateau: usize,
    commit_receive_sampling_seed: u64,
    rng: &mut StdRng,
    plateau_index: usize,
    runner_events: &RunnerEventLog,
) -> Result<()> {
    let mut external_add_actor_use_counts = HashMap::<String, usize>::new();

    while active.len() < target_size {
        let remaining = target_size - active.len();
        let max_allowed = remaining.min(idle.len());
        if max_allowed == 0 {
            eprintln!(
                "[oom-attrition] plateau target {} cannot be reached; active={} no idle workers remain",
                target_size,
                active.len()
            );
            break;
        }
        let external_actor_id = external_coverage_lane
            .then(|| least_used_external_actor_id(active, &external_add_actor_use_counts, rng))
            .flatten();
        let profiled_actor_id = if protect_profile_enabled_members {
            let mut unseen_profiled = active
                .iter()
                .filter(|worker| {
                    worker.profile_enabled && !profiled_add_actor_seen.contains(&worker.id)
                })
                .map(|worker| worker.id.clone())
                .collect::<Vec<_>>();
            unseen_profiled.shuffle(rng);
            unseen_profiled.pop().or_else(|| {
                active
                    .iter()
                    .find(|worker| worker.profile_enabled)
                    .map(|worker| worker.id.clone())
            })
        } else {
            None
        };
        let forced_actor_id = external_actor_id.clone().or(profiled_actor_id);
        if let Some(actor_id) = forced_actor_id.as_ref() {
            if external_actor_id.as_ref() == Some(actor_id) {
                *external_add_actor_use_counts
                    .entry(actor_id.clone())
                    .or_default() += 1;
            }
            if active
                .iter()
                .any(|worker| worker.id == *actor_id && worker.profile_enabled)
            {
                profiled_add_actor_seen.insert(actor_id.clone());
            }
        }
        let reserved_for_external = if external_coverage_lane {
            active
                .iter()
                .filter(|worker| {
                    is_external_device(worker)
                        && external_actor_id.as_deref() != Some(worker.id.as_str())
                        && external_add_actor_use_counts
                            .get(&worker.id)
                            .copied()
                            .unwrap_or(0)
                            == 0
                })
                .count()
                .min(max_allowed.saturating_sub(1))
        } else {
            0
        };
        let decision_cap = max_allowed.saturating_sub(reserved_for_external).max(1);
        let batch_decision = add_membership_batches.next_batch(
            active.len(),
            decision_cap,
            external_actor_id.as_deref(),
        );
        add_n_members(
            http,
            active,
            idle,
            batch_decision,
            fanout,
            progress,
            process_pending_fanout,
            forced_actor_id.as_deref(),
            rng,
            plateau_index,
            target_size,
            max_commit_receive_samples_per_plateau,
            commit_receive_sampling_seed,
            runner_events,
        )
        .await?;
    }

    let mut external_remove_actor_use_counts = HashMap::<String, usize>::new();

    while active.len() > target_size {
        let remaining = active.len() - target_size;
        let max_allowed = remaining.min(active.len().saturating_sub(1));
        if max_allowed == 0 {
            return Err(anyhow!("Cannot remove the last remaining member"));
        }
        let forced_actor_id = external_coverage_lane
            .then(|| least_used_external_actor_id(active, &external_remove_actor_use_counts, rng))
            .flatten();
        if let Some(actor_id) = forced_actor_id.as_ref() {
            *external_remove_actor_use_counts
                .entry(actor_id.clone())
                .or_default() += 1;
        }
        let reserved_for_external = if external_coverage_lane {
            active
                .iter()
                .filter(|worker| {
                    is_external_device(worker)
                        && forced_actor_id.as_deref() != Some(worker.id.as_str())
                        && external_remove_actor_use_counts
                            .get(&worker.id)
                            .copied()
                            .unwrap_or(0)
                            == 0
                })
                .count()
                .min(max_allowed.saturating_sub(1))
        } else {
            0
        };
        let decision_cap = max_allowed.saturating_sub(reserved_for_external).max(1);
        let batch_decision = remove_membership_batches.next_batch(
            active.len(),
            decision_cap,
            forced_actor_id.as_deref(),
        );
        remove_n_members(
            http,
            active,
            idle,
            batch_decision,
            fanout,
            progress,
            process_pending_fanout,
            forced_actor_id.as_deref(),
            external_coverage_lane,
            protect_profile_enabled_members,
            target_size,
            max_commit_receive_samples_per_plateau,
            commit_receive_sampling_seed,
            rng,
            runner_events,
        )
        .await?;
    }

    Ok(())
}

async fn restore_density_plateau(
    http: &reqwest::Client,
    active: &mut Vec<WorkerSpec>,
    idle: &mut VecDeque<WorkerSpec>,
    fanout: &mut FanoutController,
    progress: &mut Progress,
    process_pending_fanout: bool,
    plateau_size: usize,
    max_commit_receive_samples_per_plateau: usize,
    commit_receive_sampling_seed: u64,
    rng: &mut StdRng,
    plateau_index: usize,
    runner_events: &RunnerEventLog,
) -> Result<()> {
    let mut requeued_external = idle
        .iter()
        .filter(|worker| worker.profile_enabled && is_external_device(worker))
        .map(|worker| worker.id.clone())
        .collect::<Vec<_>>();
    requeued_external.sort();
    for worker_id in requeued_external.into_iter().rev() {
        if let Some(position) = idle.iter().position(|worker| worker.id == worker_id) {
            let worker = idle
                .remove(position)
                .expect("worker position came from the same idle queue");
            idle.push_front(worker);
        }
    }

    while active.len() < plateau_size {
        let batch_size = plateau_size
            .saturating_sub(active.len())
            .min(idle.len())
            .min(MAX_RANDOM_MEMBERSHIP_BATCH_SIZE);
        if batch_size == 0 {
            break;
        }
        let actor_id = active
            .iter()
            .find(|worker| worker.profile_enabled && is_external_device(worker))
            .map(|worker| worker.id.clone());
        let before = active.len();
        add_n_members(
            http,
            active,
            idle,
            MembershipBatchDecision {
                requested: batch_size,
                effective: batch_size,
                group_cap: membership_batch_group_cap(active.len()),
                transition_cap: batch_size,
                source: "external_density_recovery",
            },
            fanout,
            progress,
            process_pending_fanout,
            actor_id.as_deref(),
            rng,
            plateau_index,
            plateau_size,
            max_commit_receive_samples_per_plateau,
            commit_receive_sampling_seed,
            runner_events,
        )
        .await?;
        if active.len() <= before {
            return Err(anyhow!(
                "external density recovery made no progress at plateau {} (active={}, idle={})",
                plateau_size,
                active.len(),
                idle.len(),
            ));
        }
    }
    if active.len() != plateau_size {
        return Err(anyhow!(
            "external density recovery could not restore plateau {} (active={}, idle={})",
            plateau_size,
            active.len(),
            idle.len(),
        ));
    }
    Ok(())
}

fn external_density_source(batch_size: usize, reset: bool) -> &'static str {
    match (batch_size, reset) {
        (1, false) => "external_density_k1",
        (8, false) => "external_density_k8",
        (1, true) => "external_density_reset_k1",
        (8, true) => "external_density_reset_k8",
        _ => "external_density_other",
    }
}

async fn run_external_add_density_phase(
    http: &reqwest::Client,
    active: &mut Vec<WorkerSpec>,
    idle: &mut VecDeque<WorkerSpec>,
    fanout: &mut FanoutController,
    progress: &mut Progress,
    process_pending_fanout: bool,
    external_coverage_lane: bool,
    min_profiled_samples_per_operation: usize,
    plateau_size: usize,
    max_commit_receive_samples_per_plateau: usize,
    commit_receive_sampling_seed: u64,
    rng: &mut StdRng,
    plateau_index: usize,
    runner_events: &RunnerEventLog,
) -> Result<()> {
    if !external_coverage_lane || min_profiled_samples_per_operation == 0 {
        return Ok(());
    }

    let protected_count = active
        .iter()
        .filter(|worker| worker.profile_enabled || is_external_device(worker))
        .count();
    let removable_count = active.len().saturating_sub(protected_count);
    let mut batch_sizes = vec![1usize];
    if removable_count >= MAX_RANDOM_MEMBERSHIP_BATCH_SIZE {
        batch_sizes.push(MAX_RANDOM_MEMBERSHIP_BATCH_SIZE);
    } else {
        eprintln!(
            "[add-density] plateau {}: k=8 is infeasible with {} non-profiled removable member(s); k=1 remains enabled",
            plateau_size,
            removable_count,
        );
    }

    let mut success_counts = HashMap::<(usize, String), usize>::new();
    eprintln!(
        "[add-density] plateau {} completing {} successful AddCommit samples per active external device for k={:?}",
        plateau_size,
        min_profiled_samples_per_operation,
        batch_sizes,
    );

    loop {
        let mut external_ids = active
            .iter()
            .filter(|worker| worker.profile_enabled && is_external_device(worker))
            .map(|worker| worker.id.clone())
            .collect::<Vec<_>>();
        external_ids.sort();
        if external_ids.is_empty() {
            eprintln!(
                "[add-density] plateau {} stopped: no active external devices remain",
                plateau_size,
            );
            return Ok(());
        }

        let next = batch_sizes
            .iter()
            .flat_map(|batch_size| {
                external_ids
                    .iter()
                    .map(move |worker_id| (*batch_size, worker_id.clone()))
            })
            .filter(|key| {
                success_counts.get(key).copied().unwrap_or(0) < min_profiled_samples_per_operation
            })
            .min_by_key(|key| (success_counts.get(key).copied().unwrap_or(0), key.clone()));
        let Some((batch_size, actor_id)) = next else {
            break;
        };

        if active.len() != plateau_size {
            restore_density_plateau(
                http,
                active,
                idle,
                fanout,
                progress,
                process_pending_fanout,
                plateau_size,
                max_commit_receive_samples_per_plateau,
                commit_receive_sampling_seed,
                rng,
                plateau_index,
                runner_events,
            )
            .await?;
        }

        remove_n_members(
            http,
            active,
            idle,
            MembershipBatchDecision {
                requested: batch_size,
                effective: batch_size,
                group_cap: membership_batch_group_cap(active.len()),
                transition_cap: batch_size,
                source: external_density_source(batch_size, true),
            },
            fanout,
            progress,
            process_pending_fanout,
            Some(&actor_id),
            true,
            true,
            plateau_size,
            max_commit_receive_samples_per_plateau,
            commit_receive_sampling_seed,
            rng,
            runner_events,
        )
        .await?;

        if !active.iter().any(|worker| worker.id == actor_id)
            || active.len() != plateau_size.saturating_sub(batch_size)
        {
            restore_density_plateau(
                http,
                active,
                idle,
                fanout,
                progress,
                process_pending_fanout,
                plateau_size,
                max_commit_receive_samples_per_plateau,
                commit_receive_sampling_seed,
                rng,
                plateau_index,
                runner_events,
            )
            .await?;
            continue;
        }

        add_n_members(
            http,
            active,
            idle,
            MembershipBatchDecision {
                requested: batch_size,
                effective: batch_size,
                group_cap: membership_batch_group_cap(active.len()),
                transition_cap: batch_size,
                source: external_density_source(batch_size, false),
            },
            fanout,
            progress,
            process_pending_fanout,
            Some(&actor_id),
            rng,
            plateau_index,
            plateau_size,
            max_commit_receive_samples_per_plateau,
            commit_receive_sampling_seed,
            runner_events,
        )
        .await?;

        if active.iter().any(|worker| worker.id == actor_id) && active.len() == plateau_size {
            let completed = success_counts
                .entry((batch_size, actor_id.clone()))
                .or_default();
            *completed += 1;
            eprintln!(
                "[add-density] plateau={} k={} actor={} samples={}/{}",
                plateau_size, batch_size, actor_id, *completed, min_profiled_samples_per_operation,
            );
        } else {
            restore_density_plateau(
                http,
                active,
                idle,
                fanout,
                progress,
                process_pending_fanout,
                plateau_size,
                max_commit_receive_samples_per_plateau,
                commit_receive_sampling_seed,
                rng,
                plateau_index,
                runner_events,
            )
            .await?;
        }
    }

    Ok(())
}

fn select_supplemental_remove_receiver_roles(
    active: &[WorkerSpec],
    external_ids: &[String],
    actor_counts: &HashMap<String, usize>,
    receiver_counts: &HashMap<String, usize>,
    required: usize,
    plateau_size: usize,
) -> Result<Option<(String, String, String)>> {
    let Some(receiver_id) = external_ids
        .iter()
        .filter(|id| receiver_counts.get(*id).copied().unwrap_or(0) < required)
        .min_by_key(|id| {
            (
                receiver_counts.get(*id).copied().unwrap_or(0),
                (*id).clone(),
            )
        })
        .cloned()
    else {
        return Ok(None);
    };
    let actor_id = external_ids
        .iter()
        .filter(|id| **id != receiver_id)
        .min_by_key(|id| (actor_counts.get(*id).copied().unwrap_or(0), (*id).clone()))
        .cloned()
        .expect("at least two external devices are active");
    let victim_id = active
        .iter()
        .filter(|worker| !is_external_device(worker))
        .filter(|worker| worker.id != actor_id && worker.id != receiver_id)
        .min_by_key(|worker| (worker.profile_enabled, worker.id.clone()))
        .map(|worker| worker.id.clone())
        .ok_or_else(|| {
            anyhow!(
                "remove/rejoin receiver density at plateau {} needs a non-external victim after external attrition",
                plateau_size
            )
        })?;
    Ok(Some((actor_id, victim_id, receiver_id)))
}

async fn run_remove_rejoin_phase(
    http: &reqwest::Client,
    active: &mut Vec<WorkerSpec>,
    idle: &mut VecDeque<WorkerSpec>,
    fanout: &mut FanoutController,
    progress: &mut Progress,
    process_pending_fanout: bool,
    external_coverage_lane: bool,
    min_profiled_samples_per_operation: usize,
    plateau_size: usize,
    max_commit_receive_samples_per_plateau: usize,
    commit_receive_sampling_seed: u64,
    rng: &mut StdRng,
    plateau_index: usize,
    runner_events: &RunnerEventLog,
) -> Result<()> {
    let profiled = active_profiled_indices(active);
    if profiled.len() < 2 {
        eprintln!(
            "[remove-rejoin] plateau {} skipped: need >=2 profiled, have {}",
            plateau_size,
            profiled.len()
        );
        return Ok(());
    }

    if external_coverage_lane && !external_remove_rejoin_pairs(active).is_empty() {
        let required = min_profiled_samples_per_operation.max(1);
        let mut welcome_counts = HashMap::<String, usize>::new();
        let mut actor_counts = HashMap::<String, usize>::new();
        let mut receiver_counts = HashMap::<String, usize>::new();
        eprintln!(
            "[remove-rejoin] plateau {} completing {} successful welcome, remove-actor, and remove-receiver samples per active external device",
            plateau_size,
            required,
        );
        loop {
            let mut external_ids = active
                .iter()
                .filter(|worker| worker.profile_enabled && is_external_device(worker))
                .map(|worker| worker.id.clone())
                .collect::<Vec<_>>();
            external_ids.sort();
            if external_ids.len() < 2 {
                eprintln!(
                    "[remove-rejoin] plateau {} stopped after external attrition: {} active external device(s) remain",
                    plateau_size,
                    external_ids.len(),
                );
                return Ok(());
            }

            let actor_id = external_ids
                .iter()
                .filter(|id| actor_counts.get(*id).copied().unwrap_or(0) < required)
                .min_by_key(|id| (actor_counts.get(*id).copied().unwrap_or(0), (*id).clone()))
                .cloned();
            let victim_id = external_ids
                .iter()
                .filter(|id| actor_id.as_ref() != Some(*id))
                .filter(|id| welcome_counts.get(*id).copied().unwrap_or(0) < required)
                .min_by_key(|id| (welcome_counts.get(*id).copied().unwrap_or(0), (*id).clone()))
                .cloned()
                .or_else(|| {
                    external_ids
                        .iter()
                        .filter(|id| actor_id.as_ref() != Some(*id))
                        .min_by_key(|id| {
                            (welcome_counts.get(*id).copied().unwrap_or(0), (*id).clone())
                        })
                        .cloned()
                });

            let (actor_id, victim_id, supplemental_receiver_id) = match (actor_id, victim_id) {
                (Some(actor_id), Some(victim_id)) => (actor_id, victim_id, None),
                (None, _) => {
                    if let Some(victim_id) = external_ids
                        .iter()
                        .filter(|id| welcome_counts.get(*id).copied().unwrap_or(0) < required)
                        .min_by_key(|id| {
                            (welcome_counts.get(*id).copied().unwrap_or(0), (*id).clone())
                        })
                        .cloned()
                    {
                        let actor_id = external_ids
                            .iter()
                            .filter(|id| **id != victim_id)
                            .min_by_key(|id| {
                                (actor_counts.get(*id).copied().unwrap_or(0), (*id).clone())
                            })
                            .cloned()
                            .expect("at least two external devices are active");
                        (actor_id, victim_id, None)
                    } else {
                        let Some((actor_id, victim_id, receiver_id)) =
                            select_supplemental_remove_receiver_roles(
                                active,
                                &external_ids,
                                &actor_counts,
                                &receiver_counts,
                                required,
                                plateau_size,
                            )?
                        else {
                            break;
                        };
                        (actor_id, victim_id, Some(receiver_id))
                    }
                }
                _ => unreachable!("two active external devices always provide a victim"),
            };

            let victim = active
                .iter()
                .find(|worker| worker.id == victim_id)
                .cloned()
                .expect("selected remove/rejoin victim remains active");
            let actor = active
                .iter()
                .find(|worker| worker.id == actor_id)
                .cloned()
                .expect("selected external actor remains active");
            let receiver_ids = external_ids
                .iter()
                .filter(|id| **id != actor_id && **id != victim_id)
                .cloned()
                .collect::<Vec<_>>();
            if let Some(required_receiver_id) = supplemental_receiver_id.as_ref() {
                debug_assert!(receiver_ids.contains(required_receiver_id));
            }
            let cycle_result = run_single_remove_rejoin_cycle(
                http,
                active,
                idle,
                fanout,
                process_pending_fanout,
                plateau_size,
                max_commit_receive_samples_per_plateau,
                commit_receive_sampling_seed,
                plateau_index,
                &victim,
                &actor,
            )
            .await;
            if let Err(error) = cycle_result {
                if recover_remove_rejoin_cycle_failure(
                    http,
                    active,
                    idle,
                    fanout,
                    process_pending_fanout,
                    plateau_size,
                    max_commit_receive_samples_per_plateau,
                    commit_receive_sampling_seed,
                    plateau_index,
                    &victim,
                    &actor,
                    runner_events,
                    &error,
                )
                .await?
                {
                    restore_density_plateau(
                        http,
                        active,
                        idle,
                        fanout,
                        progress,
                        process_pending_fanout,
                        plateau_size,
                        max_commit_receive_samples_per_plateau,
                        commit_receive_sampling_seed,
                        rng,
                        plateau_index,
                        runner_events,
                    )
                    .await?;
                    continue;
                }
                return Err(error);
            }
            if is_external_device(&victim) {
                *welcome_counts.entry(victim.id.clone()).or_default() += 1;
            }
            *actor_counts.entry(actor.id.clone()).or_default() += 1;
            for receiver_id in &receiver_ids {
                *receiver_counts.entry(receiver_id.clone()).or_default() += 1;
            }
            eprintln!(
                "[remove-rejoin] plateau={} victim={} welcome_samples={}/{} actor={} remove_samples={}/{} receivers={:?}",
                plateau_size,
                victim.id,
                welcome_counts.get(&victim.id).copied().unwrap_or(0),
                required,
                actor.id,
                actor_counts[&actor.id],
                required,
                receiver_ids
                    .iter()
                    .map(|id| format!("{}:{}/{}", id, receiver_counts[id], required))
                    .collect::<Vec<_>>(),
            );
        }
        return Ok(());
    }

    let victim_idx = profiled[rng.gen_range(0..profiled.len())];
    let others: Vec<usize> = profiled
        .iter()
        .filter(|&&i| i != victim_idx)
        .copied()
        .collect();
    let actor_idx = others[rng.gen_range(0..others.len())];
    let victim = active[victim_idx].clone();
    let actor = active[actor_idx].clone();
    let cycle_result = run_single_remove_rejoin_cycle(
        http,
        active,
        idle,
        fanout,
        process_pending_fanout,
        plateau_size,
        max_commit_receive_samples_per_plateau,
        commit_receive_sampling_seed,
        plateau_index,
        &victim,
        &actor,
    )
    .await;
    if let Err(error) = cycle_result {
        if recover_remove_rejoin_cycle_failure(
            http,
            active,
            idle,
            fanout,
            process_pending_fanout,
            plateau_size,
            max_commit_receive_samples_per_plateau,
            commit_receive_sampling_seed,
            plateau_index,
            &victim,
            &actor,
            runner_events,
            &error,
        )
        .await?
        {
            return Ok(());
        }
        return Err(error);
    }

    Ok(())
}

fn remove_rejoin_failure_operation(error: &anyhow::Error) -> String {
    if let Some(batch_error) = error.downcast_ref::<BatchFanoutError>() {
        if batch_error.phase.contains("remove") {
            return "remove_commit".to_string();
        }
        if batch_error.phase.contains("add") {
            return "add_commit".to_string();
        }
        return batch_error.operation.clone();
    }

    if let Some(command_error) = error.downcast_ref::<WorkerCommandError>() {
        return match command_error.command {
            "GenerateKeyPackage" => "generate_key_package",
            "RemoveMembers" => "remove_commit",
            "AddMembers" => "add_commit",
            "JoinFromWelcome" => "welcome_receive",
            "ReceiveCommit" | "ProcessPending" => "receive_commit",
            "ShowGroupState" => "show_group_state",
            other => other,
        }
        .to_string();
    }

    "remove_rejoin_cycle".to_string()
}

async fn recover_remove_rejoin_cycle_failure(
    http: &reqwest::Client,
    active: &mut Vec<WorkerSpec>,
    idle: &mut VecDeque<WorkerSpec>,
    fanout: &mut FanoutController,
    process_pending_fanout: bool,
    plateau_size: usize,
    max_commit_receive_samples_per_plateau: usize,
    commit_receive_sampling_seed: u64,
    plateau_index: usize,
    victim: &WorkerSpec,
    actor: &WorkerSpec,
    runner_events: &RunnerEventLog,
    error: &anyhow::Error,
) -> Result<bool> {
    let operation = remove_rejoin_failure_operation(error);
    let cursor = BenchmarkCursor::new(
        plateau_index,
        plateau_size,
        active.len(),
        "remove_rejoin",
        &operation,
    );

    let failed_workers = if error.downcast_ref::<BatchFanoutError>().is_some() {
        let Some(dead_workers) = record_batch_oom_failures(
            runner_events,
            &cursor,
            error,
            "quarantine_remove_rejoin_cycle_and_continue",
            active
                .iter()
                .find(|worker| worker.id != actor.id && worker.id != victim.id),
        )
        .await?
        else {
            return Ok(false);
        };
        dead_workers
    } else if let Some(command_error) = error.downcast_ref::<WorkerCommandError>() {
        let Some(failed_worker) = active
            .iter()
            .chain(idle.iter())
            .chain([victim, actor])
            .find(|worker| worker.id == command_error.worker_id)
            .cloned()
        else {
            return Ok(false);
        };
        if !record_profiled_worker_failure(
            runner_events,
            &cursor,
            &failed_worker,
            error,
            "quarantine_remove_rejoin_cycle_and_continue",
            active
                .iter()
                .find(|worker| worker.id != failed_worker.id && worker.id != victim.id),
        )
        .await?
        {
            return Ok(false);
        }
        vec![failed_worker]
    } else {
        return Ok(false);
    };

    let failed_ids = failed_workers
        .iter()
        .map(|worker| worker.id.clone())
        .collect::<HashSet<_>>();
    let mut quarantined = failed_workers;
    for participant in [victim, actor] {
        if !quarantined.iter().any(|worker| worker.id == participant.id) {
            quarantined.push(participant.clone());
        }
    }

    // A failure can occur while either the remove or re-add commit is in
    // flight. Removing both cycle participants gives the surviving members a
    // single unambiguous state; healthy participants can rejoin later.
    evict_oom_group_members(
        http,
        active,
        &quarantined,
        fanout,
        process_pending_fanout,
        plateau_size,
        max_commit_receive_samples_per_plateau,
        commit_receive_sampling_seed,
        runner_events,
    )
    .await?;

    let quarantined_ids = quarantined
        .iter()
        .map(|worker| worker.id.as_str())
        .collect::<HashSet<_>>();
    idle.retain(|worker| !quarantined_ids.contains(worker.id.as_str()));
    for participant in [victim, actor] {
        if !failed_ids.contains(&participant.id) {
            idle.push_back(participant.clone());
        }
    }

    eprintln!(
        "[remove-rejoin] plateau={} recovered from failure; failed={:?} healthy_cycle_participants_requeued={:?} active={} idle={}",
        plateau_size,
        failed_ids,
        [victim, actor]
            .iter()
            .filter(|worker| !failed_ids.contains(&worker.id))
            .map(|worker| worker.id.as_str())
            .collect::<Vec<_>>(),
        active.len(),
        idle.len()
    );
    Ok(true)
}

async fn run_single_remove_rejoin_cycle(
    http: &reqwest::Client,
    active: &mut Vec<WorkerSpec>,
    idle: &mut VecDeque<WorkerSpec>,
    fanout: &mut FanoutController,
    process_pending_fanout: bool,
    plateau_size: usize,
    max_commit_receive_samples_per_plateau: usize,
    commit_receive_sampling_seed: u64,
    plateau_index: usize,
    victim: &WorkerSpec,
    actor: &WorkerSpec,
) -> Result<()> {
    let remove_victim_active = active.len();

    eprintln!(
        "[remove-rejoin] plateau={} victim={} actor={} active={}",
        plateau_size, victim.id, actor.id, remove_victim_active
    );

    // ── Generate KeyPackage (profiled, uploaded to DS) ──
    let kp_cursor = BenchmarkCursor::new(
        plateau_index,
        plateau_size,
        remove_victim_active,
        "remove_rejoin",
        "generate_key_package",
    );
    let kp_context = WorkerCommandContext::with_metadata(
        victim,
        &Command::GenerateKeyPackage,
        None,
        Some("remove_rejoin.generate_key_package"),
        Some(&kp_cursor),
    );
    let _ =
        send_command_with_context(http, victim, &Command::GenerateKeyPackage, &kp_context).await?;
    // KeyPackage is now stored on DS at /keypackage/{victim.id}; AddMembers will fetch it.

    // ── Remove victim ──
    let remove_cursor = BenchmarkCursor::new(
        plateau_index,
        plateau_size,
        remove_victim_active,
        "remove_rejoin",
        "remove_commit",
    );
    let remove_cmd = Command::RemoveMembers {
        members: vec![victim.id.clone()],
    };
    let remove_ctx = WorkerCommandContext::with_metadata(
        actor,
        &remove_cmd,
        None,
        Some("remove_rejoin.remove"),
        Some(&remove_cursor),
    );
    let actor_before = show_group_state(http, actor).await?;
    let expected_after_remove = expected_group_state(
        &actor_before,
        actor_before.epoch + 1,
        actor_before
            .members
            .iter()
            .filter(|m| *m != &victim.id)
            .cloned()
            .collect(),
    );
    send_cmd_expect_ok_fragment_with_context(
        http,
        actor,
        &remove_cmd,
        "removed locally; group commit published",
        &remove_ctx,
    )
    .await?;
    if process_pending_fanout {
        process_pending_commit_expect(
            http,
            actor,
            ExpectedReceiveCommitState::Group(expected_after_remove.clone()),
            "remove_rejoin.actor_process_pending",
            Some(&remove_cursor),
        )
        .await?;
    } else {
        receive_commit_expect(
            http,
            actor,
            "own commit accepted from DS",
            ExpectedReceiveCommitState::Group(expected_after_remove.clone()),
            "remove_rejoin.actor_receive_commit",
            Some(&remove_cursor),
        )
        .await?;
    }

    // Fanout RemoveCommit receivers
    {
        let recipients: Vec<WorkerSpec> = active
            .iter()
            .filter(|w| w.id != victim.id && w.id != actor.id)
            .cloned()
            .collect();
        let expected_state = ExpectedReceiveCommitState::Group(expected_after_remove.clone());
        let expected_ep = expected_epoch(&expected_state);
        let (sampled_ids, sample_index_map, sample_count) = build_commit_receive_sampling_map(
            &recipients,
            max_commit_receive_samples_per_plateau,
            commit_receive_sampling_seed,
            plateau_size,
            expected_after_remove.epoch,
            0,
        );
        let fanout_phase = if process_pending_fanout {
            "remove_rejoin.fanout_process_pending_remove"
        } else {
            "remove_rejoin.fanout_receive_commit_remove"
        };
        let commands_by_physical = build_batch_commands(&recipients, |worker| {
            let sampled = sampled_ids.contains(&worker.id);
            BatchFanoutCommand {
                client_id: worker.id.clone(),
                request_id: None,
                command: if process_pending_fanout {
                    Command::ProcessPending {
                        kinds: Some(vec![PendingKind::Commits]),
                        max_messages: None,
                        expected_epoch: expected_ep,
                        profile: sampled,
                        commit_create_op: Some("remove".to_string()),
                        commit_receive_sampling_policy: Some("edge_middle_seeded_v1".to_string()),
                        commit_receive_sampling_seed: Some(commit_receive_sampling_seed),
                        commit_receive_sample_index: sample_index_map.get(&worker.id).copied(),
                        commit_receive_sample_count: Some(sample_count),
                        commit_receive_population_size: Some(recipients.len()),
                    }
                } else {
                    Command::ReceiveCommit {
                        profile: sampled,
                        commit_create_op: Some("remove".to_string()),
                        commit_receive_sampling_policy: Some("edge_middle_seeded_v1".to_string()),
                        commit_receive_sampling_seed: Some(commit_receive_sampling_seed),
                        commit_receive_sample_index: sample_index_map.get(&worker.id).copied(),
                        commit_receive_sample_count: Some(sample_count),
                        commit_receive_population_size: Some(recipients.len()),
                    }
                },
                expected_epoch: expected_ep,
                phase: Some(fanout_phase.to_string()),
                profile: sampled.then_some(true),
                benchmark_plateau_index: None,
                benchmark_target_size: None,
                benchmark_active_size: None,
                benchmark_phase: None,
                benchmark_operation: None,
                benchmark_operation_seq: None,
                benchmark_payload_size: None,
                membership_batch_requested: None,
                membership_batch_effective: None,
                membership_batch_group_cap: None,
                membership_batch_transition_cap: None,
                membership_batch_source: None,
            }
            .with_benchmark_cursor(&remove_cursor)
        });
        let expected_by_client = recipients
            .iter()
            .map(|w| (w.id.clone(), expected_state.clone()))
            .collect::<HashMap<_, _>>();
        batch_fanout_workers(
            http,
            fanout_phase,
            plateau_size,
            "receive_commit",
            &recipients,
            fanout,
            &commands_by_physical,
            Some(&expected_by_client),
        )
        .await?;
    }

    // Move victim to idle
    active.retain(|w| w.id != victim.id);
    idle.push_back(victim.clone());

    // ── Re-add victim ──
    let re_add_active = active.len() + 1;
    let add_cursor = BenchmarkCursor::new(
        plateau_index,
        plateau_size,
        re_add_active,
        "remove_rejoin",
        "add_commit",
    )
    .with_membership_batch(&MembershipBatchDecision {
        requested: 1,
        effective: 1,
        group_cap: membership_batch_group_cap(plateau_size),
        transition_cap: 1,
        source: "remove_rejoin",
    });
    let add_cmd = Command::AddMembers {
        members: vec![victim.id.clone()],
    };
    let add_ctx = WorkerCommandContext::with_metadata(
        actor,
        &add_cmd,
        None,
        Some("remove_rejoin.add"),
        Some(&add_cursor),
    );
    let actor_before = show_group_state(http, actor).await?;
    let mut expected_members = actor_before.members.clone();
    expected_members.push(victim.id.clone());
    expected_members.sort();
    let expected_after_add =
        expected_group_state(&actor_before, actor_before.epoch + 1, expected_members);
    send_cmd_expect_ok_fragment_with_context(
        http,
        actor,
        &add_cmd,
        "added locally in one commit",
        &add_ctx,
    )
    .await?;
    if process_pending_fanout {
        process_pending_commit_expect(
            http,
            actor,
            ExpectedReceiveCommitState::Group(expected_after_add.clone()),
            "remove_rejoin.actor_process_pending_add",
            Some(&add_cursor),
        )
        .await?;
    } else {
        receive_commit_expect(
            http,
            actor,
            "own commit accepted from DS",
            ExpectedReceiveCommitState::Group(expected_after_add.clone()),
            "remove_rejoin.actor_receive_commit_add",
            Some(&add_cursor),
        )
        .await?;
    }

    // Fanout AddCommit receivers
    {
        let recipients: Vec<WorkerSpec> = active
            .iter()
            .filter(|w| w.id != actor.id)
            .cloned()
            .collect();
        let expected_state = ExpectedReceiveCommitState::Group(expected_after_add.clone());
        let expected_ep = expected_epoch(&expected_state);
        let (sampled_ids, sample_index_map, sample_count) = build_commit_receive_sampling_map(
            &recipients,
            max_commit_receive_samples_per_plateau,
            commit_receive_sampling_seed,
            plateau_size,
            expected_after_add.epoch,
            0,
        );
        let fanout_phase = if process_pending_fanout {
            "remove_rejoin.fanout_process_pending_add"
        } else {
            "remove_rejoin.fanout_receive_commit_add"
        };
        let commands_by_physical = build_batch_commands(&recipients, |worker| {
            let sampled = sampled_ids.contains(&worker.id);
            BatchFanoutCommand {
                client_id: worker.id.clone(),
                request_id: None,
                command: if process_pending_fanout {
                    Command::ProcessPending {
                        kinds: Some(vec![PendingKind::Commits]),
                        max_messages: None,
                        expected_epoch: expected_ep,
                        profile: sampled,
                        commit_create_op: Some("add".to_string()),
                        commit_receive_sampling_policy: Some("edge_middle_seeded_v1".to_string()),
                        commit_receive_sampling_seed: Some(commit_receive_sampling_seed),
                        commit_receive_sample_index: sample_index_map.get(&worker.id).copied(),
                        commit_receive_sample_count: Some(sample_count),
                        commit_receive_population_size: Some(recipients.len()),
                    }
                } else {
                    Command::ReceiveCommit {
                        profile: sampled,
                        commit_create_op: Some("add".to_string()),
                        commit_receive_sampling_policy: Some("edge_middle_seeded_v1".to_string()),
                        commit_receive_sampling_seed: Some(commit_receive_sampling_seed),
                        commit_receive_sample_index: sample_index_map.get(&worker.id).copied(),
                        commit_receive_sample_count: Some(sample_count),
                        commit_receive_population_size: Some(recipients.len()),
                    }
                },
                expected_epoch: expected_ep,
                phase: Some(fanout_phase.to_string()),
                profile: sampled.then_some(true),
                benchmark_plateau_index: None,
                benchmark_target_size: None,
                benchmark_active_size: None,
                benchmark_phase: None,
                benchmark_operation: None,
                benchmark_operation_seq: None,
                benchmark_payload_size: None,
                membership_batch_requested: None,
                membership_batch_effective: None,
                membership_batch_group_cap: None,
                membership_batch_transition_cap: None,
                membership_batch_source: None,
            }
            .with_benchmark_cursor(&add_cursor)
        });
        let expected_by_client = recipients
            .iter()
            .map(|w| (w.id.clone(), expected_state.clone()))
            .collect::<HashMap<_, _>>();
        batch_fanout_workers(
            http,
            fanout_phase,
            plateau_size,
            "receive_commit",
            &recipients,
            fanout,
            &commands_by_physical,
            Some(&expected_by_client),
        )
        .await?;
    }

    // ── Victim processes Welcome via JoinFromWelcome (fetches from DS) ──
    let welcome_cursor = BenchmarkCursor::new(
        plateau_index,
        plateau_size,
        re_add_active,
        "remove_rejoin",
        "welcome_receive",
    );
    let welcome_cmd = Command::JoinFromWelcome;
    let welcome_ctx = WorkerCommandContext::with_metadata(
        victim,
        &welcome_cmd,
        None,
        Some("remove_rejoin.process_welcome"),
        Some(&welcome_cursor),
    );
    send_cmd_expect_ok_fragment_with_context(
        http,
        victim,
        &welcome_cmd,
        "joined from welcome",
        &welcome_ctx,
    )
    .await?;
    idle.retain(|w| w.id != victim.id);
    active.push(victim.clone());

    eprintln!(
        "[remove-rejoin] plateau={} done: victim={} active={} idle={}",
        plateau_size,
        victim.id,
        active.len(),
        idle.len()
    );
    Ok(())
}

async fn run_update_phase(
    http: &reqwest::Client,
    active: &mut Vec<WorkerSpec>,
    plateau_size: usize,
    update_rounds: usize,
    max_update_samples_per_plateau: usize,
    max_commit_receive_samples_per_plateau: usize,
    commit_receive_sampling_seed: u64,
    fanout: &mut FanoutController,
    progress: &mut Progress,
    process_pending_fanout: bool,
    external_coverage_lane: bool,
    min_profiled_samples_per_operation: usize,
    rng: &mut StdRng,
    plateau_index: usize,
    runner_events: &RunnerEventLog,
) -> Result<()> {
    let base_updates =
        update_ops_for_plateau(plateau_size, update_rounds, max_update_samples_per_plateau);
    let initial_profiled_indices = active_profiled_indices(active);
    if initial_profiled_indices.is_empty() {
        eprintln!(
            "\n[plateau {}] update phase skipped: no profile-enabled members",
            plateau_size
        );
        return Ok(());
    }

    let required_actor_ids = if external_coverage_lane {
        let mut required = active_external_indices(active);
        if min_profiled_samples_per_operation == 0 {
            let non_external_profiled_indices = active_profiled_non_external_indices(active);
            if !non_external_profiled_indices.is_empty() {
                let sampled_pos = rng.gen_range(0..non_external_profiled_indices.len());
                required.push(non_external_profiled_indices[sampled_pos]);
            }
        }
        required
            .into_iter()
            .map(|idx| active[idx].id.clone())
            .collect::<Vec<_>>()
    } else {
        Vec::new()
    };

    let required_density_updates = required_actor_ids
        .len()
        .saturating_mul(min_profiled_samples_per_operation.max(1));
    let total_updates = base_updates.max(required_density_updates);
    if total_updates == 0 {
        return Ok(());
    }

    eprintln!(
        "\n[plateau {}] update phase: {} successful self-update cycles",
        plateau_size, total_updates
    );

    let external_actor_id = (!external_coverage_lane)
        .then(|| {
            initial_profiled_indices
                .iter()
                .find(|&&idx| is_external_device(&active[idx]))
                .map(|&idx| active[idx].id.clone())
        })
        .flatten();
    let external_actor_seq = external_actor_id
        .as_ref()
        .map(|_| rng.gen_range(0..total_updates));

    let density_enabled = external_coverage_lane && min_profiled_samples_per_operation > 0;
    let mut density_successes = HashMap::<String, usize>::new();
    let mut seq_no = 0usize;
    loop {
        let density_actor_id = density_enabled
            .then(|| {
                least_sampled_active_external_id(
                    active,
                    &density_successes,
                    min_profiled_samples_per_operation,
                )
            })
            .flatten();
        if density_enabled && density_actor_id.is_none() {
            break;
        }
        if !density_enabled && seq_no >= total_updates {
            break;
        }
        let profiled_indices = active_profiled_indices(active);
        if profiled_indices.is_empty() {
            eprintln!(
                "\n[plateau {}] update phase stopped after OOM attrition: no profile-enabled members",
                plateau_size
            );
            return Ok(());
        }
        let actor_idx = density_actor_id
            .as_ref()
            .or_else(|| required_actor_ids.get(seq_no))
            .and_then(|actor_id| active.iter().position(|worker| worker.id == *actor_id))
            .or_else(|| {
                if external_actor_seq == Some(seq_no) {
                    external_actor_id.as_ref().and_then(|actor_id| {
                        active.iter().position(|worker| worker.id == *actor_id)
                    })
                } else {
                    None
                }
            })
            .unwrap_or_else(|| {
                profiled_indices
                    [sampled_member_index(profiled_indices.len(), total_updates, seq_no)]
            });
        let actor = active[actor_idx].clone();
        let cursor = BenchmarkCursor::new(
            plateau_index,
            plateau_size,
            active.len(),
            "update",
            "self_update",
        )
        .at_operation(seq_no + 1, None);
        let actor_before = match show_group_state(http, &actor).await {
            Ok(state) => state,
            Err(error) => {
                let reassigned_to = active.iter().find(|worker| worker.id != actor.id).cloned();
                if record_profiled_worker_failure(
                    runner_events,
                    &cursor,
                    &actor,
                    &error,
                    "evict_update_actor_and_retry",
                    reassigned_to.as_ref(),
                )
                .await?
                {
                    evict_oom_group_members(
                        http,
                        active,
                        &[actor],
                        fanout,
                        process_pending_fanout,
                        plateau_size,
                        max_commit_receive_samples_per_plateau,
                        commit_receive_sampling_seed,
                        runner_events,
                    )
                    .await?;
                    continue;
                }
                return Err(error);
            }
        };
        let expected_after_commit = expected_group_state(
            &actor_before,
            actor_before.epoch + 1,
            actor_before.members.clone(),
        );

        let update_command = Command::SelfUpdate;
        let update_context = WorkerCommandContext::with_metadata(
            &actor,
            &update_command,
            None,
            Some("update.create"),
            Some(&cursor),
        );
        if let Err(error) = send_cmd_expect_ok_fragment_with_context(
            http,
            &actor,
            &update_command,
            "self_update commit published to group",
            &update_context,
        )
        .await
        {
            let reassigned_to = active.iter().find(|worker| worker.id != actor.id).cloned();
            if record_profiled_worker_failure(
                runner_events,
                &cursor,
                &actor,
                &error,
                "evict_update_actor_and_retry",
                reassigned_to.as_ref(),
            )
            .await?
            {
                evict_oom_group_members(
                    http,
                    active,
                    &[actor],
                    fanout,
                    process_pending_fanout,
                    plateau_size,
                    max_commit_receive_samples_per_plateau,
                    commit_receive_sampling_seed,
                    runner_events,
                )
                .await?;
                continue;
            }
            return Err(error);
        }

        let actor_commit_result = if process_pending_fanout {
            process_pending_commit_expect(
                http,
                &actor,
                ExpectedReceiveCommitState::Group(expected_after_commit.clone()),
                "update.actor_process_pending",
                Some(&cursor),
            )
            .await
        } else {
            receive_commit_expect(
                http,
                &actor,
                "own commit accepted from DS",
                ExpectedReceiveCommitState::Group(expected_after_commit.clone()),
                "update.actor_receive_commit",
                Some(&cursor),
            )
            .await
        };
        if let Err(error) = actor_commit_result {
            let reassigned_to = active.iter().find(|worker| worker.id != actor.id).cloned();
            if record_profiled_worker_failure(
                runner_events,
                &cursor,
                &actor,
                &error,
                "evict_update_actor_and_retry",
                reassigned_to.as_ref(),
            )
            .await?
            {
                evict_oom_group_members(
                    http,
                    active,
                    &[actor],
                    fanout,
                    process_pending_fanout,
                    plateau_size,
                    max_commit_receive_samples_per_plateau,
                    commit_receive_sampling_seed,
                    runner_events,
                )
                .await?;
                continue;
            }
            return Err(error);
        }

        let recipients: Vec<WorkerSpec> = active
            .iter()
            .enumerate()
            .filter(|(idx, _)| *idx != actor_idx)
            .map(|(_, worker)| worker.clone())
            .collect();
        let expected_state = ExpectedReceiveCommitState::Group(expected_after_commit.clone());
        let expected_ep = expected_epoch(&expected_state);
        let fanout_phase = if process_pending_fanout {
            "update.fanout_process_pending"
        } else {
            "update.fanout_receive_commit"
        };
        let (sampled_ids, sample_index_map, sample_count) = build_commit_receive_sampling_map(
            &recipients,
            max_commit_receive_samples_per_plateau,
            commit_receive_sampling_seed,
            plateau_size,
            expected_after_commit.epoch,
            seq_no,
        );
        let commands_by_physical = build_batch_commands(&recipients, |worker| {
            let sampled = sampled_ids.contains(&worker.id);
            let sample_index = sample_index_map.get(&worker.id).copied();
            BatchFanoutCommand {
                client_id: worker.id.clone(),
                request_id: None,
                command: if process_pending_fanout {
                    Command::ProcessPending {
                        kinds: Some(vec![PendingKind::Commits]),
                        max_messages: None,
                        expected_epoch: expected_ep,
                        profile: sampled,
                        commit_create_op: Some("self_update".to_string()),
                        commit_receive_sampling_policy: Some("edge_middle_seeded_v1".to_string()),
                        commit_receive_sampling_seed: Some(commit_receive_sampling_seed),
                        commit_receive_sample_index: sample_index,
                        commit_receive_sample_count: Some(sample_count),
                        commit_receive_population_size: Some(recipients.len()),
                    }
                } else {
                    Command::ReceiveCommit {
                        profile: sampled,
                        commit_create_op: Some("self_update".to_string()),
                        commit_receive_sampling_policy: Some("edge_middle_seeded_v1".to_string()),
                        commit_receive_sampling_seed: Some(commit_receive_sampling_seed),
                        commit_receive_sample_index: sample_index,
                        commit_receive_sample_count: Some(sample_count),
                        commit_receive_population_size: Some(recipients.len()),
                    }
                },
                expected_epoch: expected_ep,
                phase: Some(fanout_phase.to_string()),
                profile: sampled.then_some(true),
                benchmark_plateau_index: None,
                benchmark_target_size: None,
                benchmark_active_size: None,
                benchmark_phase: None,
                benchmark_operation: None,
                benchmark_operation_seq: None,
                benchmark_payload_size: None,
                membership_batch_requested: None,
                membership_batch_effective: None,
                membership_batch_group_cap: None,
                membership_batch_transition_cap: None,
                membership_batch_source: None,
            }
            .with_benchmark_cursor(&cursor)
        });
        let expected_by_client = recipients
            .iter()
            .map(|worker| (worker.id.clone(), expected_state.clone()))
            .collect::<HashMap<_, _>>();
        let fanout_result = batch_fanout_workers(
            http,
            "update",
            plateau_size,
            "receive_commit",
            &recipients,
            fanout,
            &commands_by_physical,
            Some(&expected_by_client),
        )
        .await;
        if let Err(error) = fanout_result {
            if let Some(dead_workers) = record_batch_oom_failures(
                runner_events,
                &cursor,
                &error,
                "evict_update_recipient_and_retry",
                Some(&actor),
            )
            .await?
            {
                evict_oom_group_members(
                    http,
                    active,
                    &dead_workers,
                    fanout,
                    process_pending_fanout,
                    plateau_size,
                    max_commit_receive_samples_per_plateau,
                    commit_receive_sampling_seed,
                    runner_events,
                )
                .await?;
                continue;
            }
            return Err(error);
        }

        progress.tick(&format!(
            "plateau {} update {}/{} actor={}",
            plateau_size,
            seq_no + 1,
            total_updates,
            actor.id
        ));
        if density_enabled {
            *density_successes.entry(actor.id.clone()).or_default() += 1;
        }
        seq_no += 1;
    }

    Ok(())
}

async fn run_application_phase(
    http: &reqwest::Client,
    active: &mut Vec<WorkerSpec>,
    plateau_size: usize,
    app_rounds: usize,
    max_app_samples_per_payload: usize,
    max_commit_receive_samples_per_plateau: usize,
    commit_receive_sampling_seed: u64,
    payload_sizes: &PayloadSizes,
    fanout: &mut FanoutController,
    progress: &mut Progress,
    external_coverage_lane: bool,
    min_profiled_samples_per_operation: usize,
    rng: &mut StdRng,
    plateau_index: usize,
    runner_events: &RunnerEventLog,
) -> Result<()> {
    if active.len() < 2 {
        eprintln!(
            "\n[plateau {}] application phase skipped: fewer than 2 active members",
            plateau_size
        );
        return Ok(());
    }

    let per_payload_count =
        app_sends_per_payload_for_plateau(plateau_size, app_rounds, max_app_samples_per_payload);
    if per_payload_count == 0 {
        return Ok(());
    }

    if active_profiled_indices(active).is_empty() {
        eprintln!(
            "\n[plateau {}] application phase skipped: no profile-enabled members",
            plateau_size
        );
        return Ok(());
    }

    for payload_source in payload_sizes.shuffled_sources(rng) {
        let profiled_at_payload_start = active_profiled_indices(active);
        let non_external_at_payload_start = active_profiled_non_external_indices(active);
        let required_actor_ids = if external_coverage_lane {
            let mut required = active_external_indices(active);
            if min_profiled_samples_per_operation == 0 && !non_external_at_payload_start.is_empty()
            {
                let sampled_pos = rng.gen_range(0..non_external_at_payload_start.len());
                required.push(non_external_at_payload_start[sampled_pos]);
            }
            required
                .into_iter()
                .map(|idx| active[idx].id.clone())
                .collect::<Vec<_>>()
        } else {
            Vec::new()
        };
        let external_actor_id = profiled_at_payload_start
            .iter()
            .find(|&&idx| is_external_device(&active[idx]))
            .map(|&idx| active[idx].id.clone());
        let force_external_receive_sample = !external_coverage_lane
            && external_actor_id.is_some()
            && !non_external_at_payload_start.is_empty();
        let planned_per_payload_count = if external_coverage_lane {
            per_payload_count.max(
                required_actor_ids
                    .len()
                    .saturating_mul(min_profiled_samples_per_operation.max(1)),
            )
        } else {
            per_payload_count + usize::from(force_external_receive_sample && per_payload_count == 1)
        };
        eprintln!(
            "\n[plateau {}] application phase: {} successful sends at {}",
            plateau_size,
            planned_per_payload_count,
            payload_source.phase_label()
        );

        let external_actor_seq = if external_coverage_lane {
            None
        } else {
            external_actor_id
                .as_ref()
                .map(|_| rng.gen_range(0..planned_per_payload_count))
        };
        let forced_non_external_actor_seq =
            if force_external_receive_sample && planned_per_payload_count > 1 {
                external_actor_seq.map(|seq| (seq + 1) % planned_per_payload_count)
            } else {
                None
            };

        let density_enabled = external_coverage_lane && min_profiled_samples_per_operation > 0;
        let mut density_successes = HashMap::<String, usize>::new();
        let mut seq_no = 0usize;
        loop {
            let density_actor_id = density_enabled
                .then(|| {
                    least_sampled_active_external_id(
                        active,
                        &density_successes,
                        min_profiled_samples_per_operation,
                    )
                })
                .flatten();
            if density_enabled && density_actor_id.is_none() {
                break;
            }
            if !density_enabled && seq_no >= planned_per_payload_count {
                break;
            }
            if active.len() < 2 {
                eprintln!(
                    "\n[plateau {}] application phase stopped after OOM attrition: fewer than 2 active members",
                    plateau_size
                );
                return Ok(());
            }
            let profiled_indices = active_profiled_indices(active);
            if profiled_indices.is_empty() {
                return Ok(());
            }
            let non_external_profiled_indices = active_profiled_non_external_indices(active);
            let actor_idx = density_actor_id
                .as_ref()
                .or_else(|| required_actor_ids.get(seq_no))
                .and_then(|actor_id| active.iter().position(|worker| worker.id == *actor_id))
                .or_else(|| {
                    if external_actor_seq == Some(seq_no) {
                        external_actor_id.as_ref().and_then(|actor_id| {
                            active.iter().position(|worker| worker.id == *actor_id)
                        })
                    } else {
                        None
                    }
                })
                .or_else(|| {
                    (forced_non_external_actor_seq == Some(seq_no)
                        && !non_external_profiled_indices.is_empty())
                    .then(|| {
                        non_external_profiled_indices[sampled_member_index(
                            non_external_profiled_indices.len(),
                            planned_per_payload_count,
                            seq_no,
                        )]
                    })
                })
                .unwrap_or_else(|| {
                    profiled_indices[sampled_member_index(
                        profiled_indices.len(),
                        planned_per_payload_count,
                        seq_no,
                    )]
                });
            let actor = active[actor_idx].clone();
            let payload_size = payload_source.sample(rng);
            let payload =
                deterministic_payload(payload_size, plateau_size, payload_size, seq_no, &actor.id);
            let cursor = BenchmarkCursor::new(
                plateau_index,
                plateau_size,
                active.len(),
                "application",
                "send_application_message",
            )
            .at_operation(seq_no + 1, Some(payload_size));

            let application_command = Command::SendApplicationMessage { message: payload };
            let application_context = WorkerCommandContext::with_metadata(
                &actor,
                &application_command,
                None,
                Some("application.create"),
                Some(&cursor),
            );
            if let Err(error) = send_cmd_expect_ok_fragment_with_context(
                http,
                &actor,
                &application_command,
                "application message broadcast to group",
                &application_context,
            )
            .await
            {
                let reassigned_to = active.iter().find(|worker| worker.id != actor.id).cloned();
                if record_profiled_worker_failure(
                    runner_events,
                    &cursor,
                    &actor,
                    &error,
                    "evict_application_actor_and_retry",
                    reassigned_to.as_ref(),
                )
                .await?
                {
                    evict_oom_group_members(
                        http,
                        active,
                        &[actor],
                        fanout,
                        false,
                        plateau_size,
                        max_commit_receive_samples_per_plateau,
                        commit_receive_sampling_seed,
                        runner_events,
                    )
                    .await?;
                    continue;
                }
                return Err(error);
            }

            let recipient_indices: Vec<usize> =
                (0..active.len()).filter(|&j| j != actor_idx).collect();

            let profiled_recipient_indices: Vec<usize> = profiled_indices
                .iter()
                .copied()
                .filter(|&i| i != actor_idx)
                .collect();

            let sampled_worker_id = if profiled_recipient_indices.is_empty() {
                String::new()
            } else {
                let sampled_pos = sampled_member_index(
                    profiled_recipient_indices.len(),
                    planned_per_payload_count,
                    seq_no,
                );
                active[profiled_recipient_indices[sampled_pos]].id.clone()
            };

            let recipient_workers: Vec<WorkerSpec> = recipient_indices
                .iter()
                .map(|recipient_idx| active[*recipient_idx].clone())
                .collect();

            let commands_by_physical = build_batch_commands(&recipient_workers, |worker| {
                let should_profile = worker.id == sampled_worker_id || is_external_device(worker);
                BatchFanoutCommand {
                    client_id: worker.id.clone(),
                    request_id: None,
                    command: Command::ReceiveApplicationMessage {
                        profile: should_profile,
                    },
                    expected_epoch: None,
                    phase: Some("application.fanout_receive_application_message".to_string()),
                    profile: should_profile.then_some(true),
                    benchmark_plateau_index: None,
                    benchmark_target_size: None,
                    benchmark_active_size: None,
                    benchmark_phase: None,
                    benchmark_operation: None,
                    benchmark_operation_seq: None,
                    benchmark_payload_size: None,
                    membership_batch_requested: None,
                    membership_batch_effective: None,
                    membership_batch_group_cap: None,
                    membership_batch_transition_cap: None,
                    membership_batch_source: None,
                }
                .with_benchmark_cursor(&cursor)
            });
            let receive_result = batch_fanout_workers(
                http,
                "application",
                plateau_size,
                "receive_application_message",
                &recipient_workers,
                fanout,
                &commands_by_physical,
                None,
            )
            .await;
            if let Err(error) = receive_result {
                if let Some(dead_workers) = record_batch_oom_failures(
                    runner_events,
                    &cursor,
                    &error,
                    "evict_application_recipient_and_retry",
                    Some(&actor),
                )
                .await?
                {
                    evict_oom_group_members(
                        http,
                        active,
                        &dead_workers,
                        fanout,
                        false,
                        plateau_size,
                        max_commit_receive_samples_per_plateau,
                        commit_receive_sampling_seed,
                        runner_events,
                    )
                    .await?;
                    continue;
                }
                return Err(error);
            }

            progress.tick(&format!(
                "plateau {} app payload={} {}/{} actor={}",
                plateau_size,
                payload_size,
                seq_no + 1,
                planned_per_payload_count,
                actor.id
            ));
            if density_enabled {
                *density_successes.entry(actor.id.clone()).or_default() += 1;
            }
            seq_no += 1;
        }
    }

    Ok(())
}

pub fn aggregate_csv(
    run_dir: &Path,
    worker_ids: &[String],
    provided_layout: &Option<WorkerLayout>,
) -> Result<u64> {
    materialize_runner_profile_events(run_dir)?;

    let csv_path = run_dir.join("events.csv");
    let tmp_path = run_dir.join("events.csv.tmp");
    let mut wtr = csv::Writer::from_path(&tmp_path)?;

    let layout_path = run_dir.join("worker_layout.json");
    let layout: Option<WorkerLayout> = if let Some(l) = provided_layout {
        Some(l.clone())
    } else if layout_path.exists() {
        fs::read_to_string(&layout_path)
            .ok()
            .and_then(|c| serde_json::from_str(&c).ok())
    } else {
        None
    };

    let profile_enabled_ids: std::collections::HashSet<&str> = if let Some(ref l) = layout {
        l.clients
            .iter()
            .filter(|c| c.profile_enabled)
            .map(|c| c.client_id.as_str())
            .collect()
    } else {
        worker_ids.iter().map(|s| s.as_str()).collect()
    };

    // Build per-client metadata lookup from layout clients
    let mut client_meta: std::collections::HashMap<&str, &WorkerLayoutClient> =
        std::collections::HashMap::new();
    let mut physical_meta: std::collections::HashMap<&str, &WorkerLayoutPhysicalWorker> =
        std::collections::HashMap::new();
    if let Some(ref l) = layout {
        for c in &l.clients {
            client_meta.insert(c.client_id.as_str(), c);
        }
        for pw in &l.physical_workers {
            physical_meta.insert(pw.physical_worker_id.as_str(), pw);
        }
    }

    let mut aggregate_worker_ids: Vec<String> = Vec::new();
    let mut aggregate_seen: std::collections::HashSet<String> = std::collections::HashSet::new();
    if let Some(ref l) = layout {
        for client in &l.clients {
            if client.profile_enabled && aggregate_seen.insert(client.client_id.clone()) {
                aggregate_worker_ids.push(client.client_id.clone());
            }
        }
    }
    for worker_id in worker_ids {
        if aggregate_seen.insert(worker_id.clone()) {
            aggregate_worker_ids.push(worker_id.clone());
        }
    }

    #[derive(Serialize)]
    struct CsvRow<'a> {
        client_id: &'a str,
        worker_id: &'a str,
        container_mode: &'a str,
        execution_backend: String,
        device_kind: String,
        profile_schema_version: Option<u32>,
        ts_unix_ns: u128,
        op: String,
        span_name: Option<String>,
        span_id: Option<u64>,
        parent_span_id: Option<u64>,
        parent_operation: Option<String>,
        span_inclusive: Option<bool>,
        runner_event_kind: Option<String>,
        failed_worker_id: Option<String>,
        failed_physical_worker_id: Option<String>,
        failure_class: Option<String>,
        failure_detail: Option<String>,
        failure_evidence_source: Option<String>,
        failure_evidence_detail: Option<String>,
        failure_action: Option<String>,
        reassigned_to_worker_id: Option<String>,
        memory_model: Option<String>,
        docker_memory_limit: Option<&'a str>,
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
        benchmark_plateau_index: Option<usize>,
        benchmark_target_size: Option<usize>,
        benchmark_active_size: Option<usize>,
        benchmark_phase: Option<String>,
        benchmark_operation: Option<String>,
        benchmark_operation_seq: Option<usize>,
        benchmark_payload_size: Option<usize>,
        membership_batch_requested: Option<usize>,
        membership_batch_effective: Option<usize>,
        membership_batch_group_cap: Option<usize>,
        membership_batch_transition_cap: Option<usize>,
        membership_batch_source: Option<String>,
        configured_payload_label: Option<String>,
        wall_ns: u128,
        cpu_thread_ns: Option<u128>,
        cpu_process_ns: u128,
        cpu_envelope_utilization: Option<f64>,
        cpu_throttled_time_ratio: Option<f64>,
        cpu_nr_periods_delta: Option<u64>,
        cpu_nr_throttled_delta: Option<u64>,
        cpu_throttled_usec_delta: Option<u128>,
        cpu_throttled_period_fraction: Option<f64>,
        cpu_nr_periods_cumulative: Option<u64>,
        cpu_nr_throttled_cumulative: Option<u64>,
        cpu_throttled_usec_cumulative: Option<u128>,
        cpu_throttled_period_fraction_cumulative: Option<f64>,
        cpu_throttled_period_threshold: Option<f64>,
        cpu_throttled_period_threshold_crossing: Option<bool>,
        alloc_bytes: Option<u64>,
        alloc_count: Option<u64>,
        alloc_measurement_scope: Option<String>,
        l1d_cache_accesses: Option<u64>,
        l1d_cache_misses: Option<u64>,
        l1d_measurement_scope: Option<String>,
        l1d_cache_status: Option<String>,
        l1d_measured_thread_count: Option<usize>,
        l1d_discovered_thread_count: Option<usize>,
        l1d_multiplexed_thread_count: Option<usize>,
        ram_rss_delta_bytes: Option<i64>,
        ram_rss_utilization: Option<f64>,
        artifact_size_bytes: Option<usize>,
        welcome_bytes: Option<usize>,
        ratchet_tree_bytes: Option<usize>,
        welcome_plus_ratchet_tree_bytes: Option<usize>,
        group_info_bytes: Option<usize>,
        group_info_plaintext_bytes: Option<usize>,
        group_info_ciphertext_bytes: Option<usize>,
        encrypted_group_info_bytes: Option<usize>,
        encrypted_secrets_count: Option<usize>,
        group_epoch: Option<u64>,
        tree_size: Option<u32>,
        tree_height: Option<u32>,
        tree_leaf_count: Option<u32>,
        tree_node_count: Option<u32>,
        operation_family: Option<String>,
        member_count: Option<usize>,
        member_count_before: Option<usize>,
        member_count_after: Option<usize>,
        invitee_count: Option<isize>,
        added_members_count: Option<usize>,
        removed_members_count: Option<usize>,
        removed_leaf_indices: Option<String>,
        removed_right_edge_count: Option<usize>,
        rightmost_removed_leaf: Option<u32>,
        removed_right_edge_suffix_count: Option<usize>,
        right_edge_suffix_fully_removed: Option<bool>,
        tree_truncated: Option<bool>,
        truncated_levels_count: Option<usize>,
        tree_size_before: Option<u32>,
        tree_size_after: Option<u32>,
        tree_leaf_count_before: Option<u32>,
        tree_leaf_count_after: Option<u32>,
        tree_node_count_before: Option<u32>,
        tree_node_count_after: Option<u32>,
        add_commit_mode: Option<String>,
        remove_commit_mode: Option<String>,
        commit_path_policy: Option<String>,
        force_self_update: Option<bool>,
        update_path_present: Option<bool>,
        ciphersuite: Option<String>,
        committer_leaf_index: Option<u32>,
        joiner_leaf_index: Option<u32>,
        direct_path_len: Option<usize>,
        filtered_direct_path_len: Option<usize>,
        copath_len: Option<usize>,
        update_path_nodes_count: Option<usize>,
        encrypted_path_secret_count: Option<usize>,
        sum_copath_resolution_sizes: Option<usize>,
        max_copath_resolution_size: Option<usize>,
        path_secret_derivation_count: Option<u64>,
        node_secret_derivation_count: Option<u64>,
        hpke_encrypt_count: Option<u64>,
        hpke_decrypt_count: Option<u64>,
        tree_hash_nodes_touched: Option<u64>,
        parent_hash_nodes_touched: Option<u64>,
        commit_size_bytes: Option<usize>,
        commit_message_size_bytes: Option<usize>,
        commit_kind: Option<String>,
        commit_create_op: Option<String>,
        commit_semantics: Option<String>,
        add_semantics: Option<String>,
        commit_id: Option<String>,
        commit_has_path: Option<bool>,
        commit_is_external: Option<bool>,
        update_path_size_bytes: Option<usize>,
        welcome_recipient_count: Option<usize>,
        ratchet_tree_included: Option<bool>,
        ratchet_tree_delivery_mode: Option<String>,
        app_msg_plaintext_bytes: Option<usize>,
        app_msg_padding_bytes: Option<usize>,
        app_msg_ciphertext_bytes: Option<usize>,
        aad_bytes: Option<usize>,
        sender_leaf_index: Option<u32>,
        sender_generation: Option<u64>,
        first_message_in_epoch: Option<bool>,
        receiver_leaf_index: Option<u32>,
        receiver_member_index: Option<u32>,
        receiver_is_committer: Option<bool>,
        commit_receive_sampled: Option<bool>,
        commit_receive_sampling_policy: Option<String>,
        commit_receive_sampling_seed: Option<u64>,
        commit_receive_sample_index: Option<usize>,
        commit_receive_sample_count: Option<usize>,
        commit_receive_population_size: Option<usize>,
        selected_encrypted_path_secret_index: Option<usize>,
        path_secret_decryption_count: Option<u64>,
        confirmation_tag_verified: Option<bool>,
        proposal_count: Option<usize>,
        inline_proposal_count: Option<usize>,
        proposal_ref_count: Option<usize>,
        add_proposal_count: Option<usize>,
        update_proposal_count: Option<usize>,
        remove_proposal_count: Option<usize>,
        first_receive_from_sender: Option<bool>,
        generation_gap: Option<u64>,
        out_of_order_message: Option<bool>,
        aead_decrypt_count: Option<u64>,
        sender_data_decrypt_count: Option<u64>,
        signature_verify_count: Option<u64>,
        pid: u32,
        thread_id: String,
        global_span_id: Option<String>,
        parent_global_span_id: Option<String>,
        run_id: Option<String>,
        scenario: Option<String>,
        scenario_seed: Option<u64>,
        resource_limit_cpus: Option<f64>,
        resource_limit_memory: Option<&'a str>,
        resource_limit_memory_bytes: Option<u64>,
        resource_limit_memory_swap: Option<&'a str>,
        resource_limit_memory_swap_bytes: Option<u64>,
        resource_limit_pids: Option<u64>,
        resource_profile: &'a str,
        resource_profile_id: &'a str,
        resource_experiment_type: &'a str,
        cpu_capacity_fraction: Option<f64>,
        assigned_core_count: Option<u32>,
        cpuset: Option<&'a str>,
        profiled_singleton: bool,
    }

    fn non_empty_or<'a>(value: Option<&'a str>, default: &'a str) -> &'a str {
        match value {
            Some(value) if !value.is_empty() => value,
            _ => default,
        }
    }

    let mut rows = Vec::new();
    for worker_id in &aggregate_worker_ids {
        let path = run_dir.join(format!("client-{worker_id}.jsonl"));

        if !profile_enabled_ids.contains(worker_id.as_str()) {
            if path.exists() {
                eprintln!(
                    "[csv] removing stale profile file for packed client {}: {}",
                    worker_id,
                    path.display()
                );
                let _ = std::fs::remove_file(&path);
            }
            continue;
        }

        if !path.exists() {
            eprintln!(
                "[csv] WARNING: profile_enabled client {} JSONL not found: {}",
                worker_id,
                path.display()
            );
            continue;
        }

        let meta = client_meta.get(worker_id.as_str()).copied();

        let file = File::open(&path)?;
        let reader = BufReader::new(file);
        let mut span_metadata: std::collections::HashMap<u64, (String, Option<String>)> =
            std::collections::HashMap::new();

        for line in reader.lines() {
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }

            let mut event: ProfileEvent = serde_json::from_str(&line)
                .with_context(|| format!("Invalid json in {}", path.display()))?;
            if event.failure_class.as_deref() == Some("app_heap_budget_exceeded") {
                if let Some((span_name, parent_operation)) = event
                    .span_id
                    .and_then(|span_id| span_metadata.get(&span_id))
                {
                    event.span_name = Some(span_name.clone());
                    event.parent_operation = parent_operation.clone();
                }
            } else if let (Some(span_id), Some(span_name)) =
                (event.span_id, event.span_name.clone())
            {
                span_metadata.insert(span_id, (span_name, event.parent_operation.clone()));
            }
            let physical_worker_id =
                non_empty_or(meta.map(|m| m.physical_worker_id.as_str()), worker_id);
            let phys = physical_meta.get(physical_worker_id).copied();

            let row = CsvRow {
                client_id: worker_id,
                worker_id: physical_worker_id,
                container_mode: non_empty_or(meta.map(|m| m.container_mode.as_str()), "singleton"),
                execution_backend: non_empty_or(
                    meta.map(|m| m.execution_backend.as_str()),
                    non_empty_or(event.execution_backend.as_deref(), "local_process"),
                )
                .to_string(),
                device_kind: non_empty_or(
                    meta.map(|m| m.device_kind.as_str()),
                    non_empty_or(event.device_kind.as_deref(), "local_process"),
                )
                .to_string(),
                profile_schema_version: event.profile_schema_version,
                ts_unix_ns: event.ts_unix_ns,
                op: event.op,
                span_name: event.span_name,
                span_id: event.span_id,
                parent_span_id: event.parent_span_id,
                parent_operation: event.parent_operation,
                span_inclusive: event.span_inclusive,
                runner_event_kind: event.runner_event_kind,
                failed_worker_id: event.failed_worker_id,
                failed_physical_worker_id: event.failed_physical_worker_id,
                failure_class: event.failure_class,
                failure_detail: event.failure_detail,
                failure_evidence_source: event.failure_evidence_source,
                failure_evidence_detail: event.failure_evidence_detail,
                failure_action: event.failure_action,
                reassigned_to_worker_id: event.reassigned_to_worker_id,
                memory_model: event.memory_model.or_else(|| {
                    phys.and_then(|m| (!m.memory_model.is_empty()).then(|| m.memory_model.clone()))
                }),
                docker_memory_limit: phys.and_then(|m| {
                    (!m.docker_memory_limit.is_empty()).then_some(m.docker_memory_limit.as_str())
                }),
                app_heap_budget: event.app_heap_budget.or_else(|| {
                    phys.and_then(|m| {
                        (!m.app_heap_budget.is_empty()).then(|| m.app_heap_budget.clone())
                    })
                }),
                app_heap_budget_bytes: event
                    .app_heap_budget_bytes
                    .or_else(|| phys.and_then(|m| m.app_heap_budget_bytes)),
                heap_current_live_bytes: event.heap_current_live_bytes,
                heap_peak_live_bytes: event.heap_peak_live_bytes,
                heap_operation_peak_live_bytes: event.heap_operation_peak_live_bytes,
                heap_total_allocated_bytes: event.heap_total_allocated_bytes,
                heap_allocation_count: event.heap_allocation_count,
                heap_deallocation_count: event.heap_deallocation_count,
                heap_failed_allocation_size_bytes: event.heap_failed_allocation_size_bytes,
                heap_failure_context: event.heap_failure_context,
                benchmark_plateau_index: event.benchmark_plateau_index,
                benchmark_target_size: event.benchmark_target_size,
                benchmark_active_size: event.benchmark_active_size,
                benchmark_phase: event.benchmark_phase,
                benchmark_operation: event.benchmark_operation,
                benchmark_operation_seq: event.benchmark_operation_seq,
                benchmark_payload_size: event.benchmark_payload_size,
                membership_batch_requested: event.membership_batch_requested,
                membership_batch_effective: event.membership_batch_effective,
                membership_batch_group_cap: event.membership_batch_group_cap,
                membership_batch_transition_cap: event.membership_batch_transition_cap,
                membership_batch_source: event.membership_batch_source,
                configured_payload_label: event.configured_payload_label,
                wall_ns: event.wall_ns,
                cpu_thread_ns: event.cpu_thread_ns,
                cpu_process_ns: event.cpu_process_ns,
                cpu_envelope_utilization: event.cpu_envelope_utilization,
                cpu_throttled_time_ratio: event.cpu_throttled_time_ratio,
                cpu_nr_periods_delta: event.cpu_nr_periods_delta,
                cpu_nr_throttled_delta: event.cpu_nr_throttled_delta,
                cpu_throttled_usec_delta: event.cpu_throttled_usec_delta,
                cpu_throttled_period_fraction: event.cpu_throttled_period_fraction,
                cpu_nr_periods_cumulative: event.cpu_nr_periods_cumulative,
                cpu_nr_throttled_cumulative: event.cpu_nr_throttled_cumulative,
                cpu_throttled_usec_cumulative: event.cpu_throttled_usec_cumulative,
                cpu_throttled_period_fraction_cumulative: event
                    .cpu_throttled_period_fraction_cumulative,
                cpu_throttled_period_threshold: event.cpu_throttled_period_threshold,
                cpu_throttled_period_threshold_crossing: event
                    .cpu_throttled_period_threshold_crossing,
                alloc_bytes: event.alloc_bytes,
                alloc_count: event.alloc_count,
                alloc_measurement_scope: event.alloc_measurement_scope,
                l1d_cache_accesses: event.l1d_cache_accesses,
                l1d_cache_misses: event.l1d_cache_misses,
                l1d_measurement_scope: event.l1d_measurement_scope,
                l1d_cache_status: event.l1d_cache_status,
                l1d_measured_thread_count: event.l1d_measured_thread_count,
                l1d_discovered_thread_count: event.l1d_discovered_thread_count,
                l1d_multiplexed_thread_count: event.l1d_multiplexed_thread_count,
                ram_rss_delta_bytes: event.ram_rss_delta_bytes,
                ram_rss_utilization: event.ram_rss_utilization,
                artifact_size_bytes: event.artifact_size_bytes,
                welcome_bytes: event.welcome_bytes,
                ratchet_tree_bytes: event.ratchet_tree_bytes,
                welcome_plus_ratchet_tree_bytes: event.welcome_plus_ratchet_tree_bytes,
                group_info_bytes: event.group_info_bytes,
                group_info_plaintext_bytes: event.group_info_plaintext_bytes,
                group_info_ciphertext_bytes: event.group_info_ciphertext_bytes,
                encrypted_group_info_bytes: event.encrypted_group_info_bytes,
                encrypted_secrets_count: event.encrypted_secrets_count,
                group_epoch: event.group_epoch,
                tree_size: event.tree_size,
                tree_height: event.tree_height,
                tree_leaf_count: event.tree_leaf_count,
                tree_node_count: event.tree_node_count,
                operation_family: event.operation_family,
                member_count: event.member_count,
                member_count_before: event.member_count_before,
                member_count_after: event.member_count_after,
                invitee_count: event.invitee_count,
                added_members_count: event.added_members_count,
                removed_members_count: event.removed_members_count,
                removed_leaf_indices: event
                    .removed_leaf_indices
                    .map(|v| serde_json::to_string(&v).unwrap_or_default()),
                removed_right_edge_count: event.removed_right_edge_count,
                rightmost_removed_leaf: event.rightmost_removed_leaf,
                removed_right_edge_suffix_count: event.removed_right_edge_suffix_count,
                right_edge_suffix_fully_removed: event.right_edge_suffix_fully_removed,
                tree_truncated: event.tree_truncated,
                truncated_levels_count: event.truncated_levels_count,
                tree_size_before: event.tree_size_before,
                tree_size_after: event.tree_size_after,
                tree_leaf_count_before: event.tree_leaf_count_before,
                tree_leaf_count_after: event.tree_leaf_count_after,
                tree_node_count_before: event.tree_node_count_before,
                tree_node_count_after: event.tree_node_count_after,
                add_commit_mode: event.add_commit_mode,
                remove_commit_mode: event.remove_commit_mode,
                commit_path_policy: event.commit_path_policy,
                force_self_update: event.force_self_update,
                update_path_present: event.update_path_present,
                ciphersuite: event.ciphersuite,
                committer_leaf_index: event.committer_leaf_index,
                joiner_leaf_index: event.joiner_leaf_index,
                direct_path_len: event.direct_path_len,
                filtered_direct_path_len: event.filtered_direct_path_len,
                copath_len: event.copath_len,
                update_path_nodes_count: event.update_path_nodes_count,
                encrypted_path_secret_count: event.encrypted_path_secret_count,
                sum_copath_resolution_sizes: event.sum_copath_resolution_sizes,
                max_copath_resolution_size: event.max_copath_resolution_size,
                path_secret_derivation_count: event.path_secret_derivation_count,
                node_secret_derivation_count: event.node_secret_derivation_count,
                hpke_encrypt_count: event.hpke_encrypt_count,
                hpke_decrypt_count: event.hpke_decrypt_count,
                tree_hash_nodes_touched: event.tree_hash_nodes_touched,
                parent_hash_nodes_touched: event.parent_hash_nodes_touched,
                commit_size_bytes: event.commit_size_bytes,
                commit_message_size_bytes: event.commit_message_size_bytes,
                commit_kind: event.commit_kind,
                commit_create_op: event.commit_create_op,
                commit_semantics: event.commit_semantics,
                add_semantics: event.add_semantics,
                commit_id: event.commit_id,
                commit_has_path: event.commit_has_path,
                commit_is_external: event.commit_is_external,
                update_path_size_bytes: event.update_path_size_bytes,
                welcome_recipient_count: event.welcome_recipient_count,
                ratchet_tree_included: event.ratchet_tree_included,
                ratchet_tree_delivery_mode: event.ratchet_tree_delivery_mode,
                app_msg_plaintext_bytes: event.app_msg_plaintext_bytes,
                app_msg_padding_bytes: event.app_msg_padding_bytes,
                app_msg_ciphertext_bytes: event.app_msg_ciphertext_bytes,
                aad_bytes: event.aad_bytes,
                sender_leaf_index: event.sender_leaf_index,
                sender_generation: event.sender_generation,
                first_message_in_epoch: event.first_message_in_epoch,
                receiver_leaf_index: event.receiver_leaf_index,
                receiver_member_index: event.receiver_member_index,
                receiver_is_committer: event.receiver_is_committer,
                commit_receive_sampled: event.commit_receive_sampled,
                commit_receive_sampling_policy: event.commit_receive_sampling_policy,
                commit_receive_sampling_seed: event.commit_receive_sampling_seed,
                commit_receive_sample_index: event.commit_receive_sample_index,
                commit_receive_sample_count: event.commit_receive_sample_count,
                commit_receive_population_size: event.commit_receive_population_size,
                selected_encrypted_path_secret_index: event.selected_encrypted_path_secret_index,
                path_secret_decryption_count: event.path_secret_decryption_count,
                confirmation_tag_verified: event.confirmation_tag_verified,
                proposal_count: event.proposal_count,
                inline_proposal_count: event.inline_proposal_count,
                proposal_ref_count: event.proposal_ref_count,
                add_proposal_count: event.add_proposal_count,
                update_proposal_count: event.update_proposal_count,
                remove_proposal_count: event.remove_proposal_count,
                first_receive_from_sender: event.first_receive_from_sender,
                generation_gap: event.generation_gap,
                out_of_order_message: event.out_of_order_message,
                aead_decrypt_count: event.aead_decrypt_count,
                sender_data_decrypt_count: event.sender_data_decrypt_count,
                signature_verify_count: event.signature_verify_count,
                pid: event.pid,
                thread_id: event.thread_id,
                global_span_id: event.global_span_id.clone(),
                parent_global_span_id: event.parent_global_span_id.clone(),
                run_id: event.run_id,
                scenario: event.scenario,
                scenario_seed: event.scenario_seed,
                resource_limit_cpus: phys.and_then(|m| m.resource_limit_cpus),
                resource_limit_memory: phys.and_then(|m| m.resource_limit_memory.as_deref()),
                resource_limit_memory_bytes: phys.and_then(|m| m.resource_limit_memory_bytes),
                resource_limit_memory_swap: phys
                    .and_then(|m| m.resource_limit_memory_swap.as_deref()),
                resource_limit_memory_swap_bytes: phys
                    .and_then(|m| m.resource_limit_memory_swap_bytes),
                resource_limit_pids: phys.and_then(|m| m.resource_limit_pids),
                resource_profile: non_empty_or(phys.map(|m| m.resource_profile.as_str()), ""),
                resource_profile_id: non_empty_or(phys.map(|m| m.resource_profile_id.as_str()), ""),
                resource_experiment_type: non_empty_or(
                    phys.map(|m| m.resource_experiment_type.as_str()),
                    "",
                ),
                cpu_capacity_fraction: phys.and_then(|m| m.cpu_capacity_fraction),
                assigned_core_count: phys.and_then(|m| m.assigned_core_count),
                cpuset: phys.and_then(|m| m.cpuset.as_deref()),
                profiled_singleton: phys.map(|m| m.profiled_singleton).unwrap_or(false),
            };

            rows.push(row);
        }
    }

    rows.sort_by(|left, right| {
        left.ts_unix_ns.cmp(&right.ts_unix_ns).then_with(|| {
            let left_failure = left.runner_event_kind.as_deref() == Some("worker_failure");
            let right_failure = right.runner_event_kind.as_deref() == Some("worker_failure");
            left_failure.cmp(&right_failure)
        })
    });
    let events_written = rows.len() as u64;
    for row in rows {
        wtr.serialize(row)?;
    }

    wtr.flush()?;
    drop(wtr);
    fs::rename(&tmp_path, &csv_path).with_context(|| {
        format!(
            "Failed to rename {} to {}",
            tmp_path.display(),
            csv_path.display()
        )
    })?;
    Ok(events_written)
}

#[cfg(test)]
mod validate_run_id_tests {
    use super::*;

    #[test]
    fn validate_run_id_accepts_valid() {
        for rid in &["run-001", "test.123", "bench_2026", "a", "0", "A.B_C"] {
            assert!(
                validate_run_id(rid).is_ok(),
                "Expected '{}' to be valid",
                rid
            );
        }
    }

    #[test]
    fn validate_run_id_rejects_empty() {
        assert!(validate_run_id("").is_err());
    }

    #[test]
    fn validate_run_id_rejects_path_traversal() {
        for rid in &["/", ".", "..", "foo/bar", "../../etc"] {
            assert!(
                validate_run_id(rid).is_err(),
                "Expected '{}' to be rejected",
                rid
            );
        }
    }

    #[test]
    fn validate_run_id_rejects_special_chars() {
        for rid in &["hello world", "foo$bar", "foo|bar", "foo;bar"] {
            assert!(
                validate_run_id(rid).is_err(),
                "Expected '{}' to be rejected",
                rid
            );
        }
    }
}

#[cfg(test)]
mod membership_batch_tests {
    use super::*;

    #[test]
    fn membership_batch_group_cap_is_independent_of_group_size() {
        assert_eq!(membership_batch_group_cap(1), 8);
        assert_eq!(membership_batch_group_cap(7), 8);
        assert_eq!(membership_batch_group_cap(8), 8);
        assert_eq!(membership_batch_group_cap(16), 8);
        assert_eq!(membership_batch_group_cap(32), 8);
        assert_eq!(membership_batch_group_cap(256), 8);
    }

    #[test]
    fn membership_batch_planner_covers_every_k_once_per_cycle() {
        let mut planner = MembershipBatchPlanner::new(0x1234);
        let mut observed = (0..8)
            .map(|_| planner.next_batch(32, 8, "test").requested)
            .collect::<Vec<_>>();
        observed.sort_unstable();
        assert_eq!(observed, (1..=8).collect::<Vec<_>>());
    }

    #[test]
    fn membership_batch_planner_uses_largest_feasible_batch_first() {
        let mut planner = MembershipBatchPlanner::new(0x1234);
        let decision = planner.next_batch(32, 4, "test");
        assert_eq!(decision.requested, 4);
        assert_eq!(decision.effective, 4);
    }

    #[test]
    fn membership_batch_planner_is_seed_reproducible() {
        let sequence = |seed| {
            let mut planner = MembershipBatchPlanner::new(seed);
            (0..24)
                .map(|_| planner.next_batch(32, 8, "test").requested)
                .collect::<Vec<_>>()
        };
        assert_eq!(sequence(91), sequence(91));
        assert_ne!(sequence(91), sequence(92));
    }

    #[test]
    fn membership_batch_planner_respects_transition_cap() {
        let mut planner = MembershipBatchPlanner::new(7);
        for _ in 0..128 {
            let decision = planner.next_batch(256, 3, "test");
            assert!((1..=3).contains(&decision.requested));
            assert!((1..=3).contains(&decision.effective));
            assert_eq!(decision.effective, decision.requested);
            assert_eq!(decision.group_cap, 8);
            assert_eq!(decision.transition_cap, 3);
        }
    }
}

#[cfg(test)]
mod aggregate_csv_resource_tests {
    use super::*;

    #[test]
    fn aggregate_csv_resolves_heap_failure_suboperation() {
        let run_dir = std::env::temp_dir().join(format!(
            "openmls-heap-failure-span-test-{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&run_dir).expect("create run dir");

        let completed_span = ProfileEvent {
            ts_unix_ns: 1,
            op: "commit_receive.path_secret_decrypt".to_string(),
            span_name: Some("commit_receive.path_secret_decrypt".to_string()),
            span_id: Some(77),
            parent_operation: Some("commit_receive_protocol".to_string()),
            implementation: "openmls".to_string(),
            ..ProfileEvent::default()
        };
        let mut failure = ProfileEvent {
            ts_unix_ns: 2,
            op: "benchmark.worker_failure".to_string(),
            failure_class: Some("app_heap_budget_exceeded".to_string()),
            failure_detail: Some(
                "APP_HEAP_BUDGET_EXCEEDED operation_family=commit_receive failure_span_id=77 heap_failure_context=openmls_span_execution".to_string(),
            ),
            implementation: "benchmark_runner".to_string(),
            ..ProfileEvent::default()
        };
        apply_app_heap_budget_failure_fields(&mut failure);
        assert_eq!(failure.span_id, Some(77));

        let profile = format!(
            "{}\n{}\n",
            serde_json::to_string(&completed_span).expect("serialize span"),
            serde_json::to_string(&failure).expect("serialize failure")
        );
        std::fs::write(run_dir.join("client-00001.jsonl"), profile).expect("write profile");

        aggregate_csv(&run_dir, &["00001".to_string()], &None).expect("aggregate csv");
        let mut reader = csv::Reader::from_path(run_dir.join("events.csv")).expect("open csv");
        let headers = reader.headers().expect("headers").clone();
        let failure_row = reader
            .records()
            .map(|record| record.expect("valid row"))
            .find(|record| {
                let index = headers
                    .iter()
                    .position(|header| header == "failure_class")
                    .expect("failure_class header");
                record.get(index) == Some("app_heap_budget_exceeded")
            })
            .expect("failure row");
        let value = |name: &str| {
            let index = headers
                .iter()
                .position(|header| header == name)
                .expect("header exists");
            failure_row.get(index).expect("value")
        };
        assert_eq!(value("span_id"), "77");
        assert_eq!(value("span_name"), "commit_receive.path_secret_decrypt");
        assert_eq!(value("parent_operation"), "commit_receive_protocol");
        assert_eq!(value("heap_failure_context"), "openmls_span_execution");

        let _ = std::fs::remove_dir_all(run_dir);
    }

    #[test]
    fn aggregate_csv_appends_resource_limit_columns_from_layout() {
        let unique = format!(
            "openmls-aggregate-resource-test-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system time")
                .as_nanos()
        );
        let run_dir = std::env::temp_dir().join(unique);
        std::fs::create_dir_all(&run_dir).expect("create run dir");

        let layout = serde_json::json!({
            "version": 1,
            "logical_worker_count": 1,
            "physical_worker_count": 1,
            "layout_mode": "one-container-per-client",
            "profile_policy": "all",
            "clients": [{
                "client_id": "00001",
                "physical_worker_id": "worker-00001",
                "container_mode": "singleton",
                "profile_enabled": true,
                "command_url": "http://worker-00001:8080/client/00001",
                "health_url": "http://worker-00001:8080/client/00001/health"
            }],
            "physical_workers": [{
                "physical_worker_id": "worker-00001",
                "container_mode": "singleton",
                "client_ids": ["00001"],
                "base_url": "http://worker-00001:8080",
                "profile_enabled_client_ids": ["00001"],
                "resource_limit_cpus": 0.25,
                "resource_limit_memory": "128m",
                "resource_limit_memory_bytes": 134217728,
                "resource_limit_memory_swap": "128m",
                "resource_limit_memory_swap_bytes": 134217728,
                "resource_limit_pids": 128,
                "resource_profile": "singleton-resource-envelope_cpus-0.25_memory-128m"
            }]
        });
        std::fs::write(
            run_dir.join("worker_layout.json"),
            serde_json::to_string_pretty(&layout).expect("layout json"),
        )
        .expect("write layout");

        let mut event = serde_json::json!({
            "profile_schema_version": 3,
            "ts_unix_ns": 1u128,
            "op": "create_group",
            "implementation": "openmls",
            "span_name": "create_group",
            "span_id": 44u64,
            "parent_span_id": 43u64,
            "span_inclusive": true,
            "wall_ns": 10u128,
            "cpu_thread_ns": 9u128,
            "cpu_process_ns": 11u128,
            "cpu_envelope_utilization": 0.9,
            "cpu_throttled_time_ratio": 0.1,
            "alloc_bytes": 42u64,
            "alloc_count": 3u64,
            "l1d_cache_accesses": 1000u64,
            "l1d_cache_misses": 25u64,
            "ram_rss_delta_bytes": 4096i64,
            "ram_rss_utilization": 0.25,
            "tree_height": 3u32,
            "tree_leaf_count": 8u32,
            "tree_node_count": 15u32,
            "operation_family": "add_commit_create",
            "member_count": 7usize,
            "member_count_before": 7usize,
            "member_count_after": 8usize,
            "added_members_count": 1usize,
            "direct_path_len": 3usize,
            "filtered_direct_path_len": 3usize,
            "encrypted_path_secret_count": 7usize,
            "node_secret_derivation_count": 3u64,
            "hpke_encrypt_count": 7u64,
            "tree_hash_nodes_touched": 15u64,
            "commit_size_bytes": 1234usize,
            "pid": 123,
            "thread_id": "test-thread",
            "run_id": "test-run",
            "scenario": "unit-test",
            "scenario_seed": 1u64
        });
        let event_object = event.as_object_mut().expect("fixture must be an object");
        event_object.insert("cpu_nr_periods_delta".to_string(), serde_json::json!(10));
        event_object.insert("cpu_nr_throttled_delta".to_string(), serde_json::json!(1));
        event_object.insert(
            "cpu_throttled_usec_delta".to_string(),
            serde_json::json!(50000),
        );
        event_object.insert(
            "cpu_throttled_period_fraction".to_string(),
            serde_json::json!(0.1),
        );
        event_object.insert(
            "cpu_nr_periods_cumulative".to_string(),
            serde_json::json!(20),
        );
        event_object.insert(
            "cpu_nr_throttled_cumulative".to_string(),
            serde_json::json!(1),
        );
        event_object.insert(
            "cpu_throttled_usec_cumulative".to_string(),
            serde_json::json!(50000),
        );
        event_object.insert(
            "cpu_throttled_period_fraction_cumulative".to_string(),
            serde_json::json!(0.05),
        );
        event_object.insert(
            "cpu_throttled_period_threshold".to_string(),
            serde_json::json!(0.05),
        );
        event_object.insert(
            "cpu_throttled_period_threshold_crossing".to_string(),
            serde_json::json!(true),
        );
        event_object.insert("group_info_bytes".to_string(), serde_json::json!(256usize));
        event_object.insert(
            "group_info_plaintext_bytes".to_string(),
            serde_json::json!(256usize),
        );
        event_object.insert(
            "group_info_ciphertext_bytes".to_string(),
            serde_json::json!(272usize),
        );
        event_object.insert(
            "encrypted_group_info_bytes".to_string(),
            serde_json::json!(272usize),
        );
        event_object.insert(
            "ratchet_tree_bytes".to_string(),
            serde_json::json!(128usize),
        );
        event_object.insert("ratchet_tree_included".to_string(), serde_json::json!(true));
        std::fs::write(
            run_dir.join("client-00001.jsonl"),
            serde_json::to_string(&event).expect("event json") + "\n",
        )
        .expect("write jsonl");

        let events_written =
            aggregate_csv(&run_dir, &["00001".to_string()], &None).expect("aggregate csv");
        assert_eq!(events_written, 1);

        let mut reader = csv::Reader::from_path(run_dir.join("events.csv")).expect("open csv");
        let headers = reader.headers().expect("headers").clone();
        let record = reader
            .records()
            .next()
            .expect("one row")
            .expect("valid row");
        let value = |name: &str| {
            let idx = headers
                .iter()
                .position(|header| header == name)
                .expect("header exists");
            record.get(idx).expect("value").to_string()
        };

        assert_eq!(value("resource_limit_cpus"), "0.25");
        assert_eq!(value("cpu_process_ns"), "11");
        assert_eq!(value("operation_family"), "add_commit_create");
        assert_eq!(value("member_count"), "7");
        assert_eq!(value("member_count_before"), "7");
        assert_eq!(value("member_count_after"), "8");
        assert_eq!(value("added_members_count"), "1");
        assert_eq!(value("group_info_plaintext_bytes"), "256");
        assert_eq!(value("group_info_ciphertext_bytes"), "272");
        assert_eq!(value("ratchet_tree_bytes"), "128");
        assert_eq!(value("ratchet_tree_included"), "true");
        assert_eq!(value("resource_limit_memory"), "128m");
        assert_eq!(value("resource_limit_memory_bytes"), "134217728");
        assert_eq!(value("resource_limit_memory_swap"), "128m");
        assert_eq!(value("resource_limit_memory_swap_bytes"), "134217728");
        assert_eq!(value("resource_limit_pids"), "128");
        assert_eq!(
            value("resource_profile"),
            "singleton-resource-envelope_cpus-0.25_memory-128m"
        );
        assert_eq!(value("cpu_envelope_utilization"), "0.9");
        assert_eq!(value("cpu_throttled_time_ratio"), "0.1");
        assert_eq!(value("cpu_nr_periods_delta"), "10");
        assert_eq!(value("cpu_nr_throttled_delta"), "1");
        assert_eq!(value("cpu_throttled_usec_delta"), "50000");
        assert_eq!(value("cpu_throttled_period_fraction"), "0.1");
        assert_eq!(value("cpu_nr_periods_cumulative"), "20");
        assert_eq!(value("cpu_nr_throttled_cumulative"), "1");
        assert_eq!(value("cpu_throttled_usec_cumulative"), "50000");
        assert_eq!(value("cpu_throttled_period_fraction_cumulative"), "0.05");
        assert_eq!(value("cpu_throttled_period_threshold"), "0.05");
        assert_eq!(value("cpu_throttled_period_threshold_crossing"), "true");
        assert_eq!(value("l1d_cache_accesses"), "1000");
        assert_eq!(value("l1d_cache_misses"), "25");
        assert_eq!(value("ram_rss_delta_bytes"), "4096");
        assert_eq!(value("ram_rss_utilization"), "0.25");
        assert_eq!(value("span_name"), "create_group");
        assert_eq!(value("span_id"), "44");
        assert_eq!(value("parent_span_id"), "43");
        assert_eq!(value("span_inclusive"), "true");
        assert_eq!(value("tree_height"), "3");
        assert_eq!(value("tree_leaf_count"), "8");
        assert_eq!(value("tree_node_count"), "15");
        assert_eq!(value("direct_path_len"), "3");
        assert_eq!(value("filtered_direct_path_len"), "3");
        assert_eq!(value("encrypted_path_secret_count"), "7");
        assert_eq!(value("node_secret_derivation_count"), "3");
        assert_eq!(value("hpke_encrypt_count"), "7");
        assert_eq!(value("tree_hash_nodes_touched"), "15");
        assert_eq!(value("commit_size_bytes"), "1234");

        let _ = std::fs::remove_dir_all(&run_dir);
    }

    #[test]
    fn aggregate_csv_materializes_runner_event_without_profile_append() {
        let unique = format!(
            "openmls-runner-event-test-{}-{}",
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
        assert!(run_dir.join("client-00001.jsonl").exists());

        let _ = std::fs::remove_dir_all(&run_dir);
    }

    #[test]
    fn aggregate_csv_orders_terminal_failure_after_completed_spans() {
        let unique = format!(
            "openmls-terminal-failure-order-test-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system time")
                .as_nanos()
        );
        let run_dir = std::env::temp_dir().join(unique);
        std::fs::create_dir_all(&run_dir).expect("create run dir");

        append_profile_event(
            &run_dir.join("client-00001.jsonl"),
            &ProfileEvent {
                ts_unix_ns: 100,
                op: "update_commit_create_total_local".to_string(),
                implementation: "openmls".to_string(),
                worker_id: Some("00001".to_string()),
                benchmark_phase: Some("update".to_string()),
                benchmark_operation: Some("update_commit".to_string()),
                ..ProfileEvent::default()
            },
        )
        .expect("write completed span");

        let failure = RunnerEvent {
            profile_schema_version: 10,
            ts_unix_ns: 200,
            event_kind: "worker_failure".to_string(),
            failed_worker_id: "00001".to_string(),
            failed_physical_worker_id: "worker-00001".to_string(),
            failure_class: "cpu_starvation_timeout".to_string(),
            failure_detail: "deadline elapsed".to_string(),
            failure_evidence_source: Some("runner_observed_request_failure".to_string()),
            failure_evidence_detail: Some("request timed out".to_string()),
            failure_action: "stop_run".to_string(),
            reassigned_to_worker_id: None,
            benchmark_plateau_index: 3,
            benchmark_target_size: 16,
            benchmark_active_size: 16,
            benchmark_phase: "update".to_string(),
            benchmark_operation: "update_commit".to_string(),
            benchmark_operation_seq: Some(2),
            benchmark_payload_size: None,
            membership_batch_requested: None,
            membership_batch_effective: None,
            membership_batch_group_cap: None,
            membership_batch_transition_cap: None,
            membership_batch_source: None,
            configured_payload_label: None,
        };
        std::fs::write(
            run_dir.join("runner-events.jsonl"),
            serde_json::to_string(&failure).expect("failure json") + "\n",
        )
        .expect("write runner failure");

        aggregate_csv(&run_dir, &["00001".to_string()], &None).expect("aggregate csv");
        let mut reader = csv::Reader::from_path(run_dir.join("events.csv")).expect("open csv");
        let headers = reader.headers().expect("headers").clone();
        let op_index = headers.iter().position(|header| header == "op").unwrap();
        let rows = reader
            .records()
            .collect::<std::result::Result<Vec<_>, _>>()
            .expect("rows");
        assert_eq!(
            rows.last().unwrap().get(op_index),
            Some("benchmark.worker_failure")
        );

        let _ = std::fs::remove_dir_all(&run_dir);
    }

    #[test]
    fn aggregate_csv_atomic_write_renames_temp_to_final() {
        let unique = format!(
            "openmls-atomic-test-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system time")
                .as_nanos()
        );
        let run_dir = std::env::temp_dir().join(unique);
        std::fs::create_dir_all(&run_dir).expect("create run dir");

        let event = serde_json::json!({
            "profile_schema_version": 3,
            "ts_unix_ns": 1u128,
            "op": "test.atomic",
            "implementation": "test",
            "cpu_process_ns": 0u128,
            "wall_ns": 0u128,
            "pid": 1,
            "thread_id": "t1",
        });
        std::fs::write(
            run_dir.join("client-00001.jsonl"),
            serde_json::to_string(&event).expect("event json") + "\n",
        )
        .expect("write jsonl");

        aggregate_csv(&run_dir, &["00001".to_string()], &None).expect("aggregate csv");

        assert!(
            run_dir.join("events.csv").exists(),
            "events.csv must exist after rename"
        );
        assert!(
            !run_dir.join("events.csv.tmp").exists(),
            "tmp file must be gone after rename"
        );

        let _ = std::fs::remove_dir_all(&run_dir);
    }

    #[test]
    fn aggregate_csv_handles_missing_client_file() {
        let unique = format!(
            "openmls-missing-test-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system time")
                .as_nanos()
        );
        let run_dir = std::env::temp_dir().join(unique);
        std::fs::create_dir_all(&run_dir).expect("create run dir");

        let result = aggregate_csv(&run_dir, &["00001".to_string()], &None);
        assert!(result.is_ok(), "Should not crash on missing client file");

        let _ = std::fs::remove_dir_all(&run_dir);
    }

    #[test]
    fn aggregate_csv_handles_malformed_middle_line() {
        let unique = format!(
            "openmls-malformed-test-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system time")
                .as_nanos()
        );
        let run_dir = std::env::temp_dir().join(unique);
        std::fs::create_dir_all(&run_dir).expect("create run dir");

        let good_line = serde_json::json!({
            "profile_schema_version": 3,
            "ts_unix_ns": 1u128,
            "op": "test.good",
            "implementation": "test",
            "cpu_process_ns": 0u128,
            "wall_ns": 0u128,
            "pid": 1,
            "thread_id": "t1",
        });
        let content = format!(
            "{}\n{{invalid json}}\n{}\n",
            serde_json::to_string(&good_line).unwrap(),
            serde_json::to_string(&good_line).unwrap(),
        );
        std::fs::write(run_dir.join("client-00001.jsonl"), content).expect("write jsonl");

        let result = aggregate_csv(&run_dir, &["00001".to_string()], &None);
        assert!(
            result.is_err(),
            "Should fail on malformed line in strict mode"
        );

        let _ = std::fs::remove_dir_all(&run_dir);
    }

    #[test]
    fn aggregate_csv_handles_truncated_final_line() {
        let unique = format!(
            "openmls-truncated-test-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system time")
                .as_nanos()
        );
        let run_dir = std::env::temp_dir().join(unique);
        std::fs::create_dir_all(&run_dir).expect("create run dir");

        let good_line = serde_json::json!({
            "profile_schema_version": 3,
            "ts_unix_ns": 1u128,
            "op": "test.good",
            "implementation": "test",
            "cpu_process_ns": 0u128,
            "wall_ns": 0u128,
            "pid": 1,
            "thread_id": "t1",
        });
        let content = format!(
            "{}\n{}",
            serde_json::to_string(&good_line).unwrap(),
            r#"{"profile_schema_version": 3, "implementation": "test", "ts_unix_ns": 2, "op": "trunc"#,
        );
        std::fs::write(run_dir.join("client-00001.jsonl"), content).expect("write jsonl");

        let result = aggregate_csv(&run_dir, &["00001".to_string()], &None);
        assert!(
            result.is_err(),
            "Should fail on truncated final line in strict mode"
        );

        let _ = std::fs::remove_dir_all(&run_dir);
    }

    #[test]
    fn aggregate_csv_no_usable_records_handled() {
        let unique = format!(
            "openmls-norecords-test-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system time")
                .as_nanos()
        );
        let run_dir = std::env::temp_dir().join(unique);
        std::fs::create_dir_all(&run_dir).expect("create run dir");

        std::fs::write(run_dir.join("client-00001.jsonl"), "").expect("write empty jsonl");

        let result = aggregate_csv(&run_dir, &["00001".to_string()], &None);
        assert!(result.is_ok(), "Empty client file should not crash");

        let _ = std::fs::remove_dir_all(&run_dir);
    }
}

/// Validate a run ID string for safe filesystem usage.
/// Rejects empty, `/`, `.`, `..`, strings containing `/`, or anything outside `[A-Za-z0-9._-]+`.
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
    if config.min_profiled_samples_per_operation > 0
        && (!config.external_coverage_lane
            || !config
                .workers
                .iter()
                .any(|worker| worker.profile_enabled && is_external_device(worker)))
    {
        return Err(anyhow!(
            "--min-external-samples-per-operation requires --external-coverage-lane and at least one profile-enabled external device"
        ));
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

    if !config.plateau_sizes.is_empty() {
        if config.plateau_order != PlateauOrder::Ascending {
            return Err(anyhow!(
                "--plateau-sizes requires --plateau-order ascending"
            ));
        }
        if config.plateau_sizes.iter().any(|size| *size == 0) {
            return Err(anyhow!("--plateau-sizes values must be at least 1"));
        }
        if config
            .plateau_sizes
            .windows(2)
            .any(|pair| pair[0] >= pair[1])
        {
            return Err(anyhow!(
                "--plateau-sizes must be strictly increasing; got {:?}",
                config.plateau_sizes
            ));
        }
        let first = config.plateau_sizes[0];
        let last = *config
            .plateau_sizes
            .last()
            .expect("non-empty plateau sizes");
        if first < config.min_size || last > max_size {
            return Err(anyhow!(
                "--plateau-sizes {:?} must stay within --min-size {} and --max-size {}",
                config.plateau_sizes,
                config.min_size,
                max_size
            ));
        }
    }

    Ok(max_size)
}

#[cfg(test)]
mod random_input_tests {
    use std::collections::HashSet;

    use rand::{rngs::StdRng, SeedableRng};

    use super::{
        build_plateau_sequence, build_plateau_sequence_for_order,
        build_plateau_sequence_for_step_size, PayloadSizeSource, PayloadSizes, PlateauOrder,
        StepSize,
    };

    #[test]
    fn fixed_and_range_flag_values_parse() {
        assert_eq!("8".parse::<StepSize>(), Ok(StepSize::Fixed(8)));
        assert_eq!(
            "ascending".parse::<PlateauOrder>(),
            Ok(PlateauOrder::Ascending)
        );
        assert_eq!("asc".parse::<PlateauOrder>(), Ok(PlateauOrder::Ascending));
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
            build_plateau_sequence_for_step_size(2, 32, &StepSize::Fixed(8), 1, &mut rng);
        assert_eq!(fixed_sequence, build_plateau_sequence(2, 32, 8, 1));

        let sequence = build_plateau_sequence_for_step_size(2, 32, &step_size, 2, &mut rng);
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
    fn ascending_plateaus_only_grow() {
        let mut rng = StdRng::seed_from_u64(19);
        let sequence = build_plateau_sequence_for_order(
            2,
            16,
            &StepSize::Fixed(4),
            3,
            PlateauOrder::Ascending,
            &mut rng,
        );

        assert_eq!(sequence, vec![2, 6, 10, 14, 16]);
        assert!(sequence.windows(2).all(|pair| pair[0] < pair[1]));
    }

    #[test]
    fn randomized_plateaus_are_seeded_and_not_a_monotonic_staircase() {
        let sequence_for = |seed| {
            let mut rng = StdRng::seed_from_u64(seed);
            build_plateau_sequence_for_order(
                2,
                32,
                &StepSize::Fixed(5),
                2,
                PlateauOrder::Randomized,
                &mut rng,
            )
        };

        let first = sequence_for(17);
        assert_eq!(first, sequence_for(17));
        assert_ne!(first, sequence_for(18));
        assert!(first.contains(&2));
        assert!(first.contains(&32));
        assert!(first.windows(3).any(|window| {
            !((window[0] < window[1] && window[1] < window[2])
                || (window[0] > window[1] && window[1] > window[2]))
        }));
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };
    use std::time::Duration;

    use anyhow::anyhow;
    use rand::{rngs::StdRng, SeedableRng};

    use crate::worker_api::Command;

    use super::{
        batch_command_item, batch_physical_base_url, build_batch_commands,
        build_commit_receive_sampling_map, choose_remove_actor_and_removable_indices,
        external_remove_rejoin_pairs, fanout_workers, least_sampled_active_external_id,
        least_used_external_actor_id, partition_batch_failures_by_reconciled_state,
        protected_member_floor, removable_member_indices, remove_rejoin_failure_operation,
        retry_batch_commands_for_failures, sampled_member_index,
        select_supplemental_remove_receiver_roles, BatchFanoutCommand, BatchFanoutError,
        BenchmarkCursor, FanoutController, FanoutFailure, MembershipBatchDecision,
        WorkerCommandError, WorkerCommandErrorClass, WorkerSpec,
        DEFAULT_FANOUT_ERROR_RATE_THRESHOLD, FANOUT_LATENCY_SPIKE_P95_MS,
    };

    fn sampled_indices(member_count: usize, sample_count: usize) -> Vec<usize> {
        (0..sample_count)
            .map(|seq_no| sampled_member_index(member_count, sample_count, seq_no))
            .collect()
    }

    #[test]
    fn random_removal_preserves_profiled_and_external_clients() {
        let mut profiled = WorkerSpec::legacy("00001".into(), "http://worker-1:8080".into());
        profiled.profile_enabled = true;

        let mut external = WorkerSpec::legacy("00002".into(), "http://worker-2:8080".into());
        external.profile_enabled = false;
        external.device_kind = "external-device".into();

        let mut actor = WorkerSpec::legacy("00003".into(), "http://worker-3:8080".into());
        actor.profile_enabled = false;

        let mut ordinary = WorkerSpec::legacy("00004".into(), "http://worker-4:8080".into());
        ordinary.profile_enabled = false;

        let active = vec![profiled, external, actor, ordinary];
        assert_eq!(removable_member_indices(&active, 2, true, true), vec![3]);
        assert_eq!(
            removable_member_indices(&active, 2, false, true),
            vec![1, 3]
        );
    }

    #[test]
    fn protected_members_raise_minimum_plateau_floor() {
        let mut profiled = WorkerSpec::legacy("00001".into(), "http://worker-1:8080".into());
        profiled.profile_enabled = true;

        let mut external = WorkerSpec::legacy("00002".into(), "http://worker-2:8080".into());
        external.profile_enabled = false;
        external.device_kind = "external-device".into();

        let mut ordinary = WorkerSpec::legacy("00003".into(), "http://worker-3:8080".into());
        ordinary.profile_enabled = false;
        let mut scratch = WorkerSpec::legacy("00004".into(), "http://worker-4:8080".into());
        scratch.profile_enabled = false;
        scratch.device_kind = "scratch_container".into();
        let workers = vec![profiled, external, ordinary, scratch];

        assert_eq!(protected_member_floor(&workers, true, false), 1);
        assert_eq!(protected_member_floor(&workers, false, true), 1);
        assert_eq!(protected_member_floor(&workers, true, true), 2);
    }

    #[test]
    fn density_actor_selection_reaches_the_floor_for_each_external_only() {
        let mut external_a = WorkerSpec::legacy("ext-a".into(), "http://a".into());
        external_a.profile_enabled = true;
        external_a.device_kind = "raspberry_pi".into();
        let mut external_b = WorkerSpec::legacy("ext-b".into(), "http://b".into());
        external_b.profile_enabled = true;
        external_b.device_kind = "luckfox".into();
        let mut docker = WorkerSpec::legacy("docker".into(), "http://docker".into());
        docker.profile_enabled = true;
        docker.device_kind = "scratch_container".into();
        let active = vec![external_a, external_b, docker];
        let mut counts = HashMap::new();

        while let Some(actor_id) = least_sampled_active_external_id(&active, &counts, 20) {
            *counts.entry(actor_id).or_default() += 1;
        }

        assert_eq!(counts.get("ext-a"), Some(&20));
        assert_eq!(counts.get("ext-b"), Some(&20));
        assert_eq!(counts.get("docker"), None);
    }

    #[test]
    fn two_external_remove_rejoin_uses_non_external_victim_for_receiver_density() {
        let mut external_a = WorkerSpec::legacy("ext-a".into(), "http://a".into());
        external_a.profile_enabled = true;
        external_a.device_kind = "raspberry_pi_5".into();
        let mut external_b = WorkerSpec::legacy("ext-b".into(), "http://b".into());
        external_b.profile_enabled = true;
        external_b.device_kind = "raspberry_pi_3".into();
        let mut profiled_docker = WorkerSpec::legacy("docker".into(), "http://docker".into());
        profiled_docker.profile_enabled = true;
        profiled_docker.device_kind = "scratch_container".into();
        let mut ordinary = WorkerSpec::legacy("ordinary".into(), "http://ordinary".into());
        ordinary.profile_enabled = false;
        let active = vec![external_a, external_b, profiled_docker, ordinary];
        let external_ids = vec!["ext-a".to_string(), "ext-b".to_string()];
        let actor_counts = HashMap::from([
            ("ext-a".to_string(), 20usize),
            ("ext-b".to_string(), 20usize),
        ]);
        let mut receiver_counts = HashMap::new();

        let first = select_supplemental_remove_receiver_roles(
            &active,
            &external_ids,
            &actor_counts,
            &receiver_counts,
            20,
            128,
        )
        .unwrap()
        .unwrap();
        assert_eq!(first, ("ext-b".into(), "ordinary".into(), "ext-a".into()));

        receiver_counts.insert("ext-a".to_string(), 20);
        let second = select_supplemental_remove_receiver_roles(
            &active,
            &external_ids,
            &actor_counts,
            &receiver_counts,
            20,
            128,
        )
        .unwrap()
        .unwrap();
        assert_eq!(second, ("ext-a".into(), "ordinary".into(), "ext-b".into()));

        receiver_counts.insert("ext-b".to_string(), 20);
        assert!(select_supplemental_remove_receiver_roles(
            &active,
            &external_ids,
            &actor_counts,
            &receiver_counts,
            20,
            128,
        )
        .unwrap()
        .is_none());
    }

    #[test]
    fn removal_actor_prefers_actor_that_can_remove_full_batch() {
        let mut protected = WorkerSpec::legacy("00001".into(), "http://worker-1:8080".into());
        protected.profile_enabled = true;

        let mut ordinary_a = WorkerSpec::legacy("00002".into(), "http://worker-2:8080".into());
        ordinary_a.profile_enabled = false;
        let mut ordinary_b = WorkerSpec::legacy("00003".into(), "http://worker-3:8080".into());
        ordinary_b.profile_enabled = false;
        let active = vec![protected, ordinary_a, ordinary_b];
        let mut rng = StdRng::seed_from_u64(7);

        let (actor_idx, removable) =
            choose_remove_actor_and_removable_indices(&active, None, false, true, 2, &mut rng)
                .unwrap();

        assert_eq!(actor_idx, 0);
        assert_eq!(removable, vec![1, 2]);
    }

    #[test]
    fn samples_every_member_when_sample_count_covers_group() {
        assert_eq!(sampled_indices(16, 16), (0..16).collect::<Vec<_>>());
        assert_eq!(
            sampled_indices(5, 16),
            vec![0, 1, 2, 3, 4, 0, 1, 2, 3, 4, 0, 1, 2, 3, 4, 0]
        );
    }

    #[test]
    fn samples_equal_bucket_right_edges_when_group_exceeds_sample_count() {
        assert_eq!(sampled_indices(20, 4), vec![4, 9, 14, 19]);
        assert_eq!(sampled_indices(100, 4), vec![24, 49, 74, 99]);
    }

    #[test]
    fn includes_last_member_when_sampling_large_group() {
        let indices = sampled_indices(100, 16);

        assert_eq!(indices.last(), Some(&99));
        assert!(indices.iter().all(|&idx| idx < 100));
        assert!(indices.windows(2).all(|pair| pair[0] < pair[1]));
    }

    #[test]
    fn commit_receive_sampling_always_includes_profiled_external_devices() {
        let mut recipients = (0..10)
            .map(|idx| {
                WorkerSpec::legacy(format!("worker-{idx}"), format!("http://worker-{idx}:8080"))
            })
            .collect::<Vec<_>>();
        recipients[2].profile_enabled = true;
        recipients[2].device_kind = "raspberry_pi_5".to_string();
        recipients[7].profile_enabled = true;
        recipients[7].device_kind = "luckfox_pico_plus".to_string();

        let (ids, index_map, count) =
            build_commit_receive_sampling_map(&recipients, 1, 42, 10, 3, 0);

        assert!(ids.contains("worker-2"));
        assert!(ids.contains("worker-7"));
        assert_eq!(ids.len(), count);
        assert_eq!(index_map.len(), count);
        assert!(count >= 3, "ordinary capped sample plus two externals");
    }

    #[test]
    fn external_actor_selection_balances_use_counts() {
        let mut workers = (0..3)
            .map(|idx| {
                let mut worker = WorkerSpec::legacy(
                    format!("external-{idx}"),
                    format!("http://external-{idx}:8080"),
                );
                worker.device_kind = "external_device".to_string();
                worker
            })
            .collect::<Vec<_>>();
        workers.push(WorkerSpec::legacy(
            "container".to_string(),
            "http://container:8080".to_string(),
        ));
        let mut counts = HashMap::new();
        let mut rng = StdRng::seed_from_u64(17);

        for _ in 0..12 {
            let actor = least_used_external_actor_id(&workers, &counts, &mut rng).unwrap();
            *counts.entry(actor).or_insert(0usize) += 1;
        }

        assert_eq!(counts.len(), 3);
        assert!(counts.values().all(|count| *count == 4));
        assert!(!counts.contains_key("container"));
    }

    #[test]
    fn external_remove_rejoin_pairs_cover_each_device_in_both_roles() {
        let mut workers = ["external-c", "external-a", "external-b"]
            .into_iter()
            .map(|id| {
                let mut worker = WorkerSpec::legacy(id.to_string(), format!("http://{id}:8080"));
                worker.device_kind = "external_device".to_string();
                worker.profile_enabled = true;
                worker
            })
            .collect::<Vec<_>>();
        let mut local = WorkerSpec::legacy(
            "profiled-local".to_string(),
            "http://profiled-local:8080".to_string(),
        );
        local.profile_enabled = true;
        workers.push(local);

        assert_eq!(
            external_remove_rejoin_pairs(&workers),
            vec![
                ("external-a".to_string(), "external-b".to_string()),
                ("external-b".to_string(), "external-c".to_string()),
                ("external-c".to_string(), "external-a".to_string()),
            ]
        );
    }

    #[test]
    fn remove_rejoin_failures_preserve_attempted_operation() {
        let worker_error: anyhow::Error = WorkerCommandError {
            worker_id: "external-a".to_string(),
            command: "JoinFromWelcome",
            url: "http://external-a:8080/command".to_string(),
            request_id: "request-1".to_string(),
            attempts: 3,
            classification: WorkerCommandErrorClass::TransportRetryable,
            last_error: "connection refused".to_string(),
            diagnostic: None,
        }
        .into();
        assert_eq!(
            remove_rejoin_failure_operation(&worker_error),
            "welcome_receive"
        );

        let fanout_error: anyhow::Error = BatchFanoutError {
            phase: "remove_rejoin.fanout_receive_commit_remove".to_string(),
            operation: "receive_commit".to_string(),
            failures: Vec::new(),
        }
        .into();
        assert_eq!(
            remove_rejoin_failure_operation(&fanout_error),
            "remove_commit"
        );
    }

    #[test]
    fn build_batch_commands_assigns_request_ids() {
        let workers = (1..=2)
            .map(|idx| {
                WorkerSpec::legacy(format!("{idx:05}"), format!("http://worker-{idx:05}:8080"))
            })
            .collect::<Vec<_>>();
        let decision = MembershipBatchDecision {
            requested: 8,
            effective: 8,
            group_cap: 8,
            transition_cap: 8,
            source: "external_density_k8",
        };
        let cursor = BenchmarkCursor::new(3, 40, 32, "membership_add", "add_commit")
            .with_membership_batch(&decision);

        let commands_by_physical = build_batch_commands(&workers, |worker| {
            BatchFanoutCommand {
                client_id: worker.id.clone(),
                request_id: None,
                command: Command::ReceiveCommit {
                    profile: false,
                    commit_create_op: None,
                    commit_receive_sampling_policy: None,
                    commit_receive_sampling_seed: None,
                    commit_receive_sample_index: None,
                    commit_receive_sample_count: None,
                    commit_receive_population_size: None,
                },
                expected_epoch: Some(3),
                phase: Some("test.phase".to_string()),
                profile: None,
                benchmark_plateau_index: None,
                benchmark_target_size: None,
                benchmark_active_size: None,
                benchmark_phase: None,
                benchmark_operation: None,
                benchmark_operation_seq: None,
                benchmark_payload_size: None,
                membership_batch_requested: None,
                membership_batch_effective: None,
                membership_batch_group_cap: None,
                membership_batch_transition_cap: None,
                membership_batch_source: None,
            }
            .with_benchmark_cursor(&cursor)
        });

        let request_ids = commands_by_physical
            .iter()
            .flat_map(|(_, cmds)| cmds.iter())
            .map(|cmd| cmd.request_id.clone().expect("request id"))
            .collect::<Vec<_>>();
        let unique_request_ids = request_ids.iter().collect::<HashSet<_>>();

        assert_eq!(request_ids.len(), workers.len());
        assert_eq!(unique_request_ids.len(), workers.len());
        let command = &commands_by_physical[0].1[0];
        let item = batch_command_item(command);
        assert_eq!(
            command.membership_batch_source.as_deref(),
            Some("external_density_k8")
        );
        assert_eq!(item.membership_batch_requested, Some(8));
        assert_eq!(item.membership_batch_effective, Some(8));
        assert_eq!(
            item.membership_batch_source.as_deref(),
            Some("external_density_k8")
        );
    }

    #[test]
    fn retry_batch_commands_preserves_request_ids_for_failed_clients() {
        let worker_2 = WorkerSpec::legacy("00002".to_string(), "http://worker-00002:8080".into());
        let commands_by_physical = vec![(
            "worker-a".to_string(),
            vec![
                BatchFanoutCommand {
                    client_id: "00001".to_string(),
                    request_id: Some("rid-1".to_string()),
                    command: Command::ReceiveCommit {
                        profile: false,
                        commit_create_op: None,
                        commit_receive_sampling_policy: None,
                        commit_receive_sampling_seed: None,
                        commit_receive_sample_index: None,
                        commit_receive_sample_count: None,
                        commit_receive_population_size: None,
                    },
                    expected_epoch: Some(7),
                    phase: Some("test.phase".to_string()),
                    profile: None,
                    benchmark_plateau_index: None,
                    benchmark_target_size: None,
                    benchmark_active_size: None,
                    benchmark_phase: None,
                    benchmark_operation: None,
                    benchmark_operation_seq: None,
                    benchmark_payload_size: None,
                    membership_batch_requested: None,
                    membership_batch_effective: None,
                    membership_batch_group_cap: None,
                    membership_batch_transition_cap: None,
                    membership_batch_source: None,
                },
                BatchFanoutCommand {
                    client_id: "00002".to_string(),
                    request_id: Some("rid-2".to_string()),
                    command: Command::ReceiveCommit {
                        profile: false,
                        commit_create_op: None,
                        commit_receive_sampling_policy: None,
                        commit_receive_sampling_seed: None,
                        commit_receive_sample_index: None,
                        commit_receive_sample_count: None,
                        commit_receive_population_size: None,
                    },
                    expected_epoch: Some(7),
                    phase: Some("test.phase".to_string()),
                    profile: None,
                    benchmark_plateau_index: None,
                    benchmark_target_size: None,
                    benchmark_active_size: None,
                    benchmark_phase: None,
                    benchmark_operation: None,
                    benchmark_operation_seq: None,
                    benchmark_payload_size: None,
                    membership_batch_requested: None,
                    membership_batch_effective: None,
                    membership_batch_group_cap: None,
                    membership_batch_transition_cap: None,
                    membership_batch_source: None,
                },
            ],
        )];
        let failures = vec![FanoutFailure {
            worker: worker_2,
            error: anyhow!("synthetic batch failure"),
        }];

        let retry_commands = retry_batch_commands_for_failures(&commands_by_physical, &failures);

        assert_eq!(retry_commands.len(), 1);
        assert_eq!(retry_commands[0].1.len(), 1);
        assert_eq!(retry_commands[0].1[0].client_id, "00002");
        assert_eq!(retry_commands[0].1[0].request_id.as_deref(), Some("rid-2"));
    }

    #[test]
    fn reconciled_batch_failure_is_counted_as_success() {
        let worker_2 = WorkerSpec::legacy("00002".to_string(), "http://worker-00002:8080".into());
        let failures = vec![FanoutFailure {
            worker: worker_2,
            error: anyhow!("client 00002 batch error: DS GET failed with status 404 Not Found"),
        }];
        let reconciled_ids = HashSet::from(["00002".to_string()]);

        let (successes, remaining) =
            partition_batch_failures_by_reconciled_state(failures, &reconciled_ids);

        assert_eq!(successes.len(), 1);
        assert_eq!(successes[0].0.id, "00002");
        assert!(remaining.is_empty());
    }

    #[test]
    fn batch_physical_base_url_uses_worker_url() {
        let local_worker = WorkerSpec::legacy("00001".to_string(), "http://127.0.0.1:19481".into());
        let layout_worker = WorkerSpec {
            id: "00002".to_string(),
            url: "http://worker-00002:8080/client/00002".to_string(),
            command_url: "http://worker-00002:8080/client/00002".to_string(),
            health_url: "http://worker-00002:8080/client/00002/health".to_string(),
            physical_worker_id: "worker-00002".to_string(),
            container_mode: super::ContainerMode::Singleton,
            profile_enabled: true,
            device_kind: String::new(),
        };
        let workers_by_id = HashMap::from([
            (local_worker.id.clone(), local_worker),
            (layout_worker.id.clone(), layout_worker),
        ]);

        let local_cmd = BatchFanoutCommand {
            client_id: "00001".to_string(),
            request_id: Some("rid-1".to_string()),
            command: Command::ReceiveCommit {
                profile: false,
                commit_create_op: None,
                commit_receive_sampling_policy: None,
                commit_receive_sampling_seed: None,
                commit_receive_sample_index: None,
                commit_receive_sample_count: None,
                commit_receive_population_size: None,
            },
            expected_epoch: None,
            phase: None,
            profile: None,
            benchmark_plateau_index: None,
            benchmark_target_size: None,
            benchmark_active_size: None,
            benchmark_phase: None,
            benchmark_operation: None,
            benchmark_operation_seq: None,
            benchmark_payload_size: None,
            membership_batch_requested: None,
            membership_batch_effective: None,
            membership_batch_group_cap: None,
            membership_batch_transition_cap: None,
            membership_batch_source: None,
        };
        let layout_cmd = BatchFanoutCommand {
            client_id: "00002".to_string(),
            request_id: Some("rid-2".to_string()),
            command: Command::ReceiveCommit {
                profile: false,
                commit_create_op: None,
                commit_receive_sampling_policy: None,
                commit_receive_sampling_seed: None,
                commit_receive_sample_index: None,
                commit_receive_sample_count: None,
                commit_receive_population_size: None,
            },
            expected_epoch: None,
            phase: None,
            profile: None,
            benchmark_plateau_index: None,
            benchmark_target_size: None,
            benchmark_active_size: None,
            benchmark_phase: None,
            benchmark_operation: None,
            benchmark_operation_seq: None,
            benchmark_payload_size: None,
            membership_batch_requested: None,
            membership_batch_effective: None,
            membership_batch_group_cap: None,
            membership_batch_transition_cap: None,
            membership_batch_source: None,
        };

        assert_eq!(
            batch_physical_base_url("00001", &[local_cmd], &workers_by_id),
            "http://127.0.0.1:19481"
        );
        assert_eq!(
            batch_physical_base_url("worker-00002", &[layout_cmd], &workers_by_id),
            "http://worker-00002:8080"
        );
    }

    #[tokio::test]
    async fn fanout_attempts_all_workers_before_returning_failures() {
        let workers = (1..=4)
            .map(|idx| {
                WorkerSpec::legacy(format!("{idx:05}"), format!("http://worker-{idx:05}:8080"))
            })
            .collect::<Vec<_>>();

        let attempts = Arc::new(AtomicUsize::new(0));
        let mut fanout = FanoutController::new(
            4,
            1,
            false,
            DEFAULT_FANOUT_ERROR_RATE_THRESHOLD,
            FANOUT_LATENCY_SPIKE_P95_MS,
        );
        let attempts_for_op = attempts.clone();

        let result = fanout_workers(
            "test",
            workers.len(),
            "synthetic",
            &workers,
            &mut fanout,
            move |worker| {
                let attempts = attempts_for_op.clone();
                async move {
                    attempts.fetch_add(1, Ordering::SeqCst);
                    if worker.id == "00002" {
                        Err(anyhow!("synthetic transient failure"))
                    } else {
                        Ok(())
                    }
                }
            },
        )
        .await;

        assert!(result.is_err());
        assert_eq!(attempts.load(Ordering::SeqCst), workers.len() + 1);
    }

    #[tokio::test]
    async fn fanout_starts_later_workers_when_one_slot_is_slow() {
        let workers = (1..=4)
            .map(|idx| {
                WorkerSpec::legacy(format!("{idx:05}"), format!("http://worker-{idx:05}:8080"))
            })
            .collect::<Vec<_>>();

        let slow_finished = Arc::new(AtomicUsize::new(0));
        let later_started_before_slow_finished = Arc::new(AtomicUsize::new(0));
        let mut fanout = FanoutController::new(
            2,
            1,
            false,
            DEFAULT_FANOUT_ERROR_RATE_THRESHOLD,
            FANOUT_LATENCY_SPIKE_P95_MS,
        );

        let slow_finished_for_op = Arc::clone(&slow_finished);
        let later_started_for_op = Arc::clone(&later_started_before_slow_finished);

        fanout_workers(
            "test",
            workers.len(),
            "unordered",
            &workers,
            &mut fanout,
            move |worker| {
                let slow_finished = Arc::clone(&slow_finished_for_op);
                let later_started = Arc::clone(&later_started_for_op);
                async move {
                    match worker.id.as_str() {
                        "00001" => tokio::time::sleep(Duration::from_millis(10)).await,
                        "00002" => {
                            tokio::time::sleep(Duration::from_millis(100)).await;
                            slow_finished.store(1, Ordering::SeqCst);
                        }
                        "00003" => {
                            if slow_finished.load(Ordering::SeqCst) == 0 {
                                later_started.store(1, Ordering::SeqCst);
                            }
                        }
                        _ => {}
                    }
                    Ok(())
                }
            },
        )
        .await
        .expect("synthetic fanout should succeed");

        assert_eq!(later_started_before_slow_finished.load(Ordering::SeqCst), 1);
    }
}

#[cfg(test)]
mod failure_experiment_tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    static TEST_COUNTER: AtomicUsize = AtomicUsize::new(1);

    fn temp_dir() -> PathBuf {
        let n = TEST_COUNTER.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir()
            .join("mls_failure_experiment_test")
            .join(format!("test_{}_{}", std::process::id(), n));
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn classify_worker_error_detects_container_exit() {
        let err = anyhow!("Connection refused (os error 111)");
        assert_eq!(classify_worker_error(&err), "container_exit");
    }

    #[test]
    fn classify_worker_error_detects_timeout() {
        let err = anyhow!("request timeout after 30s");
        assert_eq!(classify_worker_error(&err), "cpu_starvation_timeout");
    }

    #[test]
    fn classify_worker_error_detects_app_heap_budget_exceeded() {
        let err = anyhow!(
            "Worker 00001 error: APP_HEAP_BUDGET_EXCEEDED failure_class=app_heap_budget_exceeded operation_family=welcome_receive member_count=32 epoch=31"
        );
        assert_eq!(classify_worker_error(&err), "app_heap_budget_exceeded");
    }

    #[test]
    fn classify_worker_error_detects_connect_failure() {
        let err = anyhow!("client error: connect failure");
        assert_eq!(classify_worker_error(&err), "worker_unreachable");
    }

    #[test]
    fn queued_epoch_race_message_is_detected() {
        assert!(is_queued_epoch_race_message(
            "remove_members for [\"00494\"] lost the epoch race and was queued for retry: Commit epoch mismatch"
        ));
        assert!(!is_queued_epoch_race_message(
            "members [\"00494\"] removed locally; group commit published"
        ));
    }

    #[test]
    fn classify_worker_error_defaults_to_infrastructure() {
        let err = anyhow!("unknown internal error");
        assert_eq!(classify_worker_error(&err), "infrastructure_failure");
    }

    #[tokio::test]
    async fn record_failure_writes_runner_event_and_profile() {
        let run_dir = temp_dir();
        let events = RunnerEventLog::new(&run_dir);

        let worker =
            WorkerSpec::legacy("test-001".to_string(), "http://127.0.0.1:9999".to_string());
        let cursor = BenchmarkCursor::new(0, 4, 4, "transition", "add_members");
        let err = anyhow!("simulated container exit");

        events
            .record_failure(
                &cursor,
                &worker,
                &err,
                "container_exit",
                Some("runner_inference"),
                Some("Connection refused after 10 retries"),
                "evict_singleton_and_continue",
                None,
            )
            .unwrap();

        let runner_jsonl = run_dir.join("runner-events.jsonl");
        assert!(runner_jsonl.exists());

        let content = fs::read_to_string(&runner_jsonl).unwrap();
        let first_line = content.lines().next().unwrap();
        let event: RunnerEvent = serde_json::from_str(first_line).unwrap();
        assert_eq!(event.event_kind, "worker_failure");
        assert_eq!(event.failure_class, "container_exit");
        assert_eq!(event.failed_worker_id, "test-001");
        assert_eq!(event.benchmark_phase, "transition");
        assert_eq!(event.benchmark_operation, "add_members");
        assert_eq!(
            event.failure_evidence_source,
            Some("runner_inference".to_string())
        );

        let profile_path = run_dir.join("client-test-001.jsonl");
        assert!(profile_path.exists());

        let _ = fs::remove_dir_all(&run_dir);
    }

    #[tokio::test]
    async fn profiled_failure_policy_controls_continuation() {
        let run_dir = temp_dir();
        let events = RunnerEventLog::new(&run_dir);
        let worker = WorkerSpec::legacy("00001".to_string(), "http://127.0.0.1:9999".to_string());
        let cursor = BenchmarkCursor::new(1, 2, 1, "membership_add", "generate_key_package");
        let err = anyhow!("APP_HEAP_BUDGET_EXCEEDED failure_class=app_heap_budget_exceeded");

        FAILURE_EXPERIMENT_MODE.store(false, Ordering::Relaxed);
        store_profiled_failure_policy(ProfiledFailurePolicy::StopOnProfiledFailure);
        let should_continue = record_profiled_worker_failure(
            &events,
            &cursor,
            &worker,
            &err,
            "drop_idle_joiner",
            None,
        )
        .await
        .unwrap();
        assert!(!should_continue);

        store_profiled_failure_policy(ProfiledFailurePolicy::RemoveAndContinue);
        let should_continue = record_profiled_worker_failure(
            &events,
            &cursor,
            &worker,
            &err,
            "drop_idle_joiner",
            None,
        )
        .await
        .unwrap();
        assert!(should_continue);

        store_profiled_failure_policy(ProfiledFailurePolicy::StopOnProfiledFailure);
        let _ = fs::remove_dir_all(&run_dir);
    }

    #[test]
    fn detects_queued_remove_members_republished_by_process_pending() {
        assert!(queued_remove_members_republished(
            "external commit received and processed; DS group state updated; queued remove_members for [\"00688\"] was retried and published"
        ));
        assert!(!queued_remove_members_republished(
            "external commit received and processed; DS group state updated; queued add_members for [\"00688\"] was retried and published"
        ));
    }

    #[tokio::test]
    async fn batch_app_heap_failures_become_dead_workers_for_attrition() {
        let run_dir = temp_dir();
        let events = RunnerEventLog::new(&run_dir);
        let cursor = BenchmarkCursor::new(4, 50, 50, "oom_eviction", "receive_commit");
        let worker = WorkerSpec::legacy("00042".to_string(), "http://127.0.0.1:9999".to_string());
        let reassigned_to =
            WorkerSpec::legacy("00001".to_string(), "http://127.0.0.1:9998".to_string());
        let error = anyhow!(
            "APP_HEAP_BUDGET_EXCEEDED failure_class=app_heap_budget_exceeded memory_model=app-heap-budget operation_family=commit_receive benchmark_operation=self_update span_or_phase=oom_eviction.fanout_receive_commit member_count=50 epoch=24 worker_id=00042 resource_profile_id=ram_app_heap_512k resource_profile_index=3 app_heap_budget=512k app_heap_budget_bytes=524288 configured_heap_budget_bytes=524288 current_live_heap_bytes=533430 peak_live_heap_bytes=533430 operation_peak_live_heap_bytes=533430 total_allocated_bytes=23941687 allocation_count=158199 deallocation_count=157172 failed_allocation_size_bytes=49723"
        );
        let batch_error: anyhow::Error = BatchFanoutError {
            phase: "oom_eviction".to_string(),
            operation: "receive_commit".to_string(),
            failures: vec![FanoutFailure {
                worker: worker.clone(),
                error,
            }],
        }
        .into();

        FAILURE_EXPERIMENT_MODE.store(false, Ordering::Relaxed);
        store_profiled_failure_policy(ProfiledFailurePolicy::RemoveAndContinue);

        let dead_workers = record_batch_oom_failures(
            &events,
            &cursor,
            &batch_error,
            "evict_oom_eviction_recipient_and_retry",
            Some(&reassigned_to),
        )
        .await
        .unwrap()
        .expect("app-heap fanout failure should be recoverable under remove-and-continue");

        assert_eq!(dead_workers.len(), 1);
        assert_eq!(dead_workers[0].id, worker.id);

        let runner_jsonl = run_dir.join("runner-events.jsonl");
        let content = fs::read_to_string(&runner_jsonl).unwrap();
        let event: RunnerEvent = serde_json::from_str(content.lines().next().unwrap()).unwrap();
        assert_eq!(event.failure_class, "app_heap_budget_exceeded");
        assert_eq!(
            event.failure_action,
            "evict_oom_eviction_recipient_and_retry"
        );

        store_profiled_failure_policy(ProfiledFailurePolicy::StopOnProfiledFailure);
        let _ = fs::remove_dir_all(&run_dir);
    }

    #[test]
    fn record_failure_includes_resource_profile_in_csv() {
        let run_dir = temp_dir();
        fs::create_dir_all(&run_dir).unwrap();

        let layout = WorkerLayout {
            version: 2,
            logical_worker_count: 4,
            physical_worker_count: 3,
            layout_mode: "hybrid".to_string(),
            singleton_min_count: 2,
            singleton_fraction: 0.5,
            packed_clients_per_container: 2,
            singleton_selection_seed: 1,
            profile_policy: "singletons_only".to_string(),
            clients: vec![
                WorkerLayoutClient {
                    client_id: "00001".to_string(),
                    physical_worker_id: "worker-00001".to_string(),
                    container_mode: "singleton".to_string(),
                    profile_enabled: true,
                    command_url: "http://worker-00001:8080/client/00001".to_string(),
                    health_url: "http://worker-00001:8080/client/00001/health".to_string(),
                    execution_backend: "docker_container".to_string(),
                    device_kind: "scratch_container".to_string(),
                    transport: "".to_string(),
                    access_backend: "".to_string(),
                    arch: "".to_string(),
                    rust_target: "".to_string(),
                },
                WorkerLayoutClient {
                    client_id: "00002".to_string(),
                    physical_worker_id: "worker-pack-000".to_string(),
                    container_mode: "packed".to_string(),
                    profile_enabled: false,
                    command_url: "http://worker-pack-000:8080/client/00002".to_string(),
                    health_url: "http://worker-pack-000:8080/client/00002/health".to_string(),
                    execution_backend: "docker_container".to_string(),
                    device_kind: "scratch_container".to_string(),
                    transport: "".to_string(),
                    access_backend: "".to_string(),
                    arch: "".to_string(),
                    rust_target: "".to_string(),
                },
                WorkerLayoutClient {
                    client_id: "00003".to_string(),
                    physical_worker_id: "worker-pack-000".to_string(),
                    container_mode: "packed".to_string(),
                    profile_enabled: false,
                    command_url: "http://worker-pack-000:8080/client/00003".to_string(),
                    health_url: "http://worker-pack-000:8080/client/00003/health".to_string(),
                    execution_backend: "docker_container".to_string(),
                    device_kind: "scratch_container".to_string(),
                    transport: "".to_string(),
                    access_backend: "".to_string(),
                    arch: "".to_string(),
                    rust_target: "".to_string(),
                },
                WorkerLayoutClient {
                    client_id: "00004".to_string(),
                    physical_worker_id: "worker-00004".to_string(),
                    container_mode: "singleton".to_string(),
                    profile_enabled: true,
                    command_url: "http://worker-00004:8080/client/00004".to_string(),
                    health_url: "http://worker-00004:8080/client/00004/health".to_string(),
                    execution_backend: "docker_container".to_string(),
                    device_kind: "scratch_container".to_string(),
                    transport: "".to_string(),
                    access_backend: "".to_string(),
                    arch: "".to_string(),
                    rust_target: "".to_string(),
                },
            ],
            physical_workers: vec![
                WorkerLayoutPhysicalWorker {
                    physical_worker_id: "worker-00001".to_string(),
                    container_mode: "singleton".to_string(),
                    client_ids: vec!["00001".to_string()],
                    base_url: "http://worker-00001:8080".to_string(),
                    profile_enabled_client_ids: vec!["00001".to_string()],
                    resource_limit_cpus: Some(0.125),
                    resource_limit_memory: Some("64m".to_string()),
                    resource_limit_memory_bytes: None,
                    resource_limit_memory_swap: Some("64m".to_string()),
                    resource_limit_memory_swap_bytes: None,
                    memory_model: String::new(),
                    docker_memory_limit: String::new(),
                    app_heap_budget: String::new(),
                    app_heap_budget_bytes: None,
                    resource_limit_pids: None,
                    resource_profile: "failure-experiment-resource-envelope_cpus-0.125_memory-64m"
                        .to_string(),
                    resource_profile_id: "cpu_1c_012".to_string(),
                    resource_experiment_type: "cpu_matrix_singleton".to_string(),
                    cpu_capacity_fraction: Some(0.125),
                    assigned_core_count: Some(1),
                    cpuset: Some("0".to_string()),
                    profiled_singleton: true,
                    execution_backend: "docker_container".to_string(),
                    device_kind: "scratch_container".to_string(),
                    transport: "".to_string(),
                    access_backend: "".to_string(),
                    arch: "".to_string(),
                    rust_target: "".to_string(),
                },
                WorkerLayoutPhysicalWorker {
                    physical_worker_id: "worker-pack-000".to_string(),
                    container_mode: "packed".to_string(),
                    client_ids: vec!["00002".to_string(), "00003".to_string()],
                    base_url: "http://worker-pack-000:8080".to_string(),
                    profile_enabled_client_ids: vec![],
                    resource_limit_cpus: None,
                    resource_limit_memory: None,
                    resource_limit_memory_bytes: None,
                    resource_limit_memory_swap: None,
                    resource_limit_memory_swap_bytes: None,
                    memory_model: String::new(),
                    docker_memory_limit: String::new(),
                    app_heap_budget: String::new(),
                    app_heap_budget_bytes: None,
                    resource_limit_pids: None,
                    resource_profile: "".to_string(),
                    resource_profile_id: "".to_string(),
                    resource_experiment_type: "".to_string(),
                    cpu_capacity_fraction: None,
                    assigned_core_count: None,
                    cpuset: None,
                    profiled_singleton: false,
                    execution_backend: "docker_container".to_string(),
                    device_kind: "scratch_container".to_string(),
                    transport: "".to_string(),
                    access_backend: "".to_string(),
                    arch: "".to_string(),
                    rust_target: "".to_string(),
                },
                WorkerLayoutPhysicalWorker {
                    physical_worker_id: "worker-00004".to_string(),
                    container_mode: "singleton".to_string(),
                    client_ids: vec!["00004".to_string()],
                    base_url: "http://worker-00004:8080".to_string(),
                    profile_enabled_client_ids: vec!["00004".to_string()],
                    resource_limit_cpus: Some(2.0),
                    resource_limit_memory: Some("1g".to_string()),
                    resource_limit_memory_bytes: None,
                    resource_limit_memory_swap: Some("1g".to_string()),
                    resource_limit_memory_swap_bytes: None,
                    memory_model: String::new(),
                    docker_memory_limit: String::new(),
                    app_heap_budget: String::new(),
                    app_heap_budget_bytes: None,
                    resource_limit_pids: None,
                    resource_profile: "failure-experiment-resource-envelope_cpus-2.0_memory-1g"
                        .to_string(),
                    resource_profile_id: "cpu_2c_100".to_string(),
                    resource_experiment_type: "cpu_matrix_singleton".to_string(),
                    cpu_capacity_fraction: Some(1.0),
                    assigned_core_count: Some(2),
                    cpuset: Some("0-1".to_string()),
                    profiled_singleton: true,
                    execution_backend: "docker_container".to_string(),
                    device_kind: "scratch_container".to_string(),
                    transport: "".to_string(),
                    access_backend: "".to_string(),
                    arch: "".to_string(),
                    rust_target: "".to_string(),
                },
            ],
            execution_backend: String::new(),
            device_kind: String::new(),
            transport: String::new(),
            access_backend: String::new(),
            arch: String::new(),
            rust_target: String::new(),
            failure_experiment: Some(FailureExperimentConfig {
                mode: "failure_experiment".to_string(),
                seed: 1,
                cpu_caps: vec![0.125, 0.25, 0.5, 1.0, 2.0],
                ram_caps: vec![
                    "64m", "96m", "128m", "192m", "256m", "384m", "512m", "768m", "1g",
                ]
                .into_iter()
                .map(|s| s.to_string())
                .collect(),
                grid_cells: 45,
                swap_equals_ram: true,
                interpretation: String::new(),
            }),
        };

        let layout_json = serde_json::to_string(&layout).unwrap();
        let layout_path = run_dir.join("worker_layout.json");
        fs::write(&layout_path, &layout_json).unwrap();

        let events = RunnerEventLog::new(&run_dir);
        let worker =
            WorkerSpec::legacy("00001".to_string(), "http://worker-00001:8080".to_string());
        let cursor = BenchmarkCursor::new(1, 8, 8, "update", "self_update");
        let err = anyhow!("container OOM killed");

        let evidence = OomEvidence {
            ts_unix_ns: Some(1000),
            source: "docker_cgroup".to_string(),
            worker_id: Some("00001".to_string()),
            physical_worker_id: Some("worker-00001".to_string()),
            detail: Some("oom_kill counter incremented".to_string()),
        };

        events
            .record_oom_failure(&cursor, &worker, &err, &evidence, "evict_and_retry", None)
            .unwrap();

        let runner_jsonl = run_dir.join("runner-events.jsonl");
        let content = fs::read_to_string(&runner_jsonl).unwrap();
        let first_line = content.lines().next().unwrap();
        let event: RunnerEvent = serde_json::from_str(first_line).unwrap();
        assert_eq!(event.failure_class, "oom_kill");
        assert_eq!(event.failed_worker_id, "00001");
        assert_eq!(event.benchmark_plateau_index, 1);
        assert_eq!(event.benchmark_target_size, 8);

        let profile_path = run_dir.join("client-00001.jsonl");
        assert!(profile_path.exists());

        let _ = fs::remove_dir_all(&run_dir);
    }
}
