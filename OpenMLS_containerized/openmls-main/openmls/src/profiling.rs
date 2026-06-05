//ADDED THIS ENTIRE FILE FOR THE MASTERS THESIS PROJECT!!!
use std::{
    cell::{Cell, RefCell},
    collections::HashMap,
    fs::{self, File, OpenOptions},
    io::{BufWriter, Write},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Mutex, OnceLock,
    },
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use cpu_time::{ProcessTime, ThreadTime};
use l1d_cache_counter::L1DCacheCounterScope;
use serde::Serialize;

static PROFILE_WRITER: OnceLock<Option<Mutex<BufWriter<File>>>> = OnceLock::new();
static CPU_STAT_PATH: OnceLock<Option<PathBuf>> = OnceLock::new();
static CPU_LIMIT_CORES: OnceLock<Option<f64>> = OnceLock::new();
static MEMORY_LIMIT_BYTES: OnceLock<Option<u64>> = OnceLock::new();
static PAGE_SIZE_BYTES: OnceLock<u64> = OnceLock::new();
static TREE_HASH_NODES_TOUCHED: AtomicU64 = AtomicU64::new(0);
static PARENT_HASH_NODES_TOUCHED: AtomicU64 = AtomicU64::new(0);
static PATH_SECRET_DERIVATION_COUNT: AtomicU64 = AtomicU64::new(0);
static NODE_SECRET_DERIVATION_COUNT: AtomicU64 = AtomicU64::new(0);
static HPKE_ENCRYPT_COUNT: AtomicU64 = AtomicU64::new(0);
static HPKE_DECRYPT_COUNT: AtomicU64 = AtomicU64::new(0);
static NEXT_SPAN_ID: AtomicU64 = AtomicU64::new(1);

thread_local! {
    static SPAN_STACK: RefCell<Vec<(u64, String)>> = const { RefCell::new(Vec::new()) };
    /// Propagated sender_generation from the most recent sender_data decryption,
    /// consumed by the parent receive protocol span.
    pub(crate) static LAST_SENDER_GENERATION: Cell<Option<u64>> = const { Cell::new(None) };
    /// Propagated app_msg_ciphertext_bytes from the most recent content decrypt,
    /// consumed by the parent receive protocol span.
    pub(crate) static LAST_CIPHERTEXT_BYTES: Cell<Option<usize>> = const { Cell::new(None) };
    /// Propagated receive-sequence metadata from the most recent receive,
    /// consumed by the parent receive protocol span.
    pub(crate) static LAST_FIRST_RECEIVE: Cell<Option<bool>> = const { Cell::new(None) };
    pub(crate) static LAST_GENERATION_GAP: Cell<Option<u64>> = const { Cell::new(None) };
    pub(crate) static LAST_OUT_OF_ORDER: Cell<Option<bool>> = const { Cell::new(None) };
    static COMMIT_RECEIVE_CONTEXT: RefCell<Option<CommitReceiveContext>> = const { RefCell::new(None) };
    static BENCHMARK_CONTEXT: RefCell<Option<BenchmarkContext>> = const { RefCell::new(None) };
    /// Worker/client ID for span identity.
    /// Set before each command by the benchmark runner.
    pub(crate) static WORKER_ID: RefCell<Option<String>> = const { RefCell::new(None) };
}

#[derive(Clone, Debug, Default)]
#[allow(dead_code, missing_docs)]
pub struct CommitReceiveContext {
    pub commit_create_op: Option<String>,
    pub commit_receive_sampling_policy: Option<String>,
    pub commit_receive_sampling_seed: Option<u64>,
    pub commit_receive_sample_index: Option<usize>,
    pub commit_receive_sample_count: Option<usize>,
    pub commit_receive_population_size: Option<usize>,
    pub commit_id: Option<String>,
    pub group_epoch: Option<u64>,
    pub tree_size: Option<u32>,
    pub member_count: Option<usize>,
    pub ciphersuite: Option<String>,
}

#[derive(Clone, Debug, Default)]
#[allow(missing_docs)]
pub struct BenchmarkContext {
    pub benchmark_plateau_index: Option<usize>,
    pub benchmark_target_size: Option<usize>,
    pub benchmark_active_size: Option<usize>,
    pub benchmark_phase: Option<String>,
    pub benchmark_operation: Option<String>,
    pub benchmark_operation_seq: Option<usize>,
    pub benchmark_payload_size: Option<usize>,
    /// Human-readable label for the configured payload size (e.g. "32", "256").
    /// Distinct from actual measured sizes like app_msg_plaintext_bytes.
    pub configured_payload_label: Option<String>,
    pub device_kind: Option<String>,
    pub execution_backend: Option<String>,
    pub ciphersuite: Option<String>,
}

pub fn set_benchmark_context(ctx: BenchmarkContext) {
    BENCHMARK_CONTEXT.with(|slot| {
        *slot.borrow_mut() = Some(ctx);
    });
}

pub fn clear_benchmark_context() {
    BENCHMARK_CONTEXT.with(|slot| {
        *slot.borrow_mut() = None;
    });
}

pub fn set_commit_receive_context(ctx: CommitReceiveContext) {
    COMMIT_RECEIVE_CONTEXT.with(|slot| {
        *slot.borrow_mut() = Some(ctx);
    });
}

pub fn clear_commit_receive_context() {
    COMMIT_RECEIVE_CONTEXT.with(|slot| {
        *slot.borrow_mut() = None;
    });
}

pub fn set_worker_id(id: String) {
    WORKER_ID.with(|slot| {
        *slot.borrow_mut() = Some(id);
    });
}

pub fn clear_worker_id() {
    WORKER_ID.with(|slot| {
        *slot.borrow_mut() = None;
    });
}

#[allow(dead_code)]
pub fn with_commit_receive_context(fill: impl FnOnce(&CommitReceiveContext)) {
    COMMIT_RECEIVE_CONTEXT.with(|slot| {
        if let Some(ctx) = slot.borrow().as_ref() {
            fill(ctx);
        }
    });
}

/// Per-(epoch, sender) receive-sequence tracker.
/// Updated after every successful application message receive.
/// Uses a Mutex to allow interior mutability from &self methods.
static RECEIVE_TRACKER: OnceLock<Mutex<HashMap<(Vec<u8>, u64, u32), u64>>> = OnceLock::new();

fn receive_tracker() -> &'static Mutex<HashMap<(Vec<u8>, u64, u32), u64>> {
    RECEIVE_TRACKER.get_or_init(|| Mutex::new(HashMap::new()))
}

pub(crate) fn compute_receive_sequence(
    group_id: &[u8],
    epoch: u64,
    sender_leaf: u32,
    generation: u64,
) -> (bool, u64, bool) {
    let mut map = receive_tracker().lock().unwrap();
    let key = (group_id.to_vec(), epoch, sender_leaf);
    let first = !map.contains_key(&key);
    let last = map.get(&key).copied().unwrap_or(0);
    let gap = if generation >= last + 1 {
        generation - (last + 1)
    } else {
        0
    };
    let ooo = !first && generation != last + 1;
    map.insert(key, generation.max(last));
    (first, gap, ooo)
}

fn profile_path() -> Option<PathBuf> {
    std::env::var_os("OPENMLS_PROFILE_PATH").map(PathBuf::from)
}

fn writer() -> &'static Option<Mutex<BufWriter<File>>> {
    PROFILE_WRITER.get_or_init(|| {
        let path = match profile_path() {
            Some(p) => p,
            None => return None,
        };

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .ok()?;

        Some(Mutex::new(BufWriter::new(file)))
    })
}

fn unix_timestamp_ns() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0)
}

fn current_pid() -> u32 {
    std::process::id()
}

fn current_thread_id() -> String {
    format!("{:?}", std::thread::current().id())
}

fn env_or_none(key: &str) -> Option<String> {
    std::env::var(key).ok().filter(|s| !s.is_empty())
}

fn env_u64_or_none(key: &str) -> Option<u64> {
    std::env::var(key).ok()?.parse().ok()
}

fn env_positive_u64_or_none(key: &str) -> Option<u64> {
    env_u64_or_none(key).filter(|value| *value > 0)
}

fn env_positive_f64_or_none(key: &str) -> Option<f64> {
    std::env::var(key)
        .ok()?
        .parse::<f64>()
        .ok()
        .filter(|value| value.is_finite() && *value > 0.0)
}

fn measurement_class_for_op(op: &str) -> &'static str {
    if op.ends_with("_protocol")
        || op.contains("_protocol_")
        || op.ends_with(".welcome_build")
        || op.ends_with(".welcome_group_secrets_encrypt")
        || op.ends_with(".proposal_apply")
        || op.ends_with(".key_schedule_step")
        || op.starts_with("join_from_welcome.")
        || op.starts_with("application_message_create.")
        || op.starts_with("application_message_receive.")
        || op.starts_with("commit_receive.")
        || op == "commit_receive_protocol"
    {
        "protocol"
    } else if op.ends_with("_serialize") {
        "serialize"
    } else if op.contains("_deserialize") {
        "deserialize"
    } else {
        "other"
    }
}

fn measurement_plane_for_op(op: &str) -> &'static str {
    if op.contains("serialize") || op.contains("deserialize") {
        "serialization"
    } else if op.starts_with("self_update.")
        || op.starts_with("commit_add.")
        || op.starts_with("commit_remove.")
        || op.starts_with("commit_receive.")
        || op.starts_with("join_from_welcome.")
        || op.starts_with("application_message_create.")
        || op.starts_with("application_message_receive.")
    {
        "protocol_scaling"
    } else if op.starts_with("update_path_") {
        "protocol_scaling"
    } else if op.ends_with("_protocol") || op.contains("_protocol_") {
        "openmls_implementation"
    } else {
        "openmls_implementation"
    }
}

fn span_kind_for_op(op: &str) -> &'static str {
    if op.contains("serialize") || op.contains("deserialize") {
        "serialization"
    } else if op.ends_with(".path_secret_derive")
        || op.ends_with(".path_hpke_encrypt")
        || op.ends_with(".group_secrets_hpke_decrypt")
        || op.ends_with(".aead_encrypt")
        || op.ends_with(".aead_decrypt")
    {
        "crypto_primitive"
    } else if op.ends_with(".tree_hash_recompute")
        || op.ends_with(".parent_hash_recompute")
        || op.ends_with(".path_structure_build")
        || op.ends_with(".tree_restructure")
    {
        "tree_structure"
    } else if op.ends_with(".key_schedule_step") {
        "key_schedule"
    } else if op == "join_from_welcome.group_info_signature_verify" {
        "authentication"
    } else if op.starts_with("join_from_welcome.") {
        "protocol_core"
    } else if op.starts_with("self_update.")
        || op.starts_with("commit_add.")
        || op.starts_with("commit_remove.")
    {
        "protocol_core"
    } else if op == "application_message_create.secret_tree_derive" {
        "key_schedule"
    } else if op == "application_message_create.sender_data_encrypt"
        || op == "application_message_create.content_encrypt"
    {
        "crypto_primitive"
    } else if op.starts_with("application_message_create.") {
        "protocol_core"
    } else if op == "application_message_receive.secret_tree_lookup_or_derive" {
        "key_schedule"
    } else if op == "application_message_receive.sender_data_decrypt"
        || op == "application_message_receive.content_decrypt"
    {
        "crypto_primitive"
    } else if op == "application_message_receive.auth_verify" {
        "authentication"
    } else if op == "commit_receive.message_auth_verify"
        || op == "commit_receive.confirmation_tag_verify"
    {
        "authentication"
    } else if op == "commit_receive.proposal_apply" || op == "commit_receive.proposal_resolve" {
        "protocol_core"
    } else if op == "commit_receive.update_path_validate"
        || op == "commit_receive.tree_hash_recompute"
        || op == "commit_receive.parent_hash_verify"
    {
        "tree_structure"
    } else if op == "commit_receive.path_secret_decrypt" {
        "crypto_primitive"
    } else if op == "commit_receive.key_schedule_step" {
        "key_schedule"
    } else if op == "commit_receive.group_state_install" {
        "state_construction"
    } else if op.starts_with("commit_receive.") {
        "protocol_core"
    } else if op == "application_message_receive.payload_extract" {
        "payload_handling"
    } else if op.starts_with("application_message_receive.") {
        "protocol_core"
    } else if op.starts_with("update_path_") {
        "tree_structure"
    } else if op.contains("welcome") {
        "openmls_api"
    } else if op.contains("join_from_welcome") {
        "openmls_api"
    } else if op.contains("application_message") {
        "openmls_api"
    } else if op.contains("commit_create") {
        "openmls_api"
    } else {
        "openmls_api"
    }
}

fn next_span_id() -> u64 {
    NEXT_SPAN_ID.fetch_add(1, Ordering::Relaxed)
}

fn current_parent_span_id() -> Option<u64> {
    SPAN_STACK.with(|stack| stack.borrow().last().map(|&(id, _)| id))
}

fn current_parent_operation() -> Option<String> {
    SPAN_STACK.with(|stack| stack.borrow().last().map(|(_, op)| op.clone()))
}

fn push_span_id(span_id: u64, op_name: String) {
    SPAN_STACK.with(|stack| stack.borrow_mut().push((span_id, op_name)));
}

fn pop_span_id(span_id: u64) {
    SPAN_STACK.with(|stack| {
        let mut stack = stack.borrow_mut();
        if stack.last().map(|&(id, _)| id) == Some(span_id) {
            stack.pop();
        } else if let Some(position) = stack.iter().rposition(|&(id, _)| id == span_id) {
            stack.remove(position);
        }
    });
}

fn cgroup_file_candidates(controller: &str, file_name: &str) -> Vec<PathBuf> {
    let mut candidates = Vec::new();

    if let Ok(cgroups) = fs::read_to_string("/proc/self/cgroup") {
        for line in cgroups.lines() {
            let mut parts = line.splitn(3, ':');
            let _hierarchy = parts.next();
            let controllers = parts.next().unwrap_or_default();
            let raw_path = parts.next().unwrap_or_default();
            let rel_path = raw_path.trim_start_matches('/');

            if controllers.is_empty() {
                candidates.push(
                    PathBuf::from("/sys/fs/cgroup")
                        .join(rel_path)
                        .join(file_name),
                );
            } else if controllers
                .split(',')
                .any(|c| c == controller || (controller == "cpu" && c == "cpuacct"))
            {
                candidates.push(
                    PathBuf::from("/sys/fs/cgroup")
                        .join(controllers)
                        .join(rel_path)
                        .join(file_name),
                );
                candidates.push(
                    PathBuf::from("/sys/fs/cgroup")
                        .join(controller)
                        .join(rel_path)
                        .join(file_name),
                );
                candidates.push(
                    PathBuf::from("/sys/fs/cgroup")
                        .join(rel_path)
                        .join(file_name),
                );
            }
        }
    }

    candidates.push(PathBuf::from("/sys/fs/cgroup").join(file_name));
    candidates
}

fn first_existing_cgroup_file(controller: &str, file_name: &str) -> Option<PathBuf> {
    cgroup_file_candidates(controller, file_name)
        .into_iter()
        .find(|path| path.exists())
}

fn parse_keyed_u128(contents: &str, key: &str) -> Option<u128> {
    contents.lines().find_map(|line| {
        let mut parts = line.split_whitespace();
        if parts.next()? == key {
            parts.next()?.parse::<u128>().ok()
        } else {
            None
        }
    })
}

fn read_cpu_throttled_ns(path: &Path) -> Option<u128> {
    let contents = fs::read_to_string(path).ok()?;
    if let Some(throttled_usec) = parse_keyed_u128(&contents, "throttled_usec") {
        return throttled_usec.checked_mul(1_000);
    }
    parse_keyed_u128(&contents, "throttled_time")
}

fn current_cpu_throttled_ns() -> Option<u128> {
    // Some embedded kernels expose no cgroup CPU-throttling counter.
    // This metric is cgroup quota throttling, so unsupported counters use zero.
    let counter = CPU_STAT_PATH.get_or_init(|| first_existing_cgroup_file("cpu", "cpu.stat"));
    Some(
        counter
            .as_ref()
            .and_then(|path| read_cpu_throttled_ns(path))
            .unwrap_or(0),
    )
}

fn read_cpu_max_limit(path: &Path) -> Option<f64> {
    let contents = fs::read_to_string(path).ok()?;
    let mut parts = contents.split_whitespace();
    let quota = parts.next()?;
    let period = parts.next()?.parse::<f64>().ok()?;
    if quota == "max" || period <= 0.0 {
        return None;
    }
    let quota = quota.parse::<f64>().ok()?;
    (quota > 0.0).then_some(quota / period)
}

fn read_i64_file(path: &Path) -> Option<i64> {
    fs::read_to_string(path).ok()?.trim().parse::<i64>().ok()
}

fn effective_cpu_limit_cores() -> Option<f64> {
    *CPU_LIMIT_CORES.get_or_init(|| {
        if let Some(value) = env_positive_f64_or_none("OPENMLS_EFFECTIVE_CPU_LIMIT_CORES") {
            return Some(value);
        }

        if let Some(path) = first_existing_cgroup_file("cpu", "cpu.max") {
            if let Some(value) = read_cpu_max_limit(&path) {
                return Some(value);
            }
        }

        let quota_path = first_existing_cgroup_file("cpu", "cpu.cfs_quota_us")?;
        let period_path = first_existing_cgroup_file("cpu", "cpu.cfs_period_us")?;
        let quota = read_i64_file(&quota_path)?;
        let period = read_i64_file(&period_path)?;
        if quota > 0 && period > 0 {
            Some(quota as f64 / period as f64)
        } else {
            None
        }
    })
}

fn read_memory_limit(path: &Path) -> Option<u64> {
    let contents = fs::read_to_string(path).ok()?;
    let token = contents.split_whitespace().next()?;
    if token == "max" {
        return None;
    }
    let value = token.parse::<u64>().ok()?;
    if value > 0 && value < (1u64 << 60) {
        Some(value)
    } else {
        None
    }
}

fn mem_total_bytes() -> Option<u64> {
    let contents = fs::read_to_string("/proc/meminfo").ok()?;
    for line in contents.lines() {
        if let Some(rest) = line.strip_prefix("MemTotal:") {
            let kb = rest.split_whitespace().next()?.parse::<u64>().ok()?;
            return kb.checked_mul(1024);
        }
    }
    None
}

fn page_size_bytes() -> u64 {
    *PAGE_SIZE_BYTES
        .get_or_init(|| env_positive_u64_or_none("OPENMLS_PAGE_SIZE_BYTES").unwrap_or(4096))
}

fn effective_memory_limit_bytes() -> Option<u64> {
    *MEMORY_LIMIT_BYTES.get_or_init(|| {
        if let Some(value) = env_positive_u64_or_none("OPENMLS_EFFECTIVE_MEMORY_LIMIT_BYTES") {
            return Some(value);
        }

        if let Some(path) = first_existing_cgroup_file("memory", "memory.max") {
            if let Some(value) = read_memory_limit(&path) {
                return Some(value);
            }
        }

        if let Some(path) = first_existing_cgroup_file("memory", "memory.limit_in_bytes") {
            if let Some(value) = read_memory_limit(&path) {
                return Some(value);
            }
        }

        mem_total_bytes()
    })
}

fn current_rss_bytes() -> Option<u64> {
    if let Ok(contents) = fs::read_to_string("/proc/self/statm") {
        if let Some(pages) = contents
            .split_whitespace()
            .nth(1)
            .and_then(|value| value.parse::<u64>().ok())
        {
            if let Some(bytes) = pages.checked_mul(page_size_bytes()) {
                return Some(bytes);
            }
        }
    }

    let contents = fs::read_to_string("/proc/self/status").ok()?;
    for line in contents.lines() {
        if let Some(rest) = line.strip_prefix("VmRSS:") {
            let kb = rest.split_whitespace().next()?.parse::<u64>().ok()?;
            return kb.checked_mul(1024);
        }
    }
    None
}

struct ResourceSnapshot {
    process_cpu_start: Option<ProcessTime>,
    throttled_ns: Option<u128>,
    rss_bytes: Option<u64>,
}

impl ResourceSnapshot {
    fn capture_start() -> Self {
        Self {
            process_cpu_start: Some(ProcessTime::now()),
            throttled_ns: current_cpu_throttled_ns(),
            rss_bytes: current_rss_bytes(),
        }
    }

    fn capture_end() -> Self {
        Self {
            process_cpu_start: None,
            throttled_ns: current_cpu_throttled_ns(),
            rss_bytes: current_rss_bytes(),
        }
    }
}

fn bounded_i64_delta(start: u64, end: u64) -> i64 {
    let delta = end as i128 - start as i128;
    delta.clamp(i64::MIN as i128, i64::MAX as i128) as i64
}

pub(crate) fn profiling_enabled() -> bool {
    writer().is_some()
}

#[derive(Clone, Copy, Debug, Default)]
struct StructuralCounterSnapshot {
    tree_hash_nodes_touched: u64,
    parent_hash_nodes_touched: u64,
    path_secret_derivation_count: u64,
    node_secret_derivation_count: u64,
    hpke_encrypt_count: u64,
    hpke_decrypt_count: u64,
}

impl StructuralCounterSnapshot {
    fn capture() -> Self {
        Self {
            tree_hash_nodes_touched: TREE_HASH_NODES_TOUCHED.load(Ordering::Relaxed),
            parent_hash_nodes_touched: PARENT_HASH_NODES_TOUCHED.load(Ordering::Relaxed),
            path_secret_derivation_count: PATH_SECRET_DERIVATION_COUNT.load(Ordering::Relaxed),
            node_secret_derivation_count: NODE_SECRET_DERIVATION_COUNT.load(Ordering::Relaxed),
            hpke_encrypt_count: HPKE_ENCRYPT_COUNT.load(Ordering::Relaxed),
            hpke_decrypt_count: HPKE_DECRYPT_COUNT.load(Ordering::Relaxed),
        }
    }

    fn delta_since(self, start: Self) -> Self {
        Self {
            tree_hash_nodes_touched: self
                .tree_hash_nodes_touched
                .saturating_sub(start.tree_hash_nodes_touched),
            parent_hash_nodes_touched: self
                .parent_hash_nodes_touched
                .saturating_sub(start.parent_hash_nodes_touched),
            path_secret_derivation_count: self
                .path_secret_derivation_count
                .saturating_sub(start.path_secret_derivation_count),
            node_secret_derivation_count: self
                .node_secret_derivation_count
                .saturating_sub(start.node_secret_derivation_count),
            hpke_encrypt_count: self
                .hpke_encrypt_count
                .saturating_sub(start.hpke_encrypt_count),
            hpke_decrypt_count: self
                .hpke_decrypt_count
                .saturating_sub(start.hpke_decrypt_count),
        }
    }
}

pub(crate) fn count_tree_hash_node_touch(count: u64) {
    TREE_HASH_NODES_TOUCHED.fetch_add(count, Ordering::Relaxed);
}

pub(crate) fn count_parent_hash_node_touch(count: u64) {
    PARENT_HASH_NODES_TOUCHED.fetch_add(count, Ordering::Relaxed);
}

pub(crate) fn count_path_secret_derivation(count: u64) {
    PATH_SECRET_DERIVATION_COUNT.fetch_add(count, Ordering::Relaxed);
}

pub(crate) fn count_node_secret_derivation(count: u64) {
    NODE_SECRET_DERIVATION_COUNT.fetch_add(count, Ordering::Relaxed);
}

pub(crate) fn count_hpke_encrypt(count: u64) {
    HPKE_ENCRYPT_COUNT.fetch_add(count, Ordering::Relaxed);
}

pub(crate) fn count_hpke_decrypt(count: u64) {
    HPKE_DECRYPT_COUNT.fetch_add(count, Ordering::Relaxed);
}

#[derive(Serialize, Debug)]
pub(crate) struct ProfileEvent {
    pub profile_schema_version: u32,
    pub ts_unix_ns: u128,
    pub op: String,
    pub measurement_class: String,
    pub measurement_plane: String,
    pub span_kind: String,
    pub span_name: String,
    pub span_id: u64,
    pub parent_span_id: Option<u64>,
    pub parent_operation: Option<String>,
    pub span_inclusive: bool,
    pub implementation: String,

    pub wall_ns: u128,
    pub cpu_thread_ns: Option<u128>,
    pub cpu_envelope_utilization: Option<f64>,
    pub cpu_throttled_time_ratio: Option<f64>,

    pub alloc_bytes: Option<u64>,
    pub alloc_count: Option<u64>,
    pub l1d_cache_accesses: Option<u64>,
    pub l1d_cache_misses: Option<u64>,
    pub ram_rss_delta_bytes: Option<i64>,
    pub ram_rss_utilization: Option<f64>,

    pub artifact_size_bytes: Option<usize>,
    pub welcome_bytes: Option<usize>,
    pub ratchet_tree_bytes: Option<usize>,
    pub welcome_plus_ratchet_tree_bytes: Option<usize>,
    pub group_info_bytes: Option<usize>,
    pub encrypted_group_info_bytes: Option<usize>,
    pub encrypted_secrets_count: Option<usize>,

    pub group_epoch: Option<u64>,
    pub tree_size: Option<u32>,
    pub tree_height: Option<u32>,
    pub tree_leaf_count: Option<u32>,
    pub tree_node_count: Option<u32>,
    pub member_count: Option<usize>,
    pub invitee_count: Option<isize>,
    pub added_members_count: Option<usize>,
    pub removed_members_count: Option<usize>,
    pub removed_leaf_indices: Option<Vec<u32>>,
    pub removed_right_edge_count: Option<usize>,
    pub rightmost_removed_leaf: Option<u32>,
    pub removed_right_edge_suffix_count: Option<usize>,
    pub right_edge_suffix_fully_removed: Option<bool>,
    pub tree_truncated: Option<bool>,
    pub truncated_levels_count: Option<usize>,
    pub tree_size_before: Option<u32>,
    pub tree_size_after: Option<u32>,
    pub tree_leaf_count_before: Option<u32>,
    pub tree_leaf_count_after: Option<u32>,
    pub tree_node_count_before: Option<u32>,
    pub tree_node_count_after: Option<u32>,
    pub ciphersuite: Option<String>,

    pub add_commit_mode: Option<String>,
    pub remove_commit_mode: Option<String>,
    pub commit_path_policy: Option<String>,
    pub force_self_update: Option<bool>,
    pub update_path_present: Option<bool>,

    pub committer_leaf_index: Option<u32>,
    pub joiner_leaf_index: Option<u32>,
    pub direct_path_len: Option<usize>,
    pub filtered_direct_path_len: Option<usize>,
    pub copath_len: Option<usize>,
    pub update_path_nodes_count: Option<usize>,
    pub encrypted_path_secret_count: Option<usize>,
    pub sum_copath_resolution_sizes: Option<usize>,
    pub max_copath_resolution_size: Option<usize>,
    pub path_secret_derivation_count: Option<u64>,
    pub node_secret_derivation_count: Option<u64>,
    pub hpke_encrypt_count: Option<u64>,
    pub hpke_decrypt_count: Option<u64>,
    pub tree_hash_nodes_touched: Option<u64>,
    pub parent_hash_nodes_touched: Option<u64>,
    pub commit_size_bytes: Option<usize>,
    pub commit_message_size_bytes: Option<usize>,
    pub commit_kind: Option<String>,
    pub commit_create_op: Option<String>,
    pub commit_semantics: Option<String>,
    /// Explicit add semantics: always "add_with_forced_update_path_and_welcome"
    /// for Add operations, because measured Add includes forced UpdatePath and Welcome work.
    pub add_semantics: Option<String>,
    pub commit_id: Option<String>,
    pub commit_has_path: Option<bool>,
    pub commit_is_external: Option<bool>,
    pub update_path_size_bytes: Option<usize>,
    pub welcome_recipient_count: Option<usize>,
    pub ratchet_tree_included: Option<bool>,
    pub ratchet_tree_delivery_mode: Option<String>,

    pub app_msg_plaintext_bytes: Option<usize>,
    pub app_msg_padding_bytes: Option<usize>,
    pub app_msg_ciphertext_bytes: Option<usize>,
    pub aad_bytes: Option<usize>,

    pub sender_leaf_index: Option<u32>,
    pub sender_generation: Option<u64>,
    pub first_message_in_epoch: Option<bool>,

    pub receiver_leaf_index: Option<u32>,
    pub receiver_member_index: Option<u32>,
    pub receiver_is_committer: Option<bool>,
    pub commit_receive_sampled: Option<bool>,
    pub commit_receive_sampling_policy: Option<String>,
    pub commit_receive_sampling_seed: Option<u64>,
    pub commit_receive_sample_index: Option<usize>,
    pub commit_receive_sample_count: Option<usize>,
    pub commit_receive_population_size: Option<usize>,
    pub selected_encrypted_path_secret_index: Option<usize>,
    pub path_secret_decryption_count: Option<u64>,
    pub confirmation_tag_verified: Option<bool>,
    pub proposal_count: Option<usize>,
    pub inline_proposal_count: Option<usize>,
    pub proposal_ref_count: Option<usize>,
    pub add_proposal_count: Option<usize>,
    pub update_proposal_count: Option<usize>,
    pub remove_proposal_count: Option<usize>,
    pub first_receive_from_sender: Option<bool>,
    pub generation_gap: Option<u64>,
    pub out_of_order_message: Option<bool>,

    pub aead_decrypt_count: Option<u64>,
    pub sender_data_decrypt_count: Option<u64>,
    pub signature_verify_count: Option<u64>,

    pub pid: u32,
    pub thread_id: String,
    pub worker_id: Option<String>,
    pub global_span_id: Option<String>,
    pub parent_global_span_id: Option<String>,

    pub run_id: Option<String>,
    pub scenario: Option<String>,
    pub scenario_seed: Option<u64>,
    pub node_name: Option<String>,
    pub pod_name: Option<String>,
    pub device_kind: Option<String>,
    pub execution_backend: Option<String>,

    pub benchmark_plateau_index: Option<usize>,
    pub benchmark_target_size: Option<usize>,
    pub benchmark_active_size: Option<usize>,
    pub benchmark_phase: Option<String>,
    pub benchmark_operation: Option<String>,
    pub benchmark_operation_seq: Option<usize>,
    pub benchmark_payload_size: Option<usize>,
    pub configured_payload_label: Option<String>,
}

pub(crate) fn emit_event(event: &ProfileEvent) {
    let Some(lock) = writer().as_ref() else {
        return;
    };

    let Ok(mut guard) = lock.lock() else {
        return;
    };

    if let Ok(line) = serde_json::to_string(event) {
        let _ = guard.write_all(line.as_bytes());
        let _ = guard.write_all(b"\n");
        let _ = guard.flush();
    }
}

pub(crate) struct ProfileScope {
    op: String,
    implementation: String,
    wall_start: Instant,
    cpu_start: Option<ThreadTime>,
    resource_start: ResourceSnapshot,
    structural_start: StructuralCounterSnapshot,
    l1d_cache_start: Option<L1DCacheCounterScope>,
    span_id: u64,
    parent_span_id: Option<u64>,
    parent_operation: Option<String>,
    finished: bool,
}

impl ProfileScope {
    pub(crate) fn start(op: impl Into<String>, implementation: impl Into<String>) -> Option<Self> {
        if !profiling_enabled() {
            return None;
        }
        let _ = L1DCacheCounterScope::counters_available();
        let span_id = next_span_id();
        let op_name: String = op.into();
        let parent_span_id = current_parent_span_id();
        let parent_operation = current_parent_operation();
        push_span_id(span_id, op_name.clone());

        Some(Self {
            op: op_name,
            implementation: implementation.into(),
            wall_start: Instant::now(),
            cpu_start: Some(ThreadTime::now()),
            resource_start: ResourceSnapshot::capture_start(),
            structural_start: StructuralCounterSnapshot::capture(),
            l1d_cache_start: L1DCacheCounterScope::start(),
            span_id,
            parent_span_id,
            parent_operation,
            finished: false,
        })
    }

    pub(crate) fn finish(mut self) -> ProfileEvent {
        let op = self.op.clone();
        let implementation = self.implementation.clone();
        let structural_counters =
            StructuralCounterSnapshot::capture().delta_since(self.structural_start);
        let l1d_cache_counts = self
            .l1d_cache_start
            .take()
            .map(L1DCacheCounterScope::finish)
            .unwrap_or_default();
        let wall_ns = self.wall_start.elapsed().as_nanos();
        let cpu_thread_ns = self
            .cpu_start
            .as_ref()
            .map(|start| start.elapsed().as_nanos());
        let resource_end = ResourceSnapshot::capture_end();
        let process_cpu_ns = self
            .resource_start
            .process_cpu_start
            .as_ref()
            .map(|start| start.elapsed().as_nanos());
        let effective_cpu_limit = effective_cpu_limit_cores().unwrap_or(1.0);
        let cpu_envelope_utilization = process_cpu_ns.and_then(|cpu_ns| {
            if wall_ns > 0 && effective_cpu_limit > 0.0 {
                Some(cpu_ns as f64 / (wall_ns as f64 * effective_cpu_limit))
            } else {
                None
            }
        });
        let cpu_throttled_time_ratio =
            match (self.resource_start.throttled_ns, resource_end.throttled_ns) {
                (Some(start), Some(end)) if wall_ns > 0 && end >= start => {
                    Some((end - start) as f64 / wall_ns as f64)
                }
                _ => None,
            };
        let ram_rss_delta_bytes = match (self.resource_start.rss_bytes, resource_end.rss_bytes) {
            (Some(start), Some(end)) => Some(bounded_i64_delta(start, end)),
            _ => None,
        };
        let ram_rss_utilization = match (
            self.resource_start.rss_bytes,
            resource_end.rss_bytes,
            effective_memory_limit_bytes(),
        ) {
            (Some(start), Some(end), Some(limit)) if limit > 0 => {
                Some(start.max(end) as f64 / limit as f64)
            }
            _ => None,
        };

        self.finished = true;
        pop_span_id(self.span_id);

        let mut event = ProfileEvent {
            profile_schema_version: 6,
            ts_unix_ns: unix_timestamp_ns(),
            measurement_class: measurement_class_for_op(&op).to_string(),
            measurement_plane: measurement_plane_for_op(&op).to_string(),
            span_kind: span_kind_for_op(&op).to_string(),
            span_name: op.clone(),
            span_id: self.span_id,
            parent_span_id: self.parent_span_id,
            parent_operation: self.parent_operation.clone(),
            span_inclusive: true,
            op,
            implementation,

            wall_ns,
            cpu_thread_ns,
            cpu_envelope_utilization,
            cpu_throttled_time_ratio,

            alloc_bytes: None,
            alloc_count: None,
            l1d_cache_accesses: l1d_cache_counts.accesses,
            l1d_cache_misses: l1d_cache_counts.misses,
            ram_rss_delta_bytes,
            ram_rss_utilization,

            artifact_size_bytes: None,
            welcome_bytes: None,
            ratchet_tree_bytes: None,
            welcome_plus_ratchet_tree_bytes: None,
            group_info_bytes: None,
            encrypted_group_info_bytes: None,
            encrypted_secrets_count: None,

            group_epoch: None,
            tree_size: None,
            tree_height: None,
            tree_leaf_count: None,
            tree_node_count: None,
            member_count: None,
            invitee_count: None,
            added_members_count: None,
            removed_members_count: None,
            removed_leaf_indices: None,
            removed_right_edge_count: None,
            rightmost_removed_leaf: None,
            removed_right_edge_suffix_count: None,
            right_edge_suffix_fully_removed: None,
            tree_truncated: None,
            truncated_levels_count: None,
            tree_size_before: None,
            tree_size_after: None,
            tree_leaf_count_before: None,
            tree_leaf_count_after: None,
            tree_node_count_before: None,
            tree_node_count_after: None,
            ciphersuite: None,

            add_commit_mode: None,
            remove_commit_mode: None,
            commit_path_policy: None,
            force_self_update: None,
            update_path_present: None,

            committer_leaf_index: None,
            joiner_leaf_index: None,
            direct_path_len: None,
            filtered_direct_path_len: None,
            copath_len: None,
            update_path_nodes_count: None,
            encrypted_path_secret_count: None,
            sum_copath_resolution_sizes: None,
            max_copath_resolution_size: None,
            path_secret_derivation_count: Some(structural_counters.path_secret_derivation_count),
            node_secret_derivation_count: Some(structural_counters.node_secret_derivation_count),
            hpke_encrypt_count: Some(structural_counters.hpke_encrypt_count),
            hpke_decrypt_count: Some(structural_counters.hpke_decrypt_count),
            tree_hash_nodes_touched: Some(structural_counters.tree_hash_nodes_touched),
            parent_hash_nodes_touched: Some(structural_counters.parent_hash_nodes_touched),
            commit_size_bytes: None,
            commit_message_size_bytes: None,
            commit_kind: None,
            commit_create_op: None,
            commit_semantics: None,
            add_semantics: None,
            commit_id: None,
            commit_has_path: None,
            commit_is_external: None,
            update_path_size_bytes: None,
            welcome_recipient_count: None,
            ratchet_tree_included: None,
            ratchet_tree_delivery_mode: None,

            app_msg_plaintext_bytes: None,
            app_msg_padding_bytes: None,
            app_msg_ciphertext_bytes: None,
            aad_bytes: None,

            sender_leaf_index: None,
            sender_generation: None,
            first_message_in_epoch: None,

            receiver_leaf_index: None,
            receiver_member_index: None,
            receiver_is_committer: None,
            commit_receive_sampled: None,
            commit_receive_sampling_policy: None,
            commit_receive_sampling_seed: None,
            commit_receive_sample_index: None,
            commit_receive_sample_count: None,
            commit_receive_population_size: None,
            selected_encrypted_path_secret_index: None,
            path_secret_decryption_count: None,
            confirmation_tag_verified: None,
            proposal_count: None,
            inline_proposal_count: None,
            proposal_ref_count: None,
            add_proposal_count: None,
            update_proposal_count: None,
            remove_proposal_count: None,
            first_receive_from_sender: None,
            generation_gap: None,
            out_of_order_message: None,

            aead_decrypt_count: None,
            sender_data_decrypt_count: None,
            signature_verify_count: None,

            pid: current_pid(),
            thread_id: current_thread_id(),
            worker_id: None,
            global_span_id: None,
            parent_global_span_id: None,

            run_id: env_or_none("OPENMLS_PROFILE_RUN_ID"),
            scenario: env_or_none("OPENMLS_PROFILE_SCENARIO"),
            scenario_seed: env_u64_or_none("OPENMLS_PROFILE_SCENARIO_SEED"),
            node_name: env_or_none("OPENMLS_PROFILE_NODE"),
            pod_name: env_or_none("OPENMLS_PROFILE_POD"),
            device_kind: env_or_none("OPENMLS_PROFILE_DEVICE_KIND"),
            execution_backend: env_or_none("OPENMLS_PROFILE_EXECUTION_BACKEND"),

            benchmark_plateau_index: None,
            benchmark_target_size: None,
            benchmark_active_size: None,
            benchmark_phase: None,
            benchmark_operation: None,
            benchmark_operation_seq: None,
            benchmark_payload_size: None,
            configured_payload_label: None,
        };

        COMMIT_RECEIVE_CONTEXT.with(|slot| {
            if let Some(ctx) = slot.borrow().as_ref() {
                event.commit_create_op = ctx.commit_create_op.clone();
                event.commit_receive_sampling_policy = ctx.commit_receive_sampling_policy.clone();
                event.commit_receive_sampling_seed = ctx.commit_receive_sampling_seed;
                event.commit_receive_sample_index = ctx.commit_receive_sample_index;
                event.commit_receive_sample_count = ctx.commit_receive_sample_count;
                event.commit_receive_population_size = ctx.commit_receive_population_size;
                event.commit_id = ctx.commit_id.clone();
                event.group_epoch = ctx.group_epoch;
                event.tree_size = ctx.tree_size;
                event.member_count = ctx.member_count;
                event.ciphersuite = ctx.ciphersuite.clone();
            }
        });

        BENCHMARK_CONTEXT.with(|slot| {
            if let Some(ctx) = slot.borrow().as_ref() {
                event.benchmark_plateau_index = ctx.benchmark_plateau_index;
                event.benchmark_target_size = ctx.benchmark_target_size;
                event.benchmark_active_size = ctx.benchmark_active_size;
                event.benchmark_phase = ctx.benchmark_phase.clone();
                event.benchmark_operation = ctx.benchmark_operation.clone();
                event.benchmark_operation_seq = ctx.benchmark_operation_seq;
                event.benchmark_payload_size = ctx.benchmark_payload_size;
                event.configured_payload_label = ctx.configured_payload_label.clone();
                if ctx.device_kind.is_some() {
                    event.device_kind = ctx.device_kind.clone();
                }
                if ctx.execution_backend.is_some() {
                    event.execution_backend = ctx.execution_backend.clone();
                }
                if ctx.ciphersuite.is_some() {
                    event.ciphersuite = ctx.ciphersuite.clone();
                }
            }
        });

        WORKER_ID.with(|slot| {
            if let Some(worker_id) = slot.borrow().as_ref() {
                event.worker_id = Some(worker_id.clone());
                event.global_span_id = Some(format!("{}:{}", worker_id, event.span_id));
                event.parent_global_span_id = event
                    .parent_span_id
                    .map(|pid| format!("{}:{}", worker_id, pid));
            }
        });

        if event.worker_id.is_none() {
            if let Some(wid) = env_or_none("OPENMLS_PROFILE_WORKER_ID") {
                event.worker_id = Some(wid.clone());
                event.global_span_id = Some(format!("{}:{}", wid, event.span_id));
                event.parent_global_span_id = event
                    .parent_span_id
                    .map(|pid| format!("{}:{}", wid, pid));
            }
        }

        event
    }
}

impl Drop for ProfileScope {
    fn drop(&mut self) {
        if !self.finished {
            pop_span_id(self.span_id);
        }
    }
}

pub(crate) fn finish_and_emit(scope: Option<ProfileScope>, fill: impl FnOnce(&mut ProfileEvent)) {
    let Some(scope) = scope else {
        return;
    };

    let mut event = scope.finish();
    fill(&mut event);
    emit_event(&event);
}
