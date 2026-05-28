use std::{
    cell::RefCell,
    fmt::Display,
    fs::{self, File, OpenOptions},
    future::{poll_fn, Future},
    io::{BufWriter, Write},
    path::{Path, PathBuf},
    pin::Pin,
    sync::{Mutex, OnceLock},
    task::Poll,
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use allocation_counter::AllocationInfo;
use cpu_time::{ProcessTime, ThreadTime};
use l1d_cache_counter::L1DCacheCounterScope;
use serde::Serialize;

static PROFILE_WRITER: OnceLock<Option<Mutex<BufWriter<File>>>> = OnceLock::new();
static CPU_STAT_PATH: OnceLock<Option<PathBuf>> = OnceLock::new();
static CPU_LIMIT_CORES: OnceLock<Option<f64>> = OnceLock::new();
static MEMORY_LIMIT_BYTES: OnceLock<Option<u64>> = OnceLock::new();
static PAGE_SIZE_BYTES: OnceLock<u64> = OnceLock::new();

thread_local! {
    static PROFILE_CONTEXT: RefCell<ProfileContext> = RefCell::new(ProfileContext::default());
}

#[derive(Clone, Debug, Default)]
pub struct ProfileContext {
    pub participant_id: Option<String>,
    pub participant_device_id: Option<u32>,
    pub peer_id: Option<String>,
    pub peer_device_id: Option<u32>,
    pub pair_id: Option<String>,
    pub role: Option<String>,
    pub direction: Option<String>,
    pub phase: Option<String>,
    pub conversation_size: Option<usize>,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct SpanMetadata {
    pub artifact_size_bytes: Option<usize>,
    pub plaintext_bytes: Option<usize>,
    pub ciphertext_bytes: Option<usize>,
    pub handshake_protocol: Option<&'static str>,
    pub handshake_side: Option<&'static str>,
    pub classical_one_time_prekey_present: Option<bool>,
    pub classical_one_time_prekey_id: Option<u32>,
    pub signed_prekey_id: Option<u32>,
    pub pq_prekey_id: Option<u32>,
    pub pq_prekey_type: Option<&'static str>,
    pub pq_prekey_signature_present: Option<bool>,
    pub ciphertext_message_type: Option<&'static str>,
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
    pub ratchet_progression_kind: Option<&'static str>,
    pub ratchet_progression_value: Option<u64>,
}

pub fn with_profile_context<R>(context: ProfileContext, run: impl FnOnce() -> R) -> R {
    PROFILE_CONTEXT.with(|cell| {
        let previous = cell.replace(context);
        let result = run();
        let _ = cell.replace(previous);
        result
    })
}

fn current_context() -> ProfileContext {
    PROFILE_CONTEXT.with(|cell| cell.borrow().clone())
}

fn profile_path() -> Option<PathBuf> {
    std::env::var_os("SIGNAL_PROFILE_PATH").map(PathBuf::from)
}

fn writer() -> &'static Option<Mutex<BufWriter<File>>> {
    PROFILE_WRITER.get_or_init(|| {
        let path = match profile_path() {
            Some(path) => path,
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

pub(crate) fn profiling_enabled() -> bool {
    writer().is_some()
}

fn unix_timestamp_ns() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0)
}

fn current_thread_id() -> String {
    format!("{:?}", std::thread::current().id())
}

fn env_or_none(key: &str) -> Option<String> {
    std::env::var(key).ok().filter(|value| !value.is_empty())
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
                candidates.push(PathBuf::from("/sys/fs/cgroup").join(rel_path).join(file_name));
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
                candidates.push(PathBuf::from("/sys/fs/cgroup").join(rel_path).join(file_name));
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
        if let Some(value) = env_positive_f64_or_none("SIGNAL_EFFECTIVE_CPU_LIMIT_CORES") {
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
        .get_or_init(|| env_positive_u64_or_none("SIGNAL_PAGE_SIZE_BYTES").unwrap_or(4096))
}

fn effective_memory_limit_bytes() -> Option<u64> {
    *MEMORY_LIMIT_BYTES.get_or_init(|| {
        if let Some(value) = env_positive_u64_or_none("SIGNAL_EFFECTIVE_MEMORY_LIMIT_BYTES") {
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

fn measurement_class_for_op(op: &str) -> &'static str {
    if op.ends_with("_protocol")
        || op.contains("_ratchet_")
        || op.contains("_aead_")
        || op.contains("_spqr_")
    {
        "protocol"
    } else {
        "protocol_helper"
    }
}

fn event_family_for_op(op: &str) -> &'static str {
    if op.starts_with("pqxdh_") {
        "pqxdh_handshake"
    } else if op.contains("_ratchet_") || op.contains("_spqr_") {
        "double_ratchet"
    } else if op.contains("_aead_") {
        "message_aead"
    } else if op.contains("_encrypt_") {
        "message_encrypt"
    } else if op.contains("_decrypt_") {
        "message_decrypt"
    } else {
        "signal_protocol"
    }
}

#[derive(Serialize, Debug)]
pub(crate) struct ProfileEvent {
    pub profile_schema_version: u32,
    pub ts_unix_ns: u128,
    pub op: String,
    pub span_layer: String,
    pub protocol_stack: String,
    pub implementation: String,
    pub measurement_class: String,
    pub event_family: String,
    pub event_subtype: String,
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
    pub success: bool,
    pub error_class: Option<String>,
    pub participant_id: Option<String>,
    pub participant_device_id: Option<u32>,
    pub peer_id: Option<String>,
    pub peer_device_id: Option<u32>,
    pub pair_id: Option<String>,
    pub role: Option<String>,
    pub direction: Option<String>,
    pub phase: Option<String>,
    pub conversation_size: Option<usize>,
    pub artifact_size_bytes: Option<usize>,
    pub plaintext_bytes: Option<usize>,
    pub ciphertext_bytes: Option<usize>,
    pub handshake_protocol: Option<&'static str>,
    pub handshake_side: Option<&'static str>,
    pub classical_one_time_prekey_present: Option<bool>,
    pub classical_one_time_prekey_id: Option<u32>,
    pub signed_prekey_id: Option<u32>,
    pub pq_prekey_id: Option<u32>,
    pub pq_prekey_type: Option<&'static str>,
    pub pq_prekey_signature_present: Option<bool>,
    pub ciphertext_message_type: Option<&'static str>,
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
    pub ratchet_progression_kind: Option<&'static str>,
    pub ratchet_progression_value: Option<u64>,
    pub pid: u32,
    pub thread_id: String,
    pub run_id: Option<String>,
    pub scenario: Option<String>,
    pub scenario_seed: Option<u64>,
    pub node_name: Option<String>,
    pub pod_name: Option<String>,
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

struct ProfileScope {
    op: String,
    metadata: SpanMetadata,
    context: ProfileContext,
    wall_start: Instant,
    cpu_start: Option<ThreadTime>,
    resource_start: ResourceSnapshot,
    l1d_cache_start: Option<L1DCacheCounterScope>,
}

impl ProfileScope {
    fn start(op: impl Into<String>, metadata: SpanMetadata) -> Option<Self> {
        if !profiling_enabled() {
            return None;
        }
        let _ = L1DCacheCounterScope::counters_available();
        Some(Self {
            op: op.into(),
            metadata,
            context: current_context(),
            wall_start: Instant::now(),
            cpu_start: Some(ThreadTime::now()),
            resource_start: ResourceSnapshot::capture_start(),
            l1d_cache_start: L1DCacheCounterScope::start(),
        })
    }

    fn finish(
        self,
        allocation_info: AllocationInfo,
        success: bool,
        error_class: Option<String>,
    ) -> ProfileEvent {
        let l1d_cache_counts = self
            .l1d_cache_start
            .map(L1DCacheCounterScope::finish)
            .unwrap_or_default();
        let wall_ns = self.wall_start.elapsed().as_nanos();
        let cpu_thread_ns = self.cpu_start.map(|start| start.elapsed().as_nanos());
        let resource_end = ResourceSnapshot::capture_end();
        let process_cpu_ns = self
            .resource_start
            .process_cpu_start
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
        let metadata = self.metadata;
        let context = self.context;

        ProfileEvent {
            profile_schema_version: 3,
            ts_unix_ns: unix_timestamp_ns(),
            span_layer: "libsignal_main".to_string(),
            protocol_stack: "signal".to_string(),
            implementation: "libsignal".to_string(),
            measurement_class: measurement_class_for_op(&self.op).to_string(),
            event_family: event_family_for_op(&self.op).to_string(),
            event_subtype: self.op.clone(),
            op: self.op,
            wall_ns,
            cpu_thread_ns,
            cpu_envelope_utilization,
            cpu_throttled_time_ratio,
            alloc_bytes: Some(allocation_info.bytes_total),
            alloc_count: Some(allocation_info.count_total),
            l1d_cache_accesses: l1d_cache_counts.accesses,
            l1d_cache_misses: l1d_cache_counts.misses,
            ram_rss_delta_bytes,
            ram_rss_utilization,
            success,
            error_class,
            participant_id: context.participant_id,
            participant_device_id: context.participant_device_id,
            peer_id: context.peer_id,
            peer_device_id: context.peer_device_id,
            pair_id: context.pair_id,
            role: context.role,
            direction: context.direction,
            phase: context.phase,
            conversation_size: context.conversation_size,
            artifact_size_bytes: metadata.artifact_size_bytes,
            plaintext_bytes: metadata.plaintext_bytes,
            ciphertext_bytes: metadata.ciphertext_bytes,
            handshake_protocol: metadata.handshake_protocol,
            handshake_side: metadata.handshake_side,
            classical_one_time_prekey_present: metadata.classical_one_time_prekey_present,
            classical_one_time_prekey_id: metadata.classical_one_time_prekey_id,
            signed_prekey_id: metadata.signed_prekey_id,
            pq_prekey_id: metadata.pq_prekey_id,
            pq_prekey_type: metadata.pq_prekey_type,
            pq_prekey_signature_present: metadata.pq_prekey_signature_present,
            ciphertext_message_type: metadata.ciphertext_message_type,
            message_counter: metadata.message_counter,
            previous_counter: metadata.previous_counter,
            sender_ratchet_key_fingerprint: metadata.sender_ratchet_key_fingerprint,
            receiver_chain_matched: metadata.receiver_chain_matched,
            dh_ratchet_performed: metadata.dh_ratchet_performed,
            root_chain_updated: metadata.root_chain_updated,
            send_chain_index_before: metadata.send_chain_index_before,
            send_chain_index_after: metadata.send_chain_index_after,
            receive_chain_index_before: metadata.receive_chain_index_before,
            receive_chain_index_after: metadata.receive_chain_index_after,
            skipped_message_keys_used: metadata.skipped_message_keys_used,
            skipped_message_keys_stored: metadata.skipped_message_keys_stored,
            spqr_step_performed: metadata.spqr_step_performed,
            ratchet_progression_kind: metadata.ratchet_progression_kind,
            ratchet_progression_value: metadata.ratchet_progression_value,
            pid: std::process::id(),
            thread_id: current_thread_id(),
            run_id: env_or_none("SIGNAL_PROFILE_RUN_ID"),
            scenario: env_or_none("SIGNAL_PROFILE_SCENARIO"),
            scenario_seed: env_u64_or_none("SIGNAL_PROFILE_SCENARIO_SEED"),
            node_name: env_or_none("SIGNAL_PROFILE_NODE"),
            pod_name: env_or_none("HOSTNAME"),
        }
    }
}

pub(crate) fn measure_result<T, E>(
    op: &'static str,
    run: impl FnOnce() -> std::result::Result<T, E>,
) -> std::result::Result<T, E>
where
    E: Display,
{
    measure_result_with(op, SpanMetadata::default(), run)
}

pub fn measure_update_opks_result<T, E>(
    run: impl FnOnce() -> std::result::Result<T, E>,
) -> std::result::Result<T, E>
where
    E: Display,
{
    measure_result("signal_update_opks_generate_protocol", run)
}

pub(crate) fn measure_result_with<T, E>(
    op: &'static str,
    metadata: SpanMetadata,
    run: impl FnOnce() -> std::result::Result<T, E>,
) -> std::result::Result<T, E>
where
    E: Display,
{
    let Some(scope) = ProfileScope::start(op, metadata) else {
        return run();
    };

    let mut result = None;
    let allocation_info = allocation_counter::measure(|| {
        result = Some(run());
    });
    let result = result.expect("allocation_counter measure closure did not run");
    let (success, error_class) = match &result {
        Ok(_) => (true, None),
        Err(err) => (false, Some(err.to_string())),
    };
    emit_event(&scope.finish(allocation_info, success, error_class));
    result
}

pub(crate) async fn measure_async_result<T, E, F>(
    op: &'static str,
    future: F,
) -> std::result::Result<T, E>
where
    E: Display,
    F: Future<Output = std::result::Result<T, E>>,
{
    measure_async_result_with(op, SpanMetadata::default(), future).await
}

pub(crate) async fn measure_async_result_with<T, E, F>(
    op: &'static str,
    metadata: SpanMetadata,
    future: F,
) -> std::result::Result<T, E>
where
    E: Display,
    F: Future<Output = std::result::Result<T, E>>,
{
    let Some(scope) = ProfileScope::start(op, metadata) else {
        return future.await;
    };

    let mut future = Box::pin(future);
    let mut allocation_info = AllocationInfo::default();
    let result = poll_fn(|cx| {
        let mut poll_result = None;
        let poll_allocation_info = allocation_counter::measure(|| {
            poll_result = Some(Pin::as_mut(&mut future).poll(cx));
        });
        allocation_info += poll_allocation_info;
        match poll_result.expect("allocation_counter measure closure did not poll") {
            Poll::Ready(result) => Poll::Ready(result),
            Poll::Pending => Poll::Pending,
        }
    })
    .await;

    let (success, error_class) = match &result {
        Ok(_) => (true, None),
        Err(err) => (false, Some(err.to_string())),
    };
    emit_event(&scope.finish(allocation_info, success, error_class));
    result
}
