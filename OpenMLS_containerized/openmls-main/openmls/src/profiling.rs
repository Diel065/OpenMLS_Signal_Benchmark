//ADDED THIS ENTIRE FILE FOR THE MASTERS THESIS PROJECT!!!
use std::{
    fs::{self, File, OpenOptions},
    io::{BufWriter, Write},
    path::{Path, PathBuf},
    sync::{Mutex, OnceLock},
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use cpu_time::{ProcessTime, ThreadTime};
use serde::Serialize;

static PROFILE_WRITER: OnceLock<Option<Mutex<BufWriter<File>>>> = OnceLock::new();
static CPU_STAT_PATH: OnceLock<Option<PathBuf>> = OnceLock::new();
static CPU_LIMIT_CORES: OnceLock<Option<f64>> = OnceLock::new();
static MEMORY_LIMIT_BYTES: OnceLock<Option<u64>> = OnceLock::new();
static PAGE_SIZE_BYTES: OnceLock<u64> = OnceLock::new();

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
    if op.ends_with("_protocol") || op.contains("_protocol_") {
        "protocol"
    } else if op.ends_with("_serialize") {
        "serialize"
    } else if op.contains("_deserialize") {
        "deserialize"
    } else {
        "other"
    }
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
    *PAGE_SIZE_BYTES.get_or_init(|| env_positive_u64_or_none("OPENMLS_PAGE_SIZE_BYTES").unwrap_or(4096))
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

#[derive(Serialize, Debug)]
pub(crate) struct ProfileEvent {
    pub profile_schema_version: u32,
    pub ts_unix_ns: u128,
    pub op: String,
    pub measurement_class: String,
    pub implementation: String,

    pub wall_ns: u128,
    pub cpu_thread_ns: Option<u128>,
    pub cpu_envelope_utilization: Option<f64>,
    pub cpu_throttled_time_ratio: Option<f64>,

    pub alloc_bytes: Option<u64>,
    pub alloc_count: Option<u64>,
    pub ram_rss_delta_bytes: Option<i64>,
    pub ram_rss_utilization: Option<f64>,

    pub artifact_size_bytes: Option<usize>,
    pub welcome_bytes: Option<usize>,
    pub ratchet_tree_bytes: Option<usize>,
    pub welcome_plus_ratchet_tree_bytes: Option<usize>,
    pub encrypted_group_info_bytes: Option<usize>,
    pub encrypted_secrets_count: Option<usize>,

    pub group_epoch: Option<u64>,
    pub tree_size: Option<u32>,
    pub member_count: Option<usize>,
    pub invitee_count: Option<isize>,
    pub ciphersuite: Option<String>,

    pub app_msg_plaintext_bytes: Option<usize>,
    pub app_msg_padding_bytes: Option<usize>,
    pub app_msg_ciphertext_bytes: Option<usize>,
    pub aad_bytes: Option<usize>,

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

pub(crate) struct ProfileScope {
    op: String,
    implementation: String,
    wall_start: Instant,
    cpu_start: Option<ThreadTime>,
    resource_start: ResourceSnapshot,
}

impl ProfileScope {
    pub(crate) fn start(
        op: impl Into<String>,
        implementation: impl Into<String>,
    ) -> Option<Self> {
        if !profiling_enabled() {
            return None;
        }

        Some(Self {
            op: op.into(),
            implementation: implementation.into(),
            wall_start: Instant::now(),
            cpu_start: Some(ThreadTime::now()),
            resource_start: ResourceSnapshot::capture_start(),
        })
    }

    pub(crate) fn finish(self) -> ProfileEvent {
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

        ProfileEvent {
            profile_schema_version: 3,
            ts_unix_ns: unix_timestamp_ns(),
            measurement_class: measurement_class_for_op(&self.op).to_string(),
            op: self.op,
            implementation: self.implementation,

            wall_ns,
            cpu_thread_ns,
            cpu_envelope_utilization,
            cpu_throttled_time_ratio,

            alloc_bytes: None,
            alloc_count: None,
            ram_rss_delta_bytes,
            ram_rss_utilization,

            artifact_size_bytes: None,
            welcome_bytes: None,
            ratchet_tree_bytes: None,
            welcome_plus_ratchet_tree_bytes: None,
            encrypted_group_info_bytes: None,
            encrypted_secrets_count: None,

            group_epoch: None,
            tree_size: None,
            member_count: None,
            invitee_count: None,
            ciphersuite: None,

            app_msg_plaintext_bytes: None,
            app_msg_padding_bytes: None,
            app_msg_ciphertext_bytes: None,
            aad_bytes: None,

            pid: current_pid(),
            thread_id: current_thread_id(),

            run_id: env_or_none("OPENMLS_PROFILE_RUN_ID"),
            scenario: env_or_none("OPENMLS_PROFILE_SCENARIO"),
            scenario_seed: env_u64_or_none("OPENMLS_PROFILE_SCENARIO_SEED"),
            node_name: env_or_none("OPENMLS_PROFILE_NODE"),
            pod_name: env_or_none("OPENMLS_PROFILE_POD"),
        }
    }
}
/*
pub(crate) fn finish_and_emit(
    scope: Option<ProfileScope>,
    fill: impl FnOnce(&mut ProfileEvent),
) {
    let Some(scope) = scope else {
        return;
    };

    let mut event = scope.finish();
    fill(&mut event);
    emit_event(&event);
}
*/
