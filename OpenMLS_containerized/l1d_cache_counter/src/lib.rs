use std::{
    cell::RefCell,
    ffi::c_void,
    mem,
    os::fd::RawFd,
    os::raw::{c_int, c_long, c_ulong},
    sync::{Arc, Mutex, OnceLock, Weak},
};

static L1D_CACHE_COUNTERS_AVAILABLE: OnceLock<bool> = OnceLock::new();
static L1D_PROFILING_ENABLED: OnceLock<bool> = OnceLock::new();

const L1D_PROFILING_ENV: &str = "OPENMLS_L1D_PROFILING_ENABLED";

thread_local! {
    static PROCESS_COUNTER_SESSION: RefCell<Weak<Mutex<ProcessPerfSession>>> =
        RefCell::new(Weak::new());
}

const PERF_TYPE_HW_CACHE: u32 = 3;
const PERF_COUNT_HW_CACHE_L1D: u64 = 0;
const PERF_COUNT_HW_CACHE_OP_READ: u64 = 0;
const PERF_COUNT_HW_CACHE_RESULT_ACCESS: u64 = 0;
const PERF_COUNT_HW_CACHE_RESULT_MISS: u64 = 1;
const PERF_FORMAT_TOTAL_TIME_ENABLED: u64 = 1 << 0;
const PERF_FORMAT_TOTAL_TIME_RUNNING: u64 = 1 << 1;
const PERF_FORMAT_GROUP: u64 = 1 << 3;
const PERF_FLAG_FD_CLOEXEC: u64 = 1 << 3;
const PERF_EVENT_IOC_ENABLE: c_ulong = 0x2400;
const PERF_EVENT_IOC_DISABLE: c_ulong = 0x2401;
const PERF_EVENT_IOC_RESET: c_ulong = 0x2403;
const PERF_IOC_FLAG_GROUP: c_ulong = 1;

#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
const SYS_PERF_EVENT_OPEN: c_long = 298;
#[cfg(all(target_os = "linux", target_arch = "aarch64"))]
const SYS_PERF_EVENT_OPEN: c_long = 241;
#[cfg(all(target_os = "linux", target_arch = "arm"))]
const SYS_PERF_EVENT_OPEN: c_long = 364;

#[repr(C)]
#[derive(Clone, Copy)]
struct PerfEventAttr {
    type_: u32,
    size: u32,
    config: u64,
    sample_period_or_freq: u64,
    sample_type: u64,
    read_format: u64,
    flags: u64,
    wakeup_events_or_watermark: u32,
    bp_type: u32,
    bp_addr_or_config1: u64,
    bp_len_or_config2: u64,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct L1DCacheCounts {
    pub accesses: Option<u64>,
    pub misses: Option<u64>,
    pub measured_thread_count: usize,
    pub discovered_thread_count: usize,
    pub multiplexed_thread_count: usize,
}

struct PerfGroup {
    leader_fd: RawFd,
    miss_fd: RawFd,
    tid: c_int,
}

#[derive(Clone, Copy)]
struct PerfGroupSnapshot {
    accesses: u64,
    misses: u64,
    time_enabled: u64,
    time_running: u64,
    runtime_ns: Option<u64>,
}

struct ProcessPerfSession {
    groups: Vec<PerfGroup>,
    discovered_thread_count: usize,
}

enum CounterBackend {
    Dedicated(Vec<PerfGroup>),
    SharedProcess {
        session: Arc<Mutex<ProcessPerfSession>>,
        group_indices: Vec<usize>,
    },
}

pub struct L1DCacheCounterScope {
    backend: CounterBackend,
    start_snapshots: Vec<PerfGroupSnapshot>,
    discovered_thread_count: usize,
}

#[cfg(any(
    all(target_os = "linux", target_arch = "x86_64"),
    all(target_os = "linux", target_arch = "aarch64"),
    all(target_os = "linux", target_arch = "arm")
))]
unsafe extern "C" {
    fn syscall(num: c_long, ...) -> c_long;
    fn ioctl(fd: c_int, request: c_ulong, ...) -> c_int;
    fn read(fd: c_int, buf: *mut c_void, count: usize) -> isize;
    fn close(fd: c_int) -> c_int;
}

#[cfg(not(any(
    all(target_os = "linux", target_arch = "x86_64"),
    all(target_os = "linux", target_arch = "aarch64"),
    all(target_os = "linux", target_arch = "arm")
)))]
unsafe fn ioctl(_fd: c_int, _request: c_ulong, _arg: c_ulong) -> c_int {
    -1
}

#[cfg(not(any(
    all(target_os = "linux", target_arch = "x86_64"),
    all(target_os = "linux", target_arch = "aarch64"),
    all(target_os = "linux", target_arch = "arm")
)))]
unsafe fn read(_fd: c_int, _buf: *mut c_void, _count: usize) -> isize {
    -1
}

#[cfg(not(any(
    all(target_os = "linux", target_arch = "x86_64"),
    all(target_os = "linux", target_arch = "aarch64"),
    all(target_os = "linux", target_arch = "arm")
)))]
unsafe fn close(_fd: c_int) -> c_int {
    -1
}

fn l1d_cache_config(result: u64) -> u64 {
    PERF_COUNT_HW_CACHE_L1D | (PERF_COUNT_HW_CACHE_OP_READ << 8) | (result << 16)
}

fn l1d_cache_attr(result: u64, disabled: bool) -> PerfEventAttr {
    let disabled_flag = u64::from(disabled);
    let exclude_kernel = 1 << 5;
    let exclude_hv = 1 << 6;

    PerfEventAttr {
        type_: PERF_TYPE_HW_CACHE,
        size: mem::size_of::<PerfEventAttr>() as u32,
        config: l1d_cache_config(result),
        sample_period_or_freq: 0,
        sample_type: 0,
        read_format: PERF_FORMAT_GROUP
            | PERF_FORMAT_TOTAL_TIME_ENABLED
            | PERF_FORMAT_TOTAL_TIME_RUNNING,
        flags: disabled_flag | exclude_kernel | exclude_hv,
        wakeup_events_or_watermark: 0,
        bp_type: 0,
        bp_addr_or_config1: 0,
        bp_len_or_config2: 0,
    }
}

#[cfg(any(
    all(target_os = "linux", target_arch = "x86_64"),
    all(target_os = "linux", target_arch = "aarch64"),
    all(target_os = "linux", target_arch = "arm")
))]
fn perf_event_open(
    attr: &mut PerfEventAttr,
    pid: c_int,
    cpu: c_int,
    group_fd: c_int,
    flags: u64,
) -> RawFd {
    unsafe {
        syscall(
            SYS_PERF_EVENT_OPEN,
            attr as *mut PerfEventAttr,
            pid,
            cpu,
            group_fd,
            flags,
        ) as RawFd
    }
}

#[cfg(not(any(
    all(target_os = "linux", target_arch = "x86_64"),
    all(target_os = "linux", target_arch = "aarch64"),
    all(target_os = "linux", target_arch = "arm")
)))]
fn perf_event_open(
    _attr: &mut PerfEventAttr,
    _pid: c_int,
    _cpu: c_int,
    _group_fd: c_int,
    _flags: u64,
) -> RawFd {
    -1
}

impl L1DCacheCounterScope {
    pub fn profiling_enabled() -> bool {
        *L1D_PROFILING_ENABLED.get_or_init(|| {
            std::env::var(L1D_PROFILING_ENV)
                .ok()
                .map(|value| {
                    matches!(
                        value.trim().to_ascii_lowercase().as_str(),
                        "1" | "true" | "yes" | "on"
                    )
                })
                .unwrap_or(false)
        })
    }

    pub fn counters_available() -> bool {
        if !Self::profiling_enabled() {
            return false;
        }
        *L1D_CACHE_COUNTERS_AVAILABLE.get_or_init(Self::probe_available)
    }

    pub fn start() -> Option<Self> {
        Self::start_current_thread()
    }

    pub fn start_current_thread() -> Option<Self> {
        if !Self::counters_available() {
            return None;
        }
        if let Some(scope) = Self::start_current_thread_from_process_session() {
            return Some(scope);
        }
        let mut group = Self::open_group(0)?;
        if !group.reset_and_enable() {
            return None;
        }
        let start_snapshots = vec![group.snapshot()?];
        Some(Self {
            backend: CounterBackend::Dedicated(vec![group]),
            start_snapshots,
            discovered_thread_count: 1,
        })
    }

    fn start_current_thread_from_process_session() -> Option<Self> {
        let session = PROCESS_COUNTER_SESSION.with(|slot| slot.borrow().upgrade())?;
        let current_tid = current_thread_id()?;
        let guard = session.lock().ok()?;
        let group_index = guard.groups.iter().position(|group| group.tid == current_tid)?;
        let start_snapshots = vec![guard.groups[group_index].snapshot()?];
        drop(guard);
        Some(Self {
            backend: CounterBackend::SharedProcess {
                session,
                group_indices: vec![group_index],
            },
            start_snapshots,
            discovered_thread_count: 1,
        })
    }

    pub fn start_process_threads() -> Option<Self> {
        if !Self::counters_available() {
            return None;
        }

        let session = PROCESS_COUNTER_SESSION.with(|slot| {
            if let Some(session) = slot.borrow().upgrade() {
                return Some(session);
            }

            let thread_ids = process_thread_ids();
            let discovered_thread_count = thread_ids.len();
            let mut groups = Vec::new();
            for tid in thread_ids {
                if let Some(mut group) = Self::open_group(tid) {
                    if group.reset_and_enable() {
                        groups.push(group);
                    }
                }
            }
            if groups.is_empty() {
                return None;
            }

            let session = Arc::new(Mutex::new(ProcessPerfSession {
                groups,
                discovered_thread_count,
            }));
            *slot.borrow_mut() = Arc::downgrade(&session);
            Some(session)
        })?;

        let guard = session.lock().ok()?;
        let start_snapshots = guard.snapshots()?;
        let discovered_thread_count = guard.discovered_thread_count;
        let group_indices = (0..guard.groups.len()).collect();
        drop(guard);
        Some(Self {
            backend: CounterBackend::SharedProcess {
                session,
                group_indices,
            },
            start_snapshots,
            discovered_thread_count,
        })
    }

    fn probe_available() -> bool {
        let Some(mut group) = Self::open_group(0) else {
            return false;
        };
        if !group.reset_and_enable() {
            return false;
        }
        let start = group.snapshot();
        for _ in 0..10_000 {
            std::hint::black_box(1usize.wrapping_add(1));
        }
        let end = group.snapshot();
        group.disable();
        let counts = start
            .zip(end)
            .and_then(|(start, end)| aggregate_snapshot_deltas(&[start], &[end], 1))
            .unwrap_or_default();
        counts.accesses.is_some() && counts.misses.is_some()
    }

    fn open_group(tid: c_int) -> Option<PerfGroup> {
        let mut access_attr = l1d_cache_attr(PERF_COUNT_HW_CACHE_RESULT_ACCESS, true);
        let leader_fd = perf_event_open(&mut access_attr, tid, -1, -1, PERF_FLAG_FD_CLOEXEC);
        if leader_fd < 0 {
            return None;
        }

        let mut miss_attr = l1d_cache_attr(PERF_COUNT_HW_CACHE_RESULT_MISS, false);
        let miss_fd = perf_event_open(&mut miss_attr, tid, -1, leader_fd, PERF_FLAG_FD_CLOEXEC);
        if miss_fd < 0 {
            unsafe {
                let _ = close(leader_fd);
            }
            return None;
        }

        Some(PerfGroup {
            leader_fd,
            miss_fd,
            tid,
        })
    }

    pub fn finish(self) -> L1DCacheCounts {
        let discovered_thread_count = self.discovered_thread_count;
        let (end_snapshots, measured_thread_count) = match self.backend {
            CounterBackend::Dedicated(groups) => {
                let snapshots = groups
                    .iter()
                    .map(PerfGroup::snapshot)
                    .collect::<Option<Vec<_>>>();
                for group in &groups {
                    group.disable();
                }
                let measured = groups.len();
                let Some(snapshots) = snapshots else {
                    return L1DCacheCounts {
                        measured_thread_count: measured,
                        discovered_thread_count,
                        ..L1DCacheCounts::default()
                    };
                };
                (snapshots, measured)
            }
            CounterBackend::SharedProcess {
                session,
                group_indices,
            } => {
                let Ok(guard) = session.lock() else {
                    return L1DCacheCounts {
                        discovered_thread_count,
                        ..L1DCacheCounts::default()
                    };
                };
                let measured = group_indices.len();
                let Some(snapshots) = guard.snapshots_for(&group_indices) else {
                    return L1DCacheCounts {
                        measured_thread_count: measured,
                        discovered_thread_count,
                        ..L1DCacheCounts::default()
                    };
                };
                (snapshots, measured)
            }
        };

        aggregate_snapshot_deltas(
            &self.start_snapshots,
            &end_snapshots,
            discovered_thread_count,
        )
        .unwrap_or(L1DCacheCounts {
            measured_thread_count,
            discovered_thread_count,
            ..L1DCacheCounts::default()
        })
    }
}

impl ProcessPerfSession {
    fn snapshots(&self) -> Option<Vec<PerfGroupSnapshot>> {
        self.groups.iter().map(PerfGroup::snapshot).collect()
    }

    fn snapshots_for(&self, group_indices: &[usize]) -> Option<Vec<PerfGroupSnapshot>> {
        group_indices
            .iter()
            .map(|index| self.groups.get(*index)?.snapshot())
            .collect()
    }
}

fn aggregate_snapshot_deltas(
    starts: &[PerfGroupSnapshot],
    ends: &[PerfGroupSnapshot],
    discovered_thread_count: usize,
) -> Option<L1DCacheCounts> {
    if starts.len() != ends.len() {
        return None;
    }

    let mut accesses = 0u64;
    let mut misses = 0u64;
    let mut multiplexed_thread_count = 0usize;
    for (start, end) in starts.iter().zip(ends) {
        let raw_accesses = end.accesses.saturating_sub(start.accesses);
        let raw_misses = end.misses.saturating_sub(start.misses);
        let time_enabled = end.time_enabled.saturating_sub(start.time_enabled);
        let time_running = end.time_running.saturating_sub(start.time_running);
        let runtime_delta_ns = start
            .runtime_ns
            .zip(end.runtime_ns)
            .map(|(start, end)| end.saturating_sub(start));

        let (group_accesses, group_misses) = if time_running > 0 {
            multiplexed_thread_count += usize::from(time_running < time_enabled);
            (
                scale_multiplexed_count(raw_accesses, time_enabled, time_running)?,
                scale_multiplexed_count(raw_misses, time_enabled, time_running)?,
            )
        } else if raw_accesses == 0 && raw_misses == 0 && runtime_delta_ns == Some(0) {
            (0, 0)
        } else {
            return None;
        };

        accesses = accesses.saturating_add(group_accesses);
        misses = misses.saturating_add(group_misses);
    }

    Some(L1DCacheCounts {
        accesses: Some(accesses),
        misses: Some(misses),
        measured_thread_count: starts.len(),
        discovered_thread_count,
        multiplexed_thread_count,
    })
}

#[cfg(target_os = "linux")]
fn process_thread_ids() -> Vec<c_int> {
    let mut tids = std::fs::read_dir("/proc/self/task")
        .into_iter()
        .flatten()
        .filter_map(Result::ok)
        .filter_map(|entry| entry.file_name().to_string_lossy().parse::<c_int>().ok())
        .collect::<Vec<_>>();
    tids.sort_unstable();
    tids.dedup();
    tids
}

#[cfg(target_os = "linux")]
fn thread_runtime_ns(tid: c_int) -> Option<u64> {
    let path = if tid == 0 {
        "/proc/thread-self/schedstat".to_string()
    } else {
        format!("/proc/self/task/{tid}/schedstat")
    };
    std::fs::read_to_string(path)
        .ok()?
        .split_whitespace()
        .next()?
        .parse()
        .ok()
}

#[cfg(target_os = "linux")]
fn current_thread_id() -> Option<c_int> {
    std::fs::read_link("/proc/thread-self")
        .ok()?
        .file_name()?
        .to_string_lossy()
        .parse()
        .ok()
}

#[cfg(not(target_os = "linux"))]
fn current_thread_id() -> Option<c_int> {
    None
}

#[cfg(not(target_os = "linux"))]
fn thread_runtime_ns(_tid: c_int) -> Option<u64> {
    None
}

#[cfg(not(target_os = "linux"))]
fn process_thread_ids() -> Vec<c_int> {
    vec![0]
}

impl PerfGroup {
    fn reset_and_enable(&mut self) -> bool {
        let enabled = unsafe {
            ioctl(self.leader_fd, PERF_EVENT_IOC_RESET, PERF_IOC_FLAG_GROUP) == 0
                && ioctl(self.leader_fd, PERF_EVENT_IOC_ENABLE, PERF_IOC_FLAG_GROUP) == 0
        };
        enabled
    }

    fn disable(&self) {
        unsafe {
            let _ = ioctl(self.leader_fd, PERF_EVENT_IOC_DISABLE, PERF_IOC_FLAG_GROUP);
        }
    }

    fn snapshot(&self) -> Option<PerfGroupSnapshot> {
        // PERF_FORMAT_GROUP with TOTAL_TIME_ENABLED and TOTAL_TIME_RUNNING yields
        // nr, time_enabled, time_running, followed by one value per event.
        let mut values = [0u64; 5];
        let expected_bytes = mem::size_of_val(&values) as isize;
        let read_bytes = unsafe {
            read(
                self.leader_fd,
                values.as_mut_ptr().cast::<c_void>(),
                mem::size_of_val(&values),
            )
        };

        (read_bytes == expected_bytes && values[0] >= 2).then_some(PerfGroupSnapshot {
            accesses: values[3],
            misses: values[4],
            time_enabled: values[1],
            time_running: values[2],
            runtime_ns: thread_runtime_ns(self.tid),
        })
    }
}

fn scale_multiplexed_count(raw: u64, time_enabled: u64, time_running: u64) -> Option<u64> {
    if time_running == 0 {
        return None;
    }
    if time_running >= time_enabled {
        return Some(raw);
    }
    let scaled = (raw as u128)
        .saturating_mul(time_enabled as u128)
        .saturating_add((time_running / 2) as u128)
        / time_running as u128;
    Some(scaled.min(u64::MAX as u128) as u64)
}

impl Drop for PerfGroup {
    fn drop(&mut self) {
        unsafe {
            let _ = close(self.miss_fd);
            let _ = close(self.leader_fd);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn multiplexed_counts_are_scaled_and_rounded() {
        assert_eq!(scale_multiplexed_count(100, 200, 100), Some(200));
        assert_eq!(scale_multiplexed_count(5, 3, 2), Some(8));
        assert_eq!(scale_multiplexed_count(7, 10, 10), Some(7));
        assert_eq!(scale_multiplexed_count(7, 10, 0), None);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn process_thread_inventory_contains_the_current_process() {
        assert!(!process_thread_ids().is_empty());
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn current_thread_runtime_is_observable() {
        assert!(thread_runtime_ns(0).is_some());
        assert!(current_thread_id().is_some());
    }

    #[test]
    fn snapshot_deltas_scale_nested_scope_counts() {
        let start = PerfGroupSnapshot {
            accesses: 100,
            misses: 10,
            time_enabled: 1_000,
            time_running: 500,
            runtime_ns: Some(100),
        };
        let end = PerfGroupSnapshot {
            accesses: 150,
            misses: 15,
            time_enabled: 1_200,
            time_running: 600,
            runtime_ns: Some(200),
        };
        let counts = aggregate_snapshot_deltas(&[start], &[end], 1).unwrap();
        assert_eq!(counts.accesses, Some(100));
        assert_eq!(counts.misses, Some(10));
        assert_eq!(counts.multiplexed_thread_count, 1);
    }

    #[test]
    fn idle_zero_running_thread_contributes_zero() {
        let snapshot = PerfGroupSnapshot {
            accesses: 0,
            misses: 0,
            time_enabled: 100,
            time_running: 0,
            runtime_ns: Some(50),
        };
        let counts = aggregate_snapshot_deltas(&[snapshot], &[snapshot], 1).unwrap();
        assert_eq!(counts.accesses, Some(0));
        assert_eq!(counts.misses, Some(0));
    }

    #[test]
    fn active_zero_running_thread_is_rejected() {
        let start = PerfGroupSnapshot {
            accesses: 0,
            misses: 0,
            time_enabled: 100,
            time_running: 0,
            runtime_ns: Some(50),
        };
        let end = PerfGroupSnapshot {
            time_enabled: 200,
            runtime_ns: Some(51),
            ..start
        };
        assert!(aggregate_snapshot_deltas(&[start], &[end], 1).is_none());
    }
}
