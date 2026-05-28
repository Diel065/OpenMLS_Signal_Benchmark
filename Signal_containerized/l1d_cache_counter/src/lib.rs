use std::{
    ffi::c_void,
    mem,
    os::fd::RawFd,
    os::raw::{c_int, c_long, c_ulong},
    sync::OnceLock,
};

static L1D_CACHE_COUNTERS_AVAILABLE: OnceLock<bool> = OnceLock::new();

const PERF_TYPE_HW_CACHE: u32 = 3;
const PERF_COUNT_HW_CACHE_L1D: u64 = 0;
const PERF_COUNT_HW_CACHE_OP_READ: u64 = 0;
const PERF_COUNT_HW_CACHE_RESULT_ACCESS: u64 = 0;
const PERF_COUNT_HW_CACHE_RESULT_MISS: u64 = 1;
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
}

pub struct L1DCacheCounterScope {
    leader_fd: RawFd,
    miss_fd: RawFd,
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
        read_format: PERF_FORMAT_GROUP,
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
    pub fn counters_available() -> bool {
        *L1D_CACHE_COUNTERS_AVAILABLE.get_or_init(Self::probe_available)
    }

    pub fn start() -> Option<Self> {
        if !Self::counters_available() {
            return None;
        }

        let scope = Self::open_group()?;
        if scope.reset_and_enable() {
            Some(scope)
        } else {
            None
        }
    }

    fn probe_available() -> bool {
        let Some(scope) = Self::open_group() else {
            return false;
        };
        if !scope.reset_and_enable() {
            return false;
        }
        let counts = scope.finish();
        counts.accesses.is_some() && counts.misses.is_some()
    }

    fn open_group() -> Option<Self> {
        let mut access_attr = l1d_cache_attr(PERF_COUNT_HW_CACHE_RESULT_ACCESS, true);
        let leader_fd = perf_event_open(&mut access_attr, 0, -1, -1, PERF_FLAG_FD_CLOEXEC);
        if leader_fd < 0 {
            return None;
        }

        let mut miss_attr = l1d_cache_attr(PERF_COUNT_HW_CACHE_RESULT_MISS, false);
        let miss_fd = perf_event_open(&mut miss_attr, 0, -1, leader_fd, PERF_FLAG_FD_CLOEXEC);
        if miss_fd < 0 {
            unsafe {
                let _ = close(leader_fd);
            }
            return None;
        }

        Some(Self { leader_fd, miss_fd })
    }

    fn reset_and_enable(&self) -> bool {
        unsafe {
            ioctl(self.leader_fd, PERF_EVENT_IOC_RESET, PERF_IOC_FLAG_GROUP) == 0
                && ioctl(self.leader_fd, PERF_EVENT_IOC_ENABLE, PERF_IOC_FLAG_GROUP) == 0
        }
    }

    pub fn finish(self) -> L1DCacheCounts {
        unsafe {
            let _ = ioctl(self.leader_fd, PERF_EVENT_IOC_DISABLE, PERF_IOC_FLAG_GROUP);
        }

        let mut values = [0u64; 3];
        let expected_bytes = mem::size_of_val(&values) as isize;
        let read_bytes = unsafe {
            read(
                self.leader_fd,
                values.as_mut_ptr().cast::<c_void>(),
                mem::size_of_val(&values),
            )
        };

        if read_bytes == expected_bytes && values[0] >= 2 {
            L1DCacheCounts {
                accesses: Some(values[1]),
                misses: Some(values[2]),
            }
        } else {
            L1DCacheCounts::default()
        }
    }
}

impl Drop for L1DCacheCounterScope {
    fn drop(&mut self) {
        unsafe {
            let _ = close(self.miss_fd);
            let _ = close(self.leader_fd);
        }
    }
}
