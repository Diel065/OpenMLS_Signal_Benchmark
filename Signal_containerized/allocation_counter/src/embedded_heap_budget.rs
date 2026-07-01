use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

static BUDGET_BYTES: AtomicU64 = AtomicU64::new(0);
static BASELINE_LIVE_BYTES: AtomicU64 = AtomicU64::new(0);
static CURRENT_LIVE_BYTES: AtomicU64 = AtomicU64::new(0);
static PEAK_LIVE_BYTES: AtomicU64 = AtomicU64::new(0);
static OPERATION_PEAK_LIVE_BYTES: AtomicU64 = AtomicU64::new(0);
static TOTAL_ALLOCATED_BYTES: AtomicU64 = AtomicU64::new(0);
static ALLOCATION_COUNT: AtomicU64 = AtomicU64::new(0);
static DEALLOCATION_COUNT: AtomicU64 = AtomicU64::new(0);
static ACTIVE_OPERATION_COUNT: AtomicU64 = AtomicU64::new(0);
static ACTIVE_SPAN_ID: AtomicU64 = AtomicU64::new(0);
static ACTIVE_CONTEXT: AtomicU64 = AtomicU64::new(0);

static BUDGET_EXCEEDED: AtomicBool = AtomicBool::new(false);
static FAILED_ALLOCATION_SIZE: AtomicU64 = AtomicU64::new(0);
static FAILURE_CURRENT_LIVE_BYTES: AtomicU64 = AtomicU64::new(0);
static FAILURE_PEAK_LIVE_BYTES: AtomicU64 = AtomicU64::new(0);
static FAILURE_OPERATION_PEAK_LIVE_BYTES: AtomicU64 = AtomicU64::new(0);
static FAILURE_TOTAL_ALLOCATED_BYTES: AtomicU64 = AtomicU64::new(0);
static FAILURE_ALLOCATION_COUNT: AtomicU64 = AtomicU64::new(0);
static FAILURE_DEALLOCATION_COUNT: AtomicU64 = AtomicU64::new(0);
static FAILURE_SPAN_ID: AtomicU64 = AtomicU64::new(0);
static FAILURE_CONTEXT: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[repr(u64)]
pub enum HeapBudgetContext {
    #[default]
    Unknown = 0,
    OperationEntry = 1,
    WorkerCommand = 2,
    OpenMlsSpanExecution = 3,
    ProfilingStart = 4,
    ProfilingFinish = 5,
    ProfilingEmit = 6,
}

impl HeapBudgetContext {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Unknown => "unknown",
            Self::OperationEntry => "operation_entry",
            Self::WorkerCommand => "worker_command_outside_profiled_span",
            Self::OpenMlsSpanExecution => "openmls_span_execution",
            Self::ProfilingStart => "profiling_start",
            Self::ProfilingFinish => "profiling_finish",
            Self::ProfilingEmit => "profiling_emit",
        }
    }

    fn from_raw(value: u64) -> Self {
        match value {
            1 => Self::OperationEntry,
            2 => Self::WorkerCommand,
            3 => Self::OpenMlsSpanExecution,
            4 => Self::ProfilingStart,
            5 => Self::ProfilingFinish,
            6 => Self::ProfilingEmit,
            _ => Self::Unknown,
        }
    }
}

pub struct HeapBudgetAttributionGuard {
    previous_span_id: u64,
    previous_context: u64,
}

impl Drop for HeapBudgetAttributionGuard {
    fn drop(&mut self) {
        ACTIVE_SPAN_ID.store(self.previous_span_id, Ordering::Release);
        ACTIVE_CONTEXT.store(self.previous_context, Ordering::Release);
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct HeapBudgetSnapshot {
    pub configured_heap_budget_bytes: u64,
    pub current_live_heap_bytes: u64,
    pub peak_live_heap_bytes: u64,
    pub operation_peak_live_heap_bytes: u64,
    pub total_allocated_bytes: u64,
    pub allocation_count: u64,
    pub deallocation_count: u64,
    pub budget_exceeded: bool,
    pub failed_allocation_size_bytes: Option<u64>,
    pub failure_current_live_heap_bytes: u64,
    pub failure_peak_live_heap_bytes: u64,
    pub failure_operation_peak_live_heap_bytes: u64,
    pub failure_total_allocated_bytes: u64,
    pub failure_allocation_count: u64,
    pub failure_deallocation_count: u64,
    pub failure_span_id: Option<u64>,
    pub failure_context: HeapBudgetContext,
}

pub struct HeapBudgetOperationGuard {
    active: bool,
}

impl Drop for HeapBudgetOperationGuard {
    fn drop(&mut self) {
        if self.active && ACTIVE_OPERATION_COUNT.fetch_sub(1, Ordering::Relaxed) == 1 {
            ACTIVE_SPAN_ID.store(0, Ordering::Release);
            ACTIVE_CONTEXT.store(0, Ordering::Release);
        }
    }
}

pub fn configure_budget(budget_bytes: Option<u64>) {
    let budget = budget_bytes.unwrap_or(0);
    BUDGET_BYTES.store(budget, Ordering::Release);
    BASELINE_LIVE_BYTES.store(CURRENT_LIVE_BYTES.load(Ordering::Acquire), Ordering::Release);
    reset_operation_state();
}

pub fn enabled() -> bool {
    BUDGET_BYTES.load(Ordering::Acquire) > 0
}

pub fn configured_budget_bytes() -> Option<u64> {
    let budget = BUDGET_BYTES.load(Ordering::Acquire);
    (budget > 0).then_some(budget)
}

pub fn begin_operation() -> HeapBudgetOperationGuard {
    if !enabled() {
        return HeapBudgetOperationGuard { active: false };
    }
    reset_operation_state();
    let current = accounted_live_bytes();
    OPERATION_PEAK_LIVE_BYTES.store(current, Ordering::Release);
    ACTIVE_OPERATION_COUNT.fetch_add(1, Ordering::Relaxed);
    ACTIVE_SPAN_ID.store(0, Ordering::Release);
    ACTIVE_CONTEXT.store(HeapBudgetContext::OperationEntry as u64, Ordering::Release);
    check_budget(0);
    HeapBudgetOperationGuard { active: true }
}

pub fn snapshot() -> HeapBudgetSnapshot {
    let failed_size = FAILED_ALLOCATION_SIZE.load(Ordering::Acquire);
    HeapBudgetSnapshot {
        configured_heap_budget_bytes: BUDGET_BYTES.load(Ordering::Acquire),
        current_live_heap_bytes: accounted_live_bytes(),
        peak_live_heap_bytes: PEAK_LIVE_BYTES
            .load(Ordering::Acquire)
            .saturating_sub(BASELINE_LIVE_BYTES.load(Ordering::Acquire)),
        operation_peak_live_heap_bytes: OPERATION_PEAK_LIVE_BYTES.load(Ordering::Acquire),
        total_allocated_bytes: TOTAL_ALLOCATED_BYTES.load(Ordering::Acquire),
        allocation_count: ALLOCATION_COUNT.load(Ordering::Acquire),
        deallocation_count: DEALLOCATION_COUNT.load(Ordering::Acquire),
        budget_exceeded: BUDGET_EXCEEDED.load(Ordering::Acquire),
        failed_allocation_size_bytes: (failed_size > 0).then_some(failed_size),
        failure_current_live_heap_bytes: FAILURE_CURRENT_LIVE_BYTES.load(Ordering::Acquire),
        failure_peak_live_heap_bytes: FAILURE_PEAK_LIVE_BYTES.load(Ordering::Acquire),
        failure_operation_peak_live_heap_bytes: FAILURE_OPERATION_PEAK_LIVE_BYTES.load(Ordering::Acquire),
        failure_total_allocated_bytes: FAILURE_TOTAL_ALLOCATED_BYTES.load(Ordering::Acquire),
        failure_allocation_count: FAILURE_ALLOCATION_COUNT.load(Ordering::Acquire),
        failure_deallocation_count: FAILURE_DEALLOCATION_COUNT.load(Ordering::Acquire),
        failure_span_id: match FAILURE_SPAN_ID.load(Ordering::Acquire) {
            0 => None,
            span_id => Some(span_id),
        },
        failure_context: HeapBudgetContext::from_raw(FAILURE_CONTEXT.load(Ordering::Acquire)),
    }
}

pub fn set_active_span_id(span_id: Option<u64>) {
    ACTIVE_SPAN_ID.store(span_id.unwrap_or(0), Ordering::Release);
}

pub fn set_active_context(context: HeapBudgetContext) {
    ACTIVE_CONTEXT.store(context as u64, Ordering::Release);
}

pub fn enter_attribution_context(
    context: HeapBudgetContext,
    span_id: Option<u64>,
) -> HeapBudgetAttributionGuard {
    let previous_span_id = ACTIVE_SPAN_ID.swap(span_id.unwrap_or(0), Ordering::AcqRel);
    let previous_context = ACTIVE_CONTEXT.swap(context as u64, Ordering::AcqRel);
    HeapBudgetAttributionGuard { previous_span_id, previous_context }
}

pub(crate) fn record_alloc(size: usize) {
    let size = size as u64;
    let current = CURRENT_LIVE_BYTES
        .fetch_add(size, Ordering::Relaxed)
        .saturating_add(size);
    update_max(&PEAK_LIVE_BYTES, current);

    if enabled() {
        TOTAL_ALLOCATED_BYTES.fetch_add(size, Ordering::Relaxed);
        ALLOCATION_COUNT.fetch_add(1, Ordering::Relaxed);
        if ACTIVE_OPERATION_COUNT.load(Ordering::Relaxed) > 0 {
            let accounted = current.saturating_sub(BASELINE_LIVE_BYTES.load(Ordering::Acquire));
            update_max(&OPERATION_PEAK_LIVE_BYTES, accounted);
            check_budget(size);
        }
    }
}

pub(crate) fn record_dealloc(size: usize) {
    let size = size as u64;
    subtract_saturating(&CURRENT_LIVE_BYTES, size);
    if enabled() {
        DEALLOCATION_COUNT.fetch_add(1, Ordering::Relaxed);
    }
}

fn reset_operation_state() {
    BUDGET_EXCEEDED.store(false, Ordering::Release);
    FAILED_ALLOCATION_SIZE.store(0, Ordering::Release);
    FAILURE_CURRENT_LIVE_BYTES.store(0, Ordering::Release);
    FAILURE_PEAK_LIVE_BYTES.store(0, Ordering::Release);
    FAILURE_OPERATION_PEAK_LIVE_BYTES.store(0, Ordering::Release);
    FAILURE_TOTAL_ALLOCATED_BYTES.store(0, Ordering::Release);
    FAILURE_ALLOCATION_COUNT.store(0, Ordering::Release);
    FAILURE_DEALLOCATION_COUNT.store(0, Ordering::Release);
    FAILURE_SPAN_ID.store(0, Ordering::Release);
    FAILURE_CONTEXT.store(0, Ordering::Release);
}

fn check_budget(failed_allocation_size: u64) {
    let budget = BUDGET_BYTES.load(Ordering::Acquire);
    if budget == 0 || ACTIVE_OPERATION_COUNT.load(Ordering::Relaxed) == 0 {
        return;
    }

    let current = accounted_live_bytes();
    if current <= budget {
        return;
    }

    if BUDGET_EXCEEDED
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        .is_ok()
    {
        FAILED_ALLOCATION_SIZE.store(failed_allocation_size, Ordering::Release);
        FAILURE_CURRENT_LIVE_BYTES.store(current, Ordering::Release);
        FAILURE_PEAK_LIVE_BYTES.store(
            PEAK_LIVE_BYTES
                .load(Ordering::Acquire)
                .saturating_sub(BASELINE_LIVE_BYTES.load(Ordering::Acquire)),
            Ordering::Release,
        );
        FAILURE_OPERATION_PEAK_LIVE_BYTES.store(
            OPERATION_PEAK_LIVE_BYTES.load(Ordering::Acquire),
            Ordering::Release,
        );
        FAILURE_TOTAL_ALLOCATED_BYTES.store(
            TOTAL_ALLOCATED_BYTES.load(Ordering::Acquire),
            Ordering::Release,
        );
        FAILURE_ALLOCATION_COUNT.store(ALLOCATION_COUNT.load(Ordering::Acquire), Ordering::Release);
        FAILURE_DEALLOCATION_COUNT.store(DEALLOCATION_COUNT.load(Ordering::Acquire), Ordering::Release);
        FAILURE_SPAN_ID.store(ACTIVE_SPAN_ID.load(Ordering::Acquire), Ordering::Release);
        FAILURE_CONTEXT.store(ACTIVE_CONTEXT.load(Ordering::Acquire), Ordering::Release);
    }
}

fn accounted_live_bytes() -> u64 {
    CURRENT_LIVE_BYTES
        .load(Ordering::Acquire)
        .saturating_sub(BASELINE_LIVE_BYTES.load(Ordering::Acquire))
}

fn update_max(target: &AtomicU64, value: u64) {
    let mut observed = target.load(Ordering::Relaxed);
    while value > observed {
        match target.compare_exchange_weak(observed, value, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => break,
            Err(next) => observed = next,
        }
    }
}

fn subtract_saturating(target: &AtomicU64, value: u64) {
    let mut observed = target.load(Ordering::Relaxed);
    loop {
        let next = observed.saturating_sub(value);
        match target.compare_exchange_weak(observed, next, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => break,
            Err(actual) => observed = actual,
        }
    }
}
