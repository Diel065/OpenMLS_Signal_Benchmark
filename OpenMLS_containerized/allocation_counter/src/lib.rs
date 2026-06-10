mod allocator;

/// Allocation totals measured by a thread-local `measure` scope.
#[derive(Clone, Copy, Default, Debug, PartialEq, Eq, Hash)]
pub struct AllocationInfo {
    pub count_total: u64,
    pub count_current: i64,
    pub count_max: u64,
    pub bytes_total: u64,
    pub bytes_current: i64,
    pub bytes_max: u64,
}

impl std::ops::AddAssign for AllocationInfo {
    fn add_assign(&mut self, other: Self) {
        self.count_total += other.count_total;
        self.count_current += other.count_current;
        self.count_max += other.count_max;
        self.bytes_total += other.bytes_total;
        self.bytes_current += other.bytes_current;
        self.bytes_max += other.bytes_max;
    }
}

/// Monotonic process-wide allocation totals suitable for snapshot deltas.
#[derive(Clone, Copy, Default, Debug, PartialEq, Eq)]
pub struct ProcessAllocationSnapshot {
    pub count_total: u64,
    pub bytes_total: u64,
}

impl ProcessAllocationSnapshot {
    pub fn delta_since(self, earlier: Self) -> Self {
        Self {
            count_total: self.count_total.saturating_sub(earlier.count_total),
            bytes_total: self.bytes_total.saturating_sub(earlier.bytes_total),
        }
    }
}

/// Return cumulative allocations made by every thread in this process.
pub fn process_snapshot() -> ProcessAllocationSnapshot {
    allocator::process_snapshot()
}

/// Run a closure while measuring allocations made on the current thread.
pub fn measure<F: FnOnce()>(run_while_measuring: F) -> AllocationInfo {
    allocator::ALLOCATIONS.with(|info_stack| {
        let mut info_stack = info_stack.borrow_mut();
        info_stack.depth += 1;
        assert!(
            (info_stack.depth as usize) < allocator::MAX_DEPTH,
            "Too deep allocation measuring nesting"
        );
        let depth = info_stack.depth;
        info_stack.elements[depth as usize] = AllocationInfo::default();
    });

    run_while_measuring();

    allocator::ALLOCATIONS.with(|info_stack| {
        let mut info_stack = info_stack.borrow_mut();
        let depth = info_stack.depth;
        let popped = info_stack.elements[depth as usize];
        info_stack.depth -= 1;
        let depth = info_stack.depth as usize;
        info_stack.elements[depth] += popped;
        popped
    })
}

/// Exclude allocations on the current thread from thread-local measurements.
pub fn opt_out<F: FnOnce()>(run_while_not_counting: F) {
    allocator::DO_COUNT.with(|depth| {
        *depth.borrow_mut() += 1;
        run_while_not_counting();
        *depth.borrow_mut() -= 1;
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn process_snapshot_includes_other_threads() {
        let before = process_snapshot();
        std::thread::spawn(|| {
            let value = std::hint::black_box(vec![0u8; 4096]);
            assert_eq!(value.len(), 4096);
        })
        .join()
        .unwrap();
        let delta = process_snapshot().delta_since(before);
        assert!(delta.count_total > 0);
        assert!(delta.bytes_total >= 4096);
    }
}
