use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::RefCell;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::{AllocationInfo, ProcessAllocationSnapshot};

pub const MAX_DEPTH: usize = 64;

pub struct AllocationInfoStack {
    pub depth: u32,
    pub elements: [AllocationInfo; MAX_DEPTH],
}

thread_local! {
    pub static ALLOCATIONS: RefCell<AllocationInfoStack> = RefCell::new(AllocationInfoStack {
        depth: 0,
        elements: [AllocationInfo::default(); MAX_DEPTH],
    });
    pub static DO_COUNT: RefCell<u32> = const { RefCell::new(0) };
}

static PROCESS_ALLOCATION_COUNT: AtomicU64 = AtomicU64::new(0);
static PROCESS_ALLOCATION_BYTES: AtomicU64 = AtomicU64::new(0);

pub fn process_snapshot() -> ProcessAllocationSnapshot {
    ProcessAllocationSnapshot {
        count_total: PROCESS_ALLOCATION_COUNT.load(Ordering::Relaxed),
        bytes_total: PROCESS_ALLOCATION_BYTES.load(Ordering::Relaxed),
    }
}

struct CountingAllocator;

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let pointer = System.alloc(layout);
        if !pointer.is_null() {
            PROCESS_ALLOCATION_COUNT.fetch_add(1, Ordering::Relaxed);
            PROCESS_ALLOCATION_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
            DO_COUNT.with(|depth| {
                if *depth.borrow() == 0 {
                    ALLOCATIONS.with(|info_stack| {
                        let mut info_stack = info_stack.borrow_mut();
                        let depth = info_stack.depth;
                        let info = &mut info_stack.elements[depth as usize];
                        info.count_total += 1;
                        info.count_current += 1;
                        info.count_max = info.count_max.max(info.count_current.max(0) as u64);
                        info.bytes_total += layout.size() as u64;
                        info.bytes_current += layout.size() as i64;
                        info.bytes_max = info.bytes_max.max(info.bytes_current.max(0) as u64);
                    });
                }
            });
        }
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        DO_COUNT.with(|depth| {
            if *depth.borrow() == 0 {
                ALLOCATIONS.with(|info_stack| {
                    let mut info_stack = info_stack.borrow_mut();
                    let depth = info_stack.depth;
                    let info = &mut info_stack.elements[depth as usize];
                    info.count_current -= 1;
                    info.bytes_current -= layout.size() as i64;
                });
            }
        });
        System.dealloc(pointer, layout);
    }
}

#[global_allocator]
static GLOBAL: CountingAllocator = CountingAllocator;
